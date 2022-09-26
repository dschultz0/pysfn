import inspect
import ast
import json
from typing import Dict, Set, List, Callable, Mapping, Any, Union, Iterable
from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    Duration,
    Stack,
)
from aws_cdk.aws_stepfunctions import IntegrationPattern, JsonPath
from dataclasses import dataclass

SFN_INDEX = 0


@dataclass
class Retry:
    errors: List[str]
    interval_seconds: int
    max_attempts: int
    backoff_rate: float = None


def state_machine(cdk_stack: Stack, sfn_name: str, local_values, express=False):
    def decorator(func):
        print([k for k in func.__globals__.keys() if k.startswith("step")])
        fts = FunctionToSteps(cdk_stack, func, local_values)
        return sfn.StateMachine(
            cdk_stack,
            sfn_name,
            state_machine_name=sfn_name,
            state_machine_type=sfn.StateMachineType.EXPRESS
            if express
            else sfn.StateMachineType.STANDARD,
            definition=fts.build_sfn_definition(),
        )

    return decorator


class FunctionToSteps:
    def __init__(self, cdk_stack: Stack, func: Callable, local_values):
        global SFN_INDEX
        self.cdk_stack = cdk_stack
        self.func = func
        self.local_values = local_values
        self.state_number = 0
        self.sfn_number = SFN_INDEX
        SFN_INDEX += 1

        # TODO: Should I use the AST instead of this to get the original parameters?
        self.req_params, self.opt_params = _get_parameters(self.func)

        # TODO: Replace this set with a dict that includes the argument types if provided
        self.variables: Set
        self.variables = set(self.req_params)

        # Retrieve the source code for the function with any indent removed
        src_code = inspect.getsource(func).split("\n")
        indent = len(src_code[0]) - len(src_code[0].lstrip())
        src_code = [c[indent:] for c in src_code]

        # Build the AST
        self.ast = ast.parse("\n".join(src_code))
        with open(f"{func.__name__}_ast.txt", "w") as fp:
            fp.write(ast.dump(self.ast, indent=2))

        # Get the function root
        if (
            isinstance(self.ast, ast.Module)
            and len(self.ast.body) == 1
            and isinstance(self.ast.body[0], ast.FunctionDef)
        ):
            self.function_def: ast.FunctionDef
            self.function_def = self.ast.body[0]
        else:
            raise Exception("Unexpected function definition")

    def state_name(self, name):
        self.state_number += 1
        return f"{name} [{self.sfn_number}:{self.state_number}]"

    def build_sfn_definition(self):

        # The first step will always be to put the inputs on the register
        start = sfn.Pass(
            self.cdk_stack,
            self.state_name("Register Input"),
            result_path=JsonPath.string_at("$.register"),
        )
        next_ = start.next

        # For optional parameters we'll check if they are present and default them if they aren't
        next_ = self._add_optional_parameter_steps(next_, self.opt_params)

        # Get the root of the function body
        self.handle_body(next_, self.function_def.body)
        with open(f"{self.func.__name__}.json", "w") as fp:
            json.dump(
                {
                    "StartAt": start.id,
                    "States": {
                        s.id: s.to_state_json()
                        for s in sfn.State.find_reachable_states(start)
                    },
                },
                fp,
                indent=4,
            )
        return start

    def handle_body(self, next_, body: List[ast.stmt]):
        for stmt in body:
            next_ = self.handle_op(next_, stmt)
        return next_

    def handle_op(self, next_, stmt: ast.stmt):
        if isinstance(stmt, ast.AnnAssign) or isinstance(stmt, ast.Assign):
            if isinstance(stmt.value, ast.Constant):
                return self.handle_assign_value(next_, stmt)
            elif isinstance(stmt.value, ast.Call):
                return self.handle_call_function(next_, stmt)
        elif isinstance(stmt, ast.If):
            return self.handle_if(next_, stmt)
        elif isinstance(stmt, ast.Return):
            return self.handle_return(next_, stmt)

        # Treat unhandled statements as a no-op
        return next_

    def _add_optional_parameter_steps(self, next_, opt_params: Mapping[str, Any]):
        for name, value in opt_params.items():
            choice_name = self.state_name(f"Has {name}")
            assign = sfn.Pass(
                self.cdk_stack,
                self.state_name(f"Assign {name} default"),
                input_path=JsonPath.string_at("$.register"),
                result_path=JsonPath.string_at("$.register"),
                parameters=self.append_to_register_params({name: value}),
            )
            choice = sfn.Choice(self.cdk_stack, choice_name)
            choice.when(sfn.Condition.is_not_present(f"$.register.{name}"), assign)
            next_ = next_step(next_, choice, [choice.otherwise, assign.next])
        return next_

    def handle_assign_value(self, next_, stmt: Union[ast.AnnAssign, ast.Assign]):
        if isinstance(stmt, ast.AnnAssign):
            if isinstance(stmt.target, ast.Name):
                var_name = stmt.target.id
            else:
                raise Exception(
                    f"Unexpected assignment target of type {type(stmt.target)}"
                )
        else:
            if len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
                var_name = stmt.targets[0].id
            else:
                raise Exception("Unexpected assignment")
        if isinstance(stmt.value, ast.Constant):
            value = stmt.value.value
        else:
            raise Exception(f"Unexpected assignment value of type {type(stmt.value)}")
        assign = sfn.Pass(
            self.cdk_stack,
            self.state_name(f"Assign {var_name}"),
            input_path=JsonPath.string_at("$.register"),
            result_path=JsonPath.string_at("$.register"),
            parameters=self.append_to_register_params({var_name: value}),
        )
        return next_step(next_, assign, assign.next)

    def handle_if(self, next_, stmt: ast.If):
        condition, name = self.build_condition(stmt.test)
        choice = sfn.Choice(self.cdk_stack, self.state_name(name))

        def if_next(step):
            choice.when(condition, step)

        else_next = choice.otherwise
        if_next = self.handle_body(if_next, stmt.body)
        else_next = self.handle_body(else_next, stmt.orelse)
        return next_step(next_, choice, [else_next, if_next])

    def build_condition(self, test):
        if isinstance(test, ast.Name):
            # We'll want to check the var type to create appropriate conditions based on the type if defined
            return (
                self._if_value(test.id, None),
                f"If {test.id}",
            )
        elif isinstance(test, ast.Compare):
            # Just going to handle simple comparison to string values for now
            if (
                isinstance(test.left, ast.Name)
                and len(test.ops) == 1
                and isinstance(test.ops[0], ast.Eq)
                and len(test.comparators) == 1
                and isinstance(test.comparators[0], ast.Constant)
            ):
                return (
                    sfn.Condition.string_equals(
                        f"$.register.{test.left.id}", test.comparators[0].value
                    ),
                    f"If {test.left.id}=='{test.comparators[0].value}'",
                )
        raise Exception(f"Unhandled test: {ast.dump(test)}")

    @staticmethod
    def _if_value(name, var_type=None):
        param = f"$.register.{name}"
        if isinstance(var_type, bool):
            return sfn.Condition.boolean_equals(param, True)
        elif isinstance(var_type, str):
            return sfn.Condition.and_(
                sfn.Condition.is_present(param),
                sfn.Condition.is_not_null(param),
                sfn.Condition.not_(sfn.Condition.string_equals(param, "")),
            )
        elif isinstance(var_type, int) or isinstance(var_type, float):
            return sfn.Condition.and_(
                sfn.Condition.is_present(param),
                sfn.Condition.is_not_null(param),
                sfn.Condition.not_(sfn.Condition.number_equals(param, 0)),
            )
        else:
            return sfn.Condition.and_(
                sfn.Condition.is_present(param),
                sfn.Condition.is_not_null(param),
                sfn.Condition.or_(
                    sfn.Condition.and_(
                        sfn.Condition.is_boolean(param),
                        sfn.Condition.boolean_equals(param, True),
                    ),
                    sfn.Condition.and_(
                        sfn.Condition.is_string(param),
                        sfn.Condition.not_(sfn.Condition.string_equals(param, "")),
                    ),
                    sfn.Condition.and_(
                        sfn.Condition.is_numeric(param),
                        sfn.Condition.not_(sfn.Condition.number_equals(param, 0)),
                    ),
                ),
            )

    def handle_call_function(self, next_, stmt: Union[ast.Assign, ast.AnnAssign]):
        # Get the function call
        call: ast.Call = stmt.value

        # Get the parameters
        param_names = [a.id for a in call.args]

        # Get the result variable names
        target = stmt.targets[0] if isinstance(stmt, ast.Assign) else stmt.target
        if isinstance(target, ast.Name):
            result_vars = [target.id]
        elif isinstance(target, ast.Tuple):
            result_vars = [n.id for n in target.elts]
        else:
            raise Exception(f"Unexpected result target of type {type(stmt.target)}")

        func = self.local_values[call.func.id]
        params = {
            a: JsonPath.string_at(f"$.register.{p}")
            for a, p in zip(func.definition.args.keys(), param_names)
        }
        if hasattr(func, "get_additional_params"):
            params.update(func.get_additional_params())
        invoke = tasks.LambdaInvoke(
            self.cdk_stack,
            self.state_name(f"Call {call.func.id}"),
            lambda_function=func.get_lambda(),
            payload=sfn.TaskInput.from_object(params),
            result_path=JsonPath.string_at("$.register.out"),
        )
        register = sfn.Pass(
            self.cdk_stack,
            self.state_name(f"Register {call.func.id}"),
            input_path=JsonPath.string_at("$.register"),
            result_path=JsonPath.string_at("$.register"),
            parameters=self.append_to_register_params(
                func.definition.get_result_mapping(result_vars)
            ),
        )
        invoke.next(register)
        return next_step(next_, invoke, register.next)

    def handle_return(self, next_, stmt: ast.Return):
        param_names = [n.id for n in stmt.value.elts]
        return_step = sfn.Pass(
            self.cdk_stack,
            self.state_name(f"Return"),
            parameters={a: JsonPath.string_at(f"$.register.{a}") for a in param_names},
        )
        return next_step(next_, return_step, return_step.next)

    def append_to_register_params(self, values: Dict):
        params = values.copy()
        # CDK is dropping None from parameters for some reason, using this to hack around it
        for k, v in params.items():
            if v is None:
                params[k] = ""
        for v in self.variables:
            if v not in params:
                params[v] = JsonPath.string_at(f"$.{v}")
        self.variables.update(values.keys())
        return params


def next_step(next_, start, end):
    if isinstance(next_, list):
        for i in flatten(next_):
            i(start)
    else:
        next_(start)
    return end


def _get_parameters(func) -> (List[str], Mapping[str, Any]):
    sig = inspect.signature(func)
    req_params = [
        p.name for p in sig.parameters.values() if p.default == inspect._empty
    ]
    opt_params = {
        p.name: p.default
        for p in sig.parameters.values()
        if p.default != inspect._empty
    }
    return req_params, opt_params


def flatten(items):
    for x in items:
        if isinstance(x, Iterable) and not isinstance(x, (str, bytes)):
            for sub_x in flatten(x):
                yield sub_x
        else:
            yield x
