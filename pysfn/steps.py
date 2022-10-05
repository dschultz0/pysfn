import inspect
import ast
import json
from dataclasses import dataclass
from types import BuiltinFunctionType
from typing import Dict, Set, List, Callable, Mapping, Any, Union, Iterable

from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    Duration,
    Stack,
)
from aws_cdk.aws_stepfunctions import JsonPath

from .condition import build_condition

SFN_INDEX = 0


@dataclass
class Retry:
    errors: List[str]
    interval_seconds: int
    max_attempts: int
    backoff_rate: float = None


@dataclass
class CatchHandler:
    errors: List[str]
    body: List[sfn.IChainable]
    result_path: str = "$.error-info"


def state_machine(
    cdk_stack: Stack, sfn_name: str, local_values, express=False, skip_pass=True
):
    """
    Function decorator to trigger creation of an AWS Step Functions state machine construct
    from the instructions in the function.
    """

    def decorator(func):
        fts = FunctionToSteps(cdk_stack, func, local_values, skip_pass=skip_pass)
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
    def __init__(self, cdk_stack: Stack, func: Callable, local_values, skip_pass=True):
        global SFN_INDEX
        self.cdk_stack = cdk_stack
        self.func = func
        self.local_values = local_values
        self.state_number = 0
        self.sfn_number = SFN_INDEX
        self.skip_pass = skip_pass
        SFN_INDEX += 1

        self.req_params, self.opt_params = _get_parameters(self.func)

        # TODO: Replace this set with a dict that includes the argument types if provided
        self.variables: Set[str]
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
        # For optional parameters we'll check if they are present and default them if they aren't
        c, n = self.build_optional_parameter_steps(self.opt_params)
        next_ = advance(start.next, c, n)

        # Get the root of the function body
        c, n = self.handle_body(self.function_def.body)
        advance(next_, c, n)

        write_definition_json(self.func.__name__, start)
        return start

    def handle_body(self, body: List[ast.stmt]) -> (List[sfn.IChainable], Callable):
        chain = []
        next_ = None
        for stmt in body:
            c, n = self.handle_op(stmt)
            chain.extend(c)
            if next_:
                next_ = advance(next_, c, n)
            else:
                next_ = n
        return chain, next_

    def handle_op(self, stmt: ast.stmt) -> (List[sfn.IChainable], Callable):
        if isinstance(stmt, ast.AnnAssign) or isinstance(stmt, ast.Assign):
            if isinstance(stmt.value, ast.Constant):
                return self.handle_assign_value(stmt)
            elif isinstance(stmt.value, ast.Call):
                return self.handle_call_function(stmt.value, stmt)
        elif isinstance(stmt, ast.If):
            return self.handle_if(stmt)
        elif isinstance(stmt, ast.Return):
            return self.handle_return(stmt)
        elif isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call):
            return self.handle_call_function(stmt.value)
        elif isinstance(stmt, ast.With):
            return self.handle_with(stmt)
        elif isinstance(stmt, ast.Try):
            return self.handle_try(stmt)
        elif isinstance(stmt, ast.Pass):
            if self.skip_pass:
                return [], None
            else:
                pass_step = sfn.Pass(self.cdk_stack, self.state_name("Pass"))
                return [pass_step], pass_step.next

        # Treat unhandled statements as a no-op
        print(f"Unhandled {str(stmt)}")
        return [], None

    def handle_with(self, stmt: ast.With):
        w_val = self.build_with(stmt)
        chain, n = self.handle_body(stmt.body)
        for s in chain:
            if hasattr(s, "add_retry"):
                s.add_retry(
                    errors=w_val.errors,
                    max_attempts=w_val.max_attempts,
                    interval=Duration.seconds(w_val.interval_seconds),
                    backoff_rate=w_val.backoff_rate,
                )
        return chain, n

    def handle_try(self, stmt: ast.Try):
        chain, n = self.handle_body(stmt.body)
        nexts = [n]
        handlers = []
        for handler in stmt.handlers:
            h_chain, h_n = self.handle_body(handler.body)
            # If the handler body isn't empty, add it in
            if h_chain:
                chain.extend(h_chain)
                nexts.append(h_n)
            handlers.append(self.build_exception_handler(handler, h_chain))

        for s in chain:
            if hasattr(s, "add_catch"):
                for handler in handlers:
                    nn = self.attach_catch(s, handler)
                    if nn:
                        nexts.append(nn)

        return chain, nexts

    def build_exception_handler(
        self, handler: ast.ExceptHandler, chain: List[sfn.IChainable]
    ):
        if isinstance(handler.type, ast.Name) and handler.type.id == "Exception":
            return CatchHandler(["States.ALL"], chain)
        raise Exception(f"Unhandled exception of type {handler.type}")

    @staticmethod
    def attach_catch(step, handler):
        def inner_next(n):
            step.add_catch(
                n, errors=handler.errors, result_path=handler.result_path,
            )

        if handler.body:
            advance(inner_next, handler.body, None)
            return None
        else:
            return inner_next

    def build_with(self, stmt: ast.With):
        if len(stmt.items) != 1:
            raise Exception(f"With statements can only support a single item")
        call = stmt.items[0].context_expr
        if isinstance(call, ast.Call):
            if call.func.id == "Retry":
                params = self.build_parameters(call, Retry, False)
                return Retry(**params)
        raise Exception(f"Unhandled with operation: {call}")

    def build_optional_parameter_steps(self, opt_params: Mapping[str, Any]):
        chain = []
        next_ = None
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
            chain.extend([choice, assign])
            if next_:
                next_ = advance(next_, choice, [choice.otherwise, assign.next])
            else:
                next_ = [choice.otherwise, assign.next]
        return chain, next_

    def handle_assign_value(
        self, stmt: Union[ast.AnnAssign, ast.Assign]
    ) -> (List[sfn.IChainable], Callable):
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
        return [assign], assign.next

    def handle_if(self, stmt: ast.If) -> (List[sfn.IChainable], Callable):
        condition, name = build_condition(stmt.test)
        choice = sfn.Choice(self.cdk_stack, self.state_name(name))

        def if_next(step):
            choice.when(condition, step)

        if_c, if_n = self.handle_body(stmt.body)
        else_c, else_n = self.handle_body(stmt.orelse)
        if_n = advance(if_next, if_c, if_n)
        else_n = advance(choice.otherwise, else_c, else_n)
        chain = [choice]
        if if_c:
            chain.extend(if_c)
        if else_c:
            chain.extend(else_c)
        return chain, [if_n, else_n]

    def handle_call_function(
        self, call: ast.Call, assign: Union[ast.Assign, ast.AnnAssign] = None,
    ) -> (List[sfn.IChainable], Callable):
        # Get the function
        func = self.local_values.get(call.func.id)

        # Build the parameters
        if func:
            params = self.build_parameters(call, func)
            if hasattr(func, "get_additional_params"):
                params.update(func.get_additional_params())
        elif call.func.id in ["time.sleep", "sleep"]:
            params = {}
        else:
            raise Exception(f"Unable to find function {call.func.id}")

        if call.func.id in ["time.sleep", "sleep"]:
            invoke = sfn.Wait(
                self.cdk_stack,
                self.state_name("Wait"),
                time=sfn.WaitTime.duration(Duration.seconds(call.args[0].value)),
            )
        elif hasattr(func, "definition"):
            invoke = tasks.LambdaInvoke(
                self.cdk_stack,
                self.state_name(f"Call {call.func.id}"),
                lambda_function=func.get_lambda(),
                payload=sfn.TaskInput.from_object(params),
                result_path=JsonPath.string_at("$.register.out"),
            )
        else:
            raise Exception(f"Function without an associated Lambda: {call.func.id}")
        chain = [invoke]
        next_ = invoke.next

        if assign:
            # Get the result variable names
            target = (
                assign.targets[0] if isinstance(assign, ast.Assign) else assign.target
            )
            if isinstance(target, ast.Name):
                result_vars = [target.id]
            elif isinstance(target, ast.Tuple):
                result_vars = [n.id for n in target.elts]
            else:
                raise Exception(
                    f"Unexpected result target of type {type(assign.target)}"
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
            chain.append(register)
            next_ = register.next

        return chain, next_

    def handle_return(self, stmt: ast.Return):
        if isinstance(stmt.value, ast.Tuple):
            param_names = [n.id for n in stmt.value.elts]
            return_step = sfn.Pass(
                self.cdk_stack,
                self.state_name(f"Return"),
                parameters={
                    a: JsonPath.string_at(f"$.register.{a}") for a in param_names
                },
            )
        elif isinstance(stmt.value, ast.Name):
            return_step = sfn.Pass(
                self.cdk_stack,
                self.state_name(f"Return"),
                parameters={
                    stmt.value.id: JsonPath.string_at(f"$.register.{stmt.value.id}")
                },
            )
        elif isinstance(stmt.value, ast.Constant):
            return_step = sfn.Pass(
                self.cdk_stack,
                self.state_name(f"Return"),
                parameters={"value": stmt.value.value},
            )
        else:
            raise Exception(f"Unhandled return value type {stmt.value}")
        return [return_step], return_step.next

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

    def build_parameters(self, call: ast.Call, func: Callable, gen_jsonpath=True):
        params = {}

        # TODO: Handle kwonly args
        if hasattr(func, "definition"):
            args = func.definition.args.keys()
        elif isinstance(func, BuiltinFunctionType):
            return None
        else:
            args = inspect.getfullargspec(func).args
            if len(args) > 0 and args[0] == "self":
                args = args[1:]

        # Add the positional parameters
        for arg_value, arg_name in zip(call.args, args):
            params[arg_name] = self.generate_value_repr(arg_value, gen_jsonpath)

        # Add the keyword parameters
        for kw in call.keywords:
            params[kw.arg] = self.generate_value_repr(kw.value, gen_jsonpath)
        return params

    def generate_value_repr(self, arg_value, gen_jsonpath=True):
        if isinstance(arg_value, ast.Name):
            if arg_value.id not in self.variables:
                raise Exception(f"Undefined variable {arg_value.id}")
            expr = f"$.register.{arg_value.id}"
            return JsonPath.string_at(expr) if gen_jsonpath else expr
        elif isinstance(arg_value, ast.Constant):
            return arg_value.value
        elif isinstance(arg_value, ast.List):
            expr = [self.generate_value_repr(val, False) for val in arg_value.elts]
            return JsonPath.array(*expr) if gen_jsonpath else expr
        else:
            raise Exception(f"Unexpected argument: {arg_value}")


def advance(
    next_: Union[Callable, List[Callable]],
    chain: Union[List[sfn.IChainable], sfn.IChainable, None],
    new_next: Union[Callable, List[Callable], None],
):
    if chain:
        if isinstance(chain, list):
            chain = chain[0]
        if isinstance(next_, list):
            for i in flatten(next_):
                i(chain)
        else:
            next_(chain)
        return new_next
    else:
        return next_


def _get_parameters(func) -> (List[str], Mapping[str, Any]):
    # TODO: Should I use the AST instead of this to get the original parameters?
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


def write_definition_json(name, start):
    with open(f"{name}.json", "w") as fp:
        json.dump(
            {
                "StartAt": start.id,
                "States": {
                    s.id: s.to_state_json()
                    for s in sfn.State.find_reachable_states(
                        start, include_error_handlers=True
                    )
                },
            },
            fp,
            indent=4,
        )
