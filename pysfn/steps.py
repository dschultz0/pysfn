import inspect
import pathlib
import ast
import json
import typing
from .function import gather_function_attributes, FunctionAttributes
from dataclasses import dataclass
from types import BuiltinFunctionType
from typing import Dict, List, Callable, Mapping, Any, Union, Iterable, Optional, Type

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


def concurrent(iterable, max_concurrency: Optional[int] = None):
    pass


def event(func: Callable):
    pass


def await_token(
    func: Callable,
    return_args: Union[List[str], Mapping[str, Type]],
    duration: Duration = None,
):
    pass


def state_machine(
    cdk_stack: Stack,
    sfn_name: str,
    local_values,
    express=False,
    skip_pass=True,
    return_vars: Optional[Union[List[str], Mapping[str, typing.Type]]] = None,
):
    """
    Function decorator to trigger creation of an AWS Step Functions state machine construct
    from the instructions in the function.
    """

    def decorator(func):
        func_attrs = gather_function_attributes(func, None, return_vars)
        fts = FunctionToSteps(cdk_stack, func_attrs, local_values, skip_pass=skip_pass)
        func.state_machine = sfn.StateMachine(
            cdk_stack,
            sfn_name,
            state_machine_name=sfn_name,
            state_machine_type=sfn.StateMachineType.EXPRESS
            if express
            else sfn.StateMachineType.STANDARD,
            definition=fts.build_sfn_definition(),
        )
        func.output = func_attrs.output
        return func

    return decorator


class FunctionToSteps:
    def __init__(
        self,
        cdk_stack: Stack,
        func_attrs: FunctionAttributes,
        local_values,
        skip_pass=True,
    ):
        global SFN_INDEX
        self.cdk_stack = cdk_stack
        self.func = func_attrs.func
        self.output = func_attrs.output
        self.local_values = local_values
        self.state_number = 0
        self.sfn_number = SFN_INDEX
        self.skip_pass = skip_pass
        SFN_INDEX += 1

        self.ast = func_attrs.tree
        with open(pathlib.Path("build", f"{func_attrs.name}_ast.txt"), "w") as fp:
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

        # TODO: Capture the parameter types to use elsewhere
        req_params, opt_params = _get_parameters(self.func)

        # Get the root of the function body
        scope = SFNScope(self)
        start, next_ = scope.generate_entry_steps(req_params, opt_params)
        c, n = scope.handle_body(self.function_def.body)
        advance(next_, c, n)

        write_definition_json(self.func.__name__, start)
        return start


class SFNScope:
    def __init__(self, fts: FunctionToSteps):
        self.fts = fts
        self.cdk_stack = fts.cdk_stack
        self.state_name = fts.state_name
        self.variables: Dict[str, typing.Type] = {}
        self.output = fts.output

    def generate_entry_steps(
        self, required_parameters, optional_parameters: Mapping[str, Any] = None
    ):
        self.variables.update({param: typing.Any for param in required_parameters})

        # The first step will always be to put the inputs on the register
        start = sfn.Pass(
            self.cdk_stack, self.state_name("Register Input"), result_path="$.register",
        )
        # For optional parameters we'll check if they are present and default them if they aren't
        c, n = self.build_optional_parameter_steps(optional_parameters)
        next_ = advance(start.next, c, n)
        return start, next_

    def build_register_assignment(self, values: Dict, register_path: str = ""):
        params = values.copy()
        for k, v in params.items():
            self._updated_var(k)
            # CDK is dropping None from parameters for some reason, using this to hack around it
            if v is None:
                params[k] = ""
        # Copy over any variables that aren't in the params
        for v in self.variables.keys():
            if v not in params:
                params[v] = JsonPath.string_at(f"$.{register_path}{v}")
        for key in values.keys():
            if key not in self.variables:
                # TODO: Assign the type of the value
                self.add_var(key)

        params = {update_param_name(k, v): v for k, v in params.items()}
        return params

    def add_var(self, key: str, var_type: Type = Any):
        self.variables[key] = var_type
        self._added_var(key)

    def _added_var(self, var: str):
        pass

    def _updated_var(self, var: str):
        pass

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
            # if isinstance(stmt.value, ast.Constant):
            #    return self.handle_assign_value(stmt)
            if isinstance(stmt.value, ast.Call) and isinstance(
                stmt.value.func, ast.Name
            ):
                return self.handle_call_function(stmt.value, stmt)
            if isinstance(stmt.value, ast.ListComp):
                return self.handle_list_comp(stmt)
            else:
                return self.handle_assign_value(stmt)
        elif isinstance(stmt, ast.If):
            return self.handle_if(stmt)
        elif isinstance(stmt, ast.Return):
            return self.handle_return(stmt)
        elif isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call):
            if isinstance(stmt.value.func, ast.Name):
                return self.handle_call_function(stmt.value)
            elif isinstance(stmt.value.func, ast.Attribute):
                if stmt.value.func.attr == "append":
                    return self.handle_array_append(stmt.value)
                elif stmt.value.func.attr == "extend":
                    # TODO
                    pass
        elif isinstance(stmt, ast.With):
            return self.handle_with(stmt)
        elif isinstance(stmt, ast.Try):
            return self.handle_try(stmt)
        elif isinstance(stmt, ast.For):
            return self.handle_for(stmt)
        elif isinstance(stmt, ast.AugAssign) and (
            isinstance(stmt.op, ast.Add) or isinstance(stmt.op, ast.Sub)
        ):
            return self.handle_math_add(stmt)
        elif isinstance(stmt, ast.Pass):
            if self.fts.skip_pass:
                return [], None
            else:
                pass_step = sfn.Pass(self.cdk_stack, self.state_name("Pass"))
                return [pass_step], pass_step.next

        # Treat unhandled statements as a no-op
        print(f"Unhandled {repr(stmt)}")
        print(ast.dump(stmt, indent=2))
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
        chain, n = ChildScope(self).handle_body(stmt.body)
        nexts = [n]
        handlers = []
        for handler in stmt.handlers:
            child_scope = ChildScope(self)
            result_path = None
            if handler.name:
                result_path = f"$.register.{handler.name}"
                child_scope.add_var(handler.name)
            h_chain, h_n = child_scope.handle_body(handler.body)

            # If the handler body isn't empty, add it in
            if h_chain:
                chain.extend(h_chain)
                nexts.append(h_n)
            handlers.append(self.build_exception_handler(handler, h_chain, result_path))

        for s in chain:
            if hasattr(s, "add_catch"):
                for handler in handlers:
                    nn = self.attach_catch(s, handler)
                    if nn:
                        nexts.append(nn)

        return chain, nexts

    def build_exception_handler(
        self,
        handler: ast.ExceptHandler,
        chain: List[sfn.IChainable],
        result_path: str = None,
    ):
        if isinstance(handler.type, ast.Name) and handler.type.id == "Exception":
            return CatchHandler(["States.ALL"], chain, result_path=result_path)
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

    @staticmethod
    def map_arg(arg):
        if isinstance(arg, ast.Name):
            return f"$.register.{arg.id}"
        elif isinstance(arg, ast.Constant):
            return arg.value
        elif (
            isinstance(arg, ast.Subscript)
            and isinstance(arg.value, ast.Name)
            and isinstance(arg.slice, ast.Constant)
        ):
            return f"$.register.{arg.value.id}[{arg.slice.value}]"
        else:
            raise Exception("Args must be Name or Constant")

    def _get_iterator(self, iterator: ast.expr) -> (str, str, sfn.State, int):
        # TODO: Support enumerate
        # TODO: Support callable iterator
        max_concurrency = 1
        iterator_step = None

        # if the iterator is wrapped in a 'concurrent' function, capture the parameter as the concurrency for the map
        if (
            isinstance(iterator, ast.Call)
            and isinstance(iterator.func, ast.Name)
            and iterator.func.id == "concurrent"
        ):
            if len(iterator.args) > 1:
                max_concurrency = iterator.args[1].value
            else:
                max_concurrency = 0
            iterator = iterator.args[0]

        # If the iterator is a range function, capture the parameters and create a pass step to build the value array
        if isinstance(iterator, ast.Call) and isinstance(iterator.func, ast.Name):
            (
                iterator_step,
                return_vars,
                func_name,
                result_prefix,
            ) = self._build_func_call(iterator, "$.iter")
            iter_var = iterator.func.id
            items_path = f"$.iter{result_prefix}.{return_vars[0]}"

        # If the iterator is a name, use that value
        elif isinstance(iterator, ast.Name):
            iter_var = iterator.id
            items_path = f"$.register.{iter_var}"
        else:
            raise Exception("Unsupported for-loop iterator, variables only")
        return iter_var, items_path, iterator_step, max_concurrency

    def handle_for(self, stmt: ast.For):
        iter_var, items_path, iterator_step, max_concurrency = self._get_iterator(
            stmt.iter
        )
        if not isinstance(stmt.target, ast.Name):
            raise Exception("Unsupported for-loop target, variables only")

        map_state = sfn.Map(
            self.cdk_stack,
            self.state_name(f"For {iter_var}"),
            max_concurrency=max_concurrency,
            items_path=items_path,
            parameters={
                "register.$": f"$.register",
                f"{stmt.target.id}.$": "$$.Map.Item.Value",
            },
            result_path="$.register.loopResult",
        )

        # Create a scope for the for loop contents and build the contained steps
        # Also build an entry step to capture the iterator target
        map_scope = MapScope(self)
        entry_step = map_scope.build_entry_step(stmt.target.id)
        chain, map_next_ = map_scope.handle_body(stmt.body)
        entry_step.next(chain[0])
        map_state.iterator(entry_step)
        next_ = map_state.next

        # if any vars from the outer scope were updated, add a 'return' step to the map operations and
        # logic to pull those results into the register after the map completes
        map_return_step_name = self.state_name("Map return")
        if map_scope.updated_vars:
            return_params = {
                v: JsonPath.string_at(f"$.register.{v}") for v in map_scope.updated_vars
            }
            map_return_step = sfn.Pass(
                self.cdk_stack, map_return_step_name, parameters=return_params
            )
            consolidate_params = {
                v: JsonPath.string_at(f"$.register.loopResult[*].{v}[*]")
                for v in map_scope.updated_vars
            }
            consolidate_step = sfn.Pass(
                self.cdk_stack,
                self.state_name(f"Consolidate map results"),
                result_path="$.register",
                parameters=self.build_register_assignment(
                    consolidate_params, "register."
                ),
            )
            map_state.next(consolidate_step)
            next_ = consolidate_step.next
        else:
            map_return_step = sfn.Pass(
                self.cdk_stack, map_return_step_name, parameters={}
            )
        advance(map_next_, [map_return_step], map_return_step.next)

        # finalize the steps
        if iterator_step:
            iterator_step.next(map_state)
            return [iterator_step, map_state], next_
        else:
            return [map_state], next_

    def handle_list_comp(self, stmt: Union[ast.Assign, ast.AnnAssign]):
        target = stmt.targets[0] if isinstance(stmt, ast.Assign) else stmt.target
        if isinstance(target, ast.Name):
            target = target.id
        else:
            raise Exception("Unsupported list comp target, variables only")

        list_comp = stmt.value
        if not isinstance(list_comp, ast.ListComp):
            raise Exception("Unhandled list comp value")
        if len(list_comp.generators) != 1:
            raise Exception("List comp can only support a single generator")
        comp = list_comp.generators[0]
        if not isinstance(comp, ast.comprehension):
            raise Exception(f"Unhandled generator {type(comp)}")

        # TODO: Support enumerate...
        if (
            isinstance(comp.iter, ast.Call)
            and isinstance(comp.iter.func, ast.Name)
            and comp.iter.func.id == "concurrent"
        ):
            max_concurrency = comp.iter.args[1].value
            iter_var = comp.iter.args[0].id
        elif isinstance(comp.iter, ast.Name):
            max_concurrency = 0
            iter_var = comp.iter.id
        else:
            raise Exception("Unsupported list comp iterator, variables only")
        if not isinstance(comp.target, ast.Name):
            raise Exception("Unsupported list comp target, variables only")

        choice_name = self.state_name(f"Has {iter_var} to map")
        map_state = sfn.Map(
            self.cdk_stack,
            self.state_name(f"For {iter_var}"),
            max_concurrency=max_concurrency,
            items_path=f"$.register.{iter_var}",
            parameters={
                "register.$": f"$.register",
                f"{comp.target.id}.$": "$$.Map.Item.Value",
            },
            result_path="$.loopResult",
        )
        choice = sfn.Choice(self.cdk_stack, choice_name)
        choice.when(
            sfn.Condition.and_(
                sfn.Condition.is_present(f"$.register.{iter_var}"),
                sfn.Condition.is_present(f"$.register.{iter_var}[0]"),
            ),
            map_state,
        )

        call_func = sfn.Pass(
            self.cdk_stack,
            self.state_name(f"Call function..."),
            parameters={"loopResult": JsonPath.string_at(f"$.{comp.target.id}")},
        )

        map_state.iterator(call_func)
        consolidate_step = sfn.Pass(
            self.cdk_stack,
            self.state_name(f"Consolidate map results"),
            result_path="$.register",
            parameters=self.build_register_assignment(
                {target: JsonPath.string_at(f"$.loopResult[*][*]")},
                "loopResult[0].register.",
            ),
        )
        map_state.next(consolidate_step)

        return [choice, map_state], [choice.otherwise, consolidate_step.next]

    def handle_math_add(self, stmt: ast.AugAssign):
        if isinstance(stmt.target, ast.Name):
            target = stmt.target.id
        else:
            raise Exception(f"Unexpected MathAdd target {type(stmt.target)}")
        if isinstance(stmt.value, ast.Constant):
            if isinstance(stmt.op, ast.Add):
                value = stmt.value.value
            elif isinstance(stmt.op, ast.Sub):
                value = -stmt.value.value
            else:
                raise Exception(f"Unsupported MathAdd op {type(stmt.op)}")
        else:
            raise Exception(f"Unsupported MathAdd target {type(stmt.value)}")
        add_step = sfn.Pass(
            self.cdk_stack,
            self.state_name(f"Add {value} to {target}"),
            input_path="$.register",
            result_path="$.register",
            parameters=self.build_register_assignment(
                {target: JsonPath.string_at(f"States.MathAdd($.{target}, {value})")}
            ),
        )
        return [add_step], add_step.next

    def handle_array_append(self, stmt: ast.Call):
        array_name = stmt.func.value.id
        array_path = f"$.register.{array_name}"
        arg = stmt.args[0]
        if isinstance(arg, ast.Name):
            value = arg.id
            path_to_add = f"$.register.{arg.id}"
        elif isinstance(arg, ast.Constant):
            value = arg.value
            path_to_add = arg.value
        else:
            raise Exception(f"Unexpected type {type(arg)} for list append")
        list_step = sfn.Pass(
            self.cdk_stack,
            self.state_name(f"Append {value} to {array_name}"),
            result_path="$.meta",
            parameters={
                "arrayConcat": JsonPath.string_at(
                    f"States.Array({array_path}, States.Array({path_to_add}))"
                )
            },
        )
        flatten_step = sfn.Pass(
            self.cdk_stack,
            self.state_name(f"Flatten {array_name}"),
            result_path="$.register",
            parameters=self.build_register_assignment(
                {array_name: JsonPath.string_at("$.meta.arrayConcat[*][*]")},
                "register.",
            ),
        )
        list_step.next(flatten_step)
        return [list_step, flatten_step], flatten_step.next

    def handle_assign_value(
        self, stmt: Union[ast.AnnAssign, ast.Assign]
    ) -> (List[sfn.IChainable], Callable):
        sub_target = None
        if isinstance(stmt, ast.AnnAssign):
            if isinstance(stmt.target, ast.Name):
                var_name = stmt.target.id
            elif (
                isinstance(stmt.target, ast.Subscript)
                and isinstance(stmt.target.value, ast.Name)
                and isinstance(stmt.target.slice, ast.Constant)
            ):
                var_name = stmt.target.value.id
                sub_target = stmt.target.slice.value
            else:
                raise Exception(
                    f"Unexpected assignment target of type {type(stmt.target)}"
                )
        else:
            if len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
                var_name = stmt.targets[0].id
            elif (
                isinstance(stmt.targets[0], ast.Subscript)
                and isinstance(stmt.targets[0].value, ast.Name)
                and isinstance(stmt.targets[0].slice, ast.Constant)
            ):
                var_name = stmt.targets[0].value.id
                sub_target = stmt.targets[0].slice.value
            else:
                raise Exception("Unexpected assignment")
        if sub_target:
            prep = sfn.Pass(
                self.cdk_stack,
                self.state_name(f"Prep assign {var_name}.{sub_target}"),
                input_path="$.register",
                result_path="$.register.itm",
                parameters={sub_target: self.generate_value_repr(stmt.value)},
            )
            assign = sfn.Pass(
                self.cdk_stack,
                self.state_name(f"Assign {var_name}.{sub_target}"),
                input_path="$.register",
                result_path="$.register",
                parameters=self.build_register_assignment(
                    # {f"{var_name}": JsonPath.json_merge(f"$.{var_name}", "$.itm")}
                    {f"{var_name}": f"States.JsonMerge($.{var_name}, $.itm, false)"}
                ),
            )
            prep.next(assign)
            return [prep, assign], assign.next
        else:
            value = self.generate_value_repr(stmt.value)
            assign = sfn.Pass(
                self.cdk_stack,
                self.state_name(f"Assign {var_name}"),
                input_path="$.register",
                result_path="$.register",
                parameters=self.build_register_assignment({var_name: value}),
            )
            return [assign], assign.next

    def handle_if(self, stmt: ast.If) -> (List[sfn.IChainable], Callable):
        condition, name = build_condition(stmt.test)
        choice = sfn.Choice(self.cdk_stack, self.state_name(name))

        def if_next(step):
            choice.when(condition, step)

        if_c, if_n = ChildScope(self).handle_body(stmt.body)
        else_c, else_n = ChildScope(self).handle_body(stmt.orelse)
        if_n = advance(if_next, if_c, if_n)
        else_n = advance(choice.otherwise, else_c, else_n)
        chain = [choice]
        if if_c:
            chain.extend(if_c)
        if else_c:
            chain.extend(else_c)
        return chain, [if_n, else_n]

    def _build_func_call(
        self,
        call: ast.Call,
        result_path: str = "$.register.out",
        invoke_event_: bool = False,
        await_token_: bool = False,
        await_duration_: Duration = None,
    ) -> (sfn.State, List[str], str):
        if isinstance(call.func, ast.Name):
            # Get the function
            func = self.fts.local_values.get(call.func.id)
            result_prefix = ""

            # Build the parameters
            if func:
                params = self.build_parameters(call, func)
                if hasattr(func, "get_additional_params"):
                    params.update(func.get_additional_params())
            elif call.func.id in [
                "time.sleep",
                "sleep",
                "range",
                event.__name__,
                await_token.__name__,
            ]:
                params = {}
            else:
                raise Exception(f"Unable to find function {call.func.id}")

            if call.func.id in ["time.sleep", "sleep"]:
                invoke = sfn.Wait(
                    self.cdk_stack,
                    self.state_name("Wait"),
                    time=sfn.WaitTime.duration(Duration.seconds(call.args[0].value)),
                )
                return_vars = []
            elif call.func.id == "range":
                start_val = 0
                end_val = 0
                step_val = 1
                args = [self.map_arg(arg) for arg in call.args]
                if len(args) == 1:
                    end_val = args[0]
                elif len(args) >= 2:
                    start_val = args[0]
                    end_val = args[1]
                if len(args) == 3:
                    step_val = args[2]
                invoke = sfn.Pass(
                    self.cdk_stack,
                    self.state_name(f"Build range"),
                    parameters={
                        "range": JsonPath.string_at(
                            f"States.ArrayRange({start_val}, States.MathAdd({end_val}, -1), {step_val})"
                        )
                    },
                    result_path=result_path,
                )
                return_vars = ["range"]
            elif (
                call.func.id == event.__name__
                and len(call.args) > 0
                and isinstance(call.args[0], ast.Call)
            ):
                return self._build_func_call(
                    call.args[0], result_path=result_path, invoke_event_=True
                )
            elif (
                call.func.id == await_token.__name__
                and len(call.args) >= 2
                and isinstance(call.args[0], ast.Call)
            ):
                duration = None
                if len(call.args) == 3:
                    duration_arg = call.args[2]
                    if isinstance(duration_arg, ast.Call) and isinstance(
                        duration_arg.func, ast.Attribute
                    ):
                        attr_name = duration_arg.func.attr
                        if len(duration_arg.args) > 0:
                            duration_arg_value = duration_arg.args[0]
                            if isinstance(duration_arg_value, ast.Constant):
                                duration = getattr(Duration, attr_name)(
                                    duration_arg_value.value
                                )
                invoke, return_vars, name, result_prefix = self._build_func_call(
                    call.args[0],
                    result_path=result_path,
                    invoke_event_=True,
                    await_token_=True,
                    await_duration_=duration,
                )
                return_arg = call.args[1]
                if isinstance(return_arg, ast.List) and all(
                    isinstance(a, ast.Constant) for a in return_arg.elts
                ):
                    return_vars = [a.value for a in return_arg.elts]
                return invoke, return_vars, name, ""
            elif hasattr(func, "definition"):
                invocation_type = tasks.LambdaInvocationType.REQUEST_RESPONSE
                integration_pattern = sfn.IntegrationPattern.REQUEST_RESPONSE
                if invoke_event_:
                    invocation_type = tasks.LambdaInvocationType.EVENT
                if await_token_:
                    integration_pattern = sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN
                invoke = tasks.LambdaInvoke(
                    self.cdk_stack,
                    self.state_name(f"Call {call.func.id}"),
                    lambda_function=func.get_lambda(),
                    payload=sfn.TaskInput.from_object(params),
                    input_path="$.register",
                    result_path=result_path,
                    invocation_type=invocation_type,
                    integration_pattern=integration_pattern,
                    heartbeat=await_duration_,
                )
                return_vars = list(func.definition.output.keys())
                result_prefix = ".Payload"
            elif hasattr(func, "state_machine"):
                invoke = tasks.StepFunctionsStartExecution(
                    self.cdk_stack,
                    self.state_name(f"Call {func.state_machine.state_machine_name}"),
                    state_machine=func.state_machine,
                    integration_pattern=sfn.IntegrationPattern.RUN_JOB,
                    input=sfn.TaskInput.from_object(params),
                )
                return_vars = list(func.output.keys())
                result_prefix = ".Payload"
            else:
                raise Exception(
                    f"Function without an associated Lambda: {call.func.id}"
                )
            return invoke, return_vars, call.func.id, result_prefix
        else:
            raise Exception(
                f"Function attribute is not of type name: {type(call.func)}"
            )

    def handle_call_function(
        self, call: ast.Call, assign: Union[ast.Assign, ast.AnnAssign] = None,
    ) -> (List[sfn.IChainable], Callable):
        invoke, return_vars, name, result_prefix = self._build_func_call(
            call, "$.register.out"
        )
        chain = [invoke]
        next_ = invoke.next

        if assign:
            # Get the result variable names
            target = (
                assign.targets[0] if isinstance(assign, ast.Assign) else assign.target
            )
            if isinstance(target, ast.Name):
                result_targets = [target.id]
            elif isinstance(target, ast.Tuple):
                result_targets = [n.id for n in target.elts]
            else:
                raise Exception(
                    f"Unexpected result target of type {type(assign.target)}"
                )
            result_params = {
                v: JsonPath.string_at(f"$.out{result_prefix}.{r}")
                for v, r in zip(result_targets, return_vars)
            }
            if len(result_params) < len(result_targets):
                raise Exception(
                    f"Unable to map all response targets to return values for {name}"
                )
            register = sfn.Pass(
                self.cdk_stack,
                self.state_name(f"Register {call.func.id}"),
                input_path="$.register",
                result_path="$.register",
                parameters=self.build_register_assignment(result_params),
            )
            invoke.next(register)
            chain.append(register)
            next_ = register.next

        return chain, next_

    @staticmethod
    def _return_value(value: Union[ast.expr, ast.stmt]):
        if isinstance(value, ast.Name):
            return JsonPath.string_at(f"$.register.{value.id}")
        elif isinstance(value, ast.Constant):
            return value.value
        else:
            raise Exception(f"Unanticipated return type: {value}")

    def handle_return(self, stmt: ast.Return):
        if isinstance(stmt.value, ast.Tuple):
            if len(self.output) != len(stmt.value.elts):
                raise Exception("Mismatched return value counts")
            return_step = sfn.Pass(
                self.cdk_stack,
                self.state_name(f"Return"),
                parameters={
                    k: self._return_value(v)
                    for k, v in zip(self.output.keys(), stmt.value.elts)
                },
            )
        elif isinstance(stmt.value, ast.Name) or isinstance(stmt.value, ast.Constant):
            if len(self.output) != 1:
                raise Exception("Mismatched return value counts")
            return_step = sfn.Pass(
                self.cdk_stack,
                self.state_name(f"Return"),
                parameters={
                    list(self.output.keys())[0]: self._return_value(stmt.value)
                },
            )
        elif stmt.value is None:
            return_step = sfn.Pass(self.cdk_stack, self.state_name(f"Return"))
        else:
            raise Exception(f"Unhandled return value type {stmt.value}")
        return [return_step], return_step.next

    def build_parameters(self, call: ast.Call, func: Callable, gen_jsonpath=True):
        params = {}

        # TODO: Handle kwonly args
        if hasattr(func, "definition"):
            args = func.definition.input.keys()
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

    def build_optional_parameter_steps(self, optional_parameters: Mapping[str, Any]):
        chain = []
        next_ = None
        for name, value in optional_parameters.items():
            choice_name = self.state_name(f"Has {name}")
            assign = sfn.Pass(
                self.cdk_stack,
                self.state_name(f"Assign {name} default"),
                input_path="$.register",
                result_path="$.register",
                parameters=self.build_register_assignment({name: value}),
            )
            choice = sfn.Choice(self.cdk_stack, choice_name)
            choice.when(sfn.Condition.is_not_present(f"$.register.{name}"), assign)
            chain.extend([choice, assign])
            if next_:
                next_ = advance(next_, choice, [choice.otherwise, assign.next])
            else:
                next_ = [choice.otherwise, assign.next]
        return chain, next_

    def generate_value_repr(self, arg_value, gen_jsonpath=True):
        if isinstance(arg_value, ast.Name):
            # if arg_value.id not in self.variables:
            #    raise Exception(f"Undefined variable {arg_value.id}")
            expr = f"$.{arg_value.id}"
            return JsonPath.string_at(expr) if gen_jsonpath else expr
        elif isinstance(arg_value, ast.Constant):
            return arg_value.value
        elif isinstance(arg_value, ast.List):
            expr = [self.generate_value_repr(val, False) for val in arg_value.elts]
            return JsonPath.array(*expr) if gen_jsonpath else expr
        elif (
            isinstance(arg_value, ast.Subscript)
            and isinstance(arg_value.value, ast.Name)
            and isinstance(arg_value.slice, ast.Constant)
        ):
            if isinstance(arg_value.slice.value, int):
                expr = f"$.{arg_value.value.id}[{arg_value.slice.value}]"
            else:
                expr = f"$.{arg_value.value.id}.{arg_value.slice.value}"
            return JsonPath.string_at(expr) if gen_jsonpath else expr
        elif isinstance(arg_value, ast.Dict):
            obj = {}
            for k, v in zip(arg_value.keys, arg_value.values):
                if not isinstance(k, ast.Constant):
                    raise Exception("Dict keys must be a constant")
                else:
                    obj[k.value] = self.generate_value_repr(v, gen_jsonpath)
            return obj
        # TODO: evaluate if this hack for handling JsonPath values is the best approach
        elif isinstance(arg_value, ast.Attribute) and (
            (
                isinstance(arg_value.value, ast.Attribute)
                and arg_value.value.attr == "JsonPath"
            )
            or (
                isinstance(arg_value.value, ast.Name)
                and arg_value.value.id == "JsonPath"
            )
        ):
            return getattr(JsonPath, arg_value.attr)
        else:
            raise Exception(f"Unexpected argument: {arg_value}")


class MapScope(SFNScope):
    def __init__(self, parent_scope: SFNScope):
        super(MapScope, self).__init__(parent_scope.fts)
        self.variables = parent_scope.variables.copy()
        self.scoped_variables = []
        self._updated_vars = []

    def _added_var(self, var: str):
        self.scoped_variables.append(var)

    def _updated_var(self, var: str):
        self._updated_vars.append(var)

    def build_entry_step(self, entry_var: str):
        return sfn.Pass(
            self.cdk_stack,
            self.state_name("Register loop value"),
            result_path="$.register",
            parameters=self.build_register_assignment(
                {entry_var: JsonPath.string_at(f"$.{entry_var}")}, "register."
            ),
        )

    @property
    def updated_vars(self):
        return [v for v in self._updated_vars if v not in self.scoped_variables]


class ChildScope(SFNScope):
    def __init__(self, parent_scope: SFNScope):
        super(ChildScope, self).__init__(parent_scope.fts)
        self.variables = parent_scope.variables.copy()
        self.scoped_variables = []

    def _added_var(self, var: str):
        self.scoped_variables.append(var)


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
    with open(pathlib.Path("build", f"{name}.json"), "w") as fp:
        states = sfn.State.find_reachable_states(start, include_error_handlers=True)
        states.sort(
            key=lambda state: int(state.id.split("[")[1].split("]")[0].split(":")[1])
        )
        json.dump(
            {"StartAt": start.id, "States": {s.id: s.to_state_json() for s in states},},
            fp,
            indent=4,
        )


def update_param_name(key, value):
    if isinstance(value, str) and value.startswith("States"):
        return f"{key}.$"
    else:
        return key
