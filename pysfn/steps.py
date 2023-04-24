import inspect
import pathlib
import ast
import json
import typing
import time

from .function import gather_function_attributes, FunctionAttributes
import dataclasses
from dataclasses import dataclass, is_dataclass, fields
from types import BuiltinFunctionType, FrameType
from typing import Dict, List, Callable, Mapping, Any, Union, Iterable, Optional, Type

from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    Duration,
    Stack,
)
from aws_cdk.aws_stepfunctions import JsonPath

from .condition import build_condition
from .service_operations import service_operations

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


def execution_start_time() -> str:
    pass


def state_entered_time() -> str:
    pass


def await_token(
    func: Callable,
    return_args: Union[List[str], Mapping[str, Type]],
    duration: Duration = None,
):
    pass


@dataclass
class SFNError(Exception):
    error: str
    cause: str = ""

    @classmethod
    def error(cls, error: str):
        return cls(error)


def state_machine(
    cdk_stack: Stack,
    sfn_name: str,
    local_values=None,  # DEPRECATED
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
        stack = inspect.stack()
        try:
            fts = FunctionToSteps(
                cdk_stack, func_attrs, stack[1].frame, skip_pass=skip_pass
            )
            print(sfn_name)
            func.state_machine = sfn.StateMachine(
                cdk_stack,
                sfn_name,
                state_machine_name=sfn_name,
                state_machine_type=sfn.StateMachineType.EXPRESS
                if express
                else sfn.StateMachineType.STANDARD,
                definition=fts.build_sfn_definition()[0],
            )
            func.output = func_attrs.output
            if fts.additional_policies:
                for policy in fts.additional_policies:
                    func.state_machine.add_to_role_policy(policy)
        finally:
            del stack
        return func

    return decorator


class FunctionToSteps:
    def __init__(
        self,
        cdk_stack: Stack,
        func_attrs: FunctionAttributes,
        frame: FrameType,
        skip_pass=True,
    ):
        global SFN_INDEX
        self.cdk_stack = cdk_stack
        self.func = func_attrs.func
        self.output = func_attrs.output
        self.local_values = frame.f_locals
        self.global_values = frame.f_globals
        self.state_number = 0
        self.sfn_number = SFN_INDEX
        self.skip_pass = skip_pass
        self.additional_policies = []
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
        return start, next_

    def get_frame_value(self, name):
        return self.local_values.get(name, self.global_values.get(name))


class SFNScope:
    # TODO: Fix all of the function call handling which largely ignores keyword args

    def __init__(self, fts: FunctionToSteps):
        self.fts = fts
        self.cdk_stack = fts.cdk_stack
        self.state_name = fts.state_name
        self.variables: Dict[str, typing.Type] = {}
        self.output = fts.output
        self.parent_scope = None

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

    def build_register_assignment(
        self, values: Dict, register_path: str = "", value_types=None
    ):
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
                self.add_var(
                    key,
                    var_type=value_types.get(key, typing.Any)
                    if value_types
                    else typing.Any,
                )

        params = {update_param_name(k, v): v for k, v in params.items()}
        return params

    def add_var(self, key: str, var_type: Type = Any):
        self.variables[key] = var_type
        self._added_var(key)

    def _added_var(self, var: str):
        pass

    def _updated_var(self, var: str):
        if self.parent_scope:
            self.parent_scope._updated_var(var)

    def handle_body(self, body: List[ast.stmt]) -> (List[sfn.IChainable], Callable):
        chain = []
        next_ = None
        # If the first statement is a function doc string we can drop it
        if (
            len(body) > 0
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
        ):
            body = body[1:]
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
            # TODO: Revisit this hack for dataclasses
            if isinstance(stmt.value, ast.Call):
                call = stmt.value
                if isinstance(call.func, ast.Name):
                    func = self.fts.get_frame_value(call.func.id)

                    if is_dataclass(func):
                        dc_fields = fields(func)
                        # print(f"Dataclass {func}")
                        # print("fields:")
                        # print([f.name for f in dc_fields])
                        if len(call.args) == 1:
                            arg = call.args[0]
                            if isinstance(arg, ast.Starred):
                                if isinstance(arg.value, ast.Call):
                                    return self.handle_call_function(
                                        arg.value, stmt, func
                                    )
                        value = self.build_dataclass_default_structure(func)
                        for key, val in zip(value.keys(), call.args):
                            value[key] = val
                        for key in call.keywords:
                            value[key.arg] = self.generate_value_repr(key.value)
                        self.validate_dataclass_values(value)
                        # print("Mapped object")
                        # print(value)
                        # TODO Fix this hack that assumes a non-annotated assignment
                        target = stmt.targets[0].id
                        # print(self.build_register_assignment({target: value}))
                        assign = sfn.Pass(
                            self.cdk_stack,
                            self.state_name(f"Assign {target}"),
                            input_path="$.register",
                            result_path="$.register",
                            parameters=self.build_register_assignment({target: value}),
                        )
                        return [assign], assign.next
                    else:
                        return self.handle_call_function(call, stmt)
            if isinstance(stmt.value, ast.ListComp):
                return self.handle_list_comp(stmt)
            elif stmt.value:
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
        elif isinstance(stmt, ast.While):
            return self.handle_while(stmt)
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
        elif isinstance(stmt, ast.Raise):
            if (
                isinstance(stmt.exc, ast.Call)
                and isinstance(stmt.exc.func, ast.Name)
                and stmt.exc.func.id == "SFNError"
                and len(stmt.exc.args) >= 1
            ):
                args = [self.map_arg(a, "register.") for a in stmt.exc.args]
                fail_step = sfn.Fail(
                    self.cdk_stack,
                    self.state_name("Raise error"),
                    error=args[0],
                    cause=args[1] if len(args) > 1 else None,
                )
                return [fail_step], None
            elif isinstance(stmt.exc, ast.Name):
                prefix = f"$.register.{stmt.exc.id}"
                fail_step = sfn.Fail(
                    self.cdk_stack,
                    self.state_name("Raise error"),
                    error=f"{prefix}.Error",
                    cause=f"{prefix}.Cause",
                )
                return [fail_step], None

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
        elif (
            isinstance(handler.type, ast.Call)
            and isinstance(handler.type.func, ast.Attribute)
            and isinstance(handler.type.func.value, ast.Name)
            and handler.type.func.value.id == "SFNError"
        ):
            if all([isinstance(a, ast.Constant) for a in handler.type.args]):
                args = [a.value for a in handler.type.args]
                return CatchHandler(args, chain, result_path=result_path)
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
    def map_arg(arg: ast.expr, var_path: str = ""):
        if isinstance(arg, ast.Name):
            return f"$.{var_path}{arg.id}"
        elif isinstance(arg, ast.Constant):
            return arg.value
        elif (
            isinstance(arg, ast.Subscript)
            and isinstance(arg.value, ast.Name)
            and isinstance(arg.slice, ast.Constant)
        ):
            if isinstance(arg.slice.value, int):
                return f"$.{var_path}{arg.value.id}[{arg.slice.value}]"
            else:
                return f"$.{var_path}{arg.value.id}.{arg.slice.value}"
        else:
            raise Exception("Args must be Name or Constant")

    def _get_iterator(self, iterator: ast.expr) -> (str, str, sfn.State, int):
        # TODO: Support enumerate
        # TODO: Support callable iterator
        max_concurrency = 1
        iterator_step = None
        has_index = False

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

        if isinstance(iterator, ast.Call) and self._is_intrinsic_function(iterator):
            items_path, iter_var = self._intrinsic_function(iterator)
            iterator_step = sfn.Pass(
                self.cdk_stack,
                self.state_name(f"Build {iter_var}"),
                input_path="$.register",
                result_path="$.iter",
                parameters={iter_var: items_path},
            )
            iter_var = iterator.func.id
            items_path = f"$.iter.{iter_var}"
        elif (
            isinstance(iterator, ast.Call)
            and isinstance(iterator.func, ast.Name)
            and iterator.func.id == "enumerate"
        ):
            if len(iterator.args) == 1 and isinstance(iterator.args[0], ast.Name):
                iter_var = iterator.args[0].id
                items_path = f"$.register.{iter_var}"
                has_index = True
            else:
                raise Exception("enumerate can only be used with variable values")
        elif isinstance(iterator, ast.Call) and isinstance(iterator.func, ast.Name):
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
        return iter_var, items_path, iterator_step, max_concurrency, has_index

    def handle_while(self, stmt: ast.While):
        condition, name = build_condition(stmt.test)
        choice = sfn.Choice(
            self.cdk_stack, self.state_name(name.replace("If", "While"))
        )

        def if_next(step):
            choice.when(condition, step)

        if_c, if_n = ChildScope(self).handle_body(stmt.body)
        if stmt.orelse:
            raise Exception("While else isn't currently supported")
        if_n = advance(if_next, if_c, if_n)
        advance(if_n, choice, None)
        chain = [choice]
        if if_c:
            chain.extend(if_c)
        return chain, [choice.otherwise]

    def handle_for(self, stmt: ast.For):
        # print(ast.dump(stmt))
        # print()
        (
            iter_var,
            items_path,
            iterator_step,
            max_concurrency,
            has_index,
        ) = self._get_iterator(stmt.iter)
        index_name = None
        if (
            isinstance(stmt.target, ast.Tuple)
            and len(stmt.target.elts) == 2
            and has_index
        ):
            index_name = stmt.target.elts[0].id
            target_name = stmt.target.elts[1].id
        elif isinstance(stmt.target, ast.Name):
            target_name = stmt.target.id
        else:
            raise Exception("Unsupported for-loop target, variables only")

        map_parameters = {
            "register.$": f"$.register",
            f"{target_name}.$": "$$.Map.Item.Value",
        }
        if index_name:
            map_parameters[f"{index_name}.$"] = "$$.Map.Item.Index"

        map_state = sfn.Map(
            self.cdk_stack,
            self.state_name(f"For {iter_var}"),
            max_concurrency=max_concurrency,
            items_path=items_path,
            parameters=map_parameters,
            result_path="$.register.loopResult",
        )

        # Create a scope for the for loop contents and build the contained steps
        # Also build an entry step to capture the iterator target
        map_scope = MapScope(self)
        entry_step = map_scope.build_entry_step(target_name, index_name)
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
        elif (
            isinstance(arg, ast.Subscript)
            and isinstance(arg.value, ast.Name)
            and isinstance(arg.slice, ast.Constant)
        ):
            value = f"{arg.value.id}.{arg.slice.value}"
            path_to_add = f"$.register.{value}"
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
        assignment_type = typing.Any
        if isinstance(stmt, ast.AnnAssign):
            if isinstance(stmt.target, ast.Name):
                var_name = stmt.target.id
                # TODO: Handle subscripted annotations such as typing.List
                if isinstance(stmt.annotation, ast.Name):
                    assignment_type = self.fts.get_frame_value(stmt.annotation.id)
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
            if len(stmt.targets) == 1:
                target = stmt.targets[0]
                if isinstance(target, ast.Name):
                    var_name = target.id
                elif (
                    isinstance(target, ast.Subscript)
                    and isinstance(target.value, ast.Name)
                    and isinstance(target.slice, ast.Constant)
                ):
                    var_name = target.value.id
                    sub_target = target.slice.value

                # dataclass attribute assignment
                elif isinstance(target, ast.Attribute) and isinstance(
                    target.value, ast.Name
                ):
                    var_name = target.value.id
                    sub_target = target.attr
                else:
                    raise Exception("Unexpected assignment")
            else:
                raise Exception("More targets than expected")
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
                    {f"{var_name}": f"States.JsonMerge($.{var_name}, $.itm, false)"},
                    value_types={var_name: assignment_type},
                ),
            )
            prep.next(assign)
            return [prep, assign], assign.next

        # Handling for array addition
        elif isinstance(stmt.value, ast.BinOp) and isinstance(stmt.value.op, ast.Add):
            op = stmt.value
            if isinstance(op.left, ast.Name):
                left_name = op.left.id
                left_type = self.variables.get(op.left.id)
            else:
                raise Exception("Unexpected left value in value addition")
            if isinstance(op.right, ast.Name):
                right_name = op.right.id
                right_type = self.variables.get(op.right.id)
            else:
                raise Exception("Unexpected right value in value addition")
            if left_type == typing.List and right_type == typing.List:
                list_step = sfn.Pass(
                    self.cdk_stack,
                    self.state_name(f"Add {left_name} and {right_name}"),
                    result_path="$.meta",
                    parameters={
                        "arrayConcat": JsonPath.string_at(
                            f"States.Array($.register.{left_name}, $.register.{right_name})"
                        )
                    },
                )
                flatten_step = sfn.Pass(
                    self.cdk_stack,
                    self.state_name(f"Flatten {var_name}"),
                    result_path="$.register",
                    parameters=self.build_register_assignment(
                        {var_name: JsonPath.string_at("$.meta.arrayConcat[*][*]")},
                        "register.",
                    ),
                )
                list_step.next(flatten_step)
                return [list_step, flatten_step], flatten_step.next
            else:
                raise Exception("Addition is currently only supported for arrays")
        else:
            value = self.generate_value_repr(stmt.value)
            assign = sfn.Pass(
                self.cdk_stack,
                self.state_name(f"Assign {var_name}"),
                input_path="$.register",
                result_path="$.register",
                parameters=self.build_register_assignment(
                    {var_name: value}, value_types={var_name: assignment_type}
                ),
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

    def _is_intrinsic_function(self, call: ast.Call):
        return isinstance(call.func, ast.Name) and call.func.id in [
            "range",
            "len",
            "execution_start_time",
            "state_entered_time",
        ]

    def _intrinsic_function(self, call: ast.Call, var_path: str = ""):
        if isinstance(call.func, ast.Name):
            args = [self.map_arg(arg, var_path) for arg in call.args]
            if call.func.id == "range":
                start_val = 0
                end_val = 0
                step_val = 1
                if len(args) == 1:
                    end_val = args[0]
                elif len(args) >= 2:
                    start_val = args[0]
                    end_val = args[1]
                if len(args) == 3:
                    step_val = args[2]
                return (
                    JsonPath.string_at(
                        f"States.ArrayRange({start_val}, States.MathAdd({end_val}, -1), {step_val})"
                    ),
                    "range",
                )
            elif call.func.id == "len":
                if len(args) == 1:
                    return (
                        JsonPath.string_at(f"States.ArrayLength({args[0]})"),
                        "len",
                    )
            elif call.func.id == "execution_start_time":
                return (
                    JsonPath.string_at(f"$$.Execution.StartTime"),
                    "time",
                )
            elif call.func.id == "state_entered_time":
                return (
                    JsonPath.string_at(f"$$.State.EnteredTime"),
                    "time",
                )
        raise Exception("Cannot handle intrinsic function")

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
            func = self.fts.get_frame_value(call.func.id)
            result_prefix = ""

            # Build the parameters
            if func:
                if func in service_operations:
                    params = self.build_parameters(call, func, False)
                elif func in [time.sleep, event, await_token]:
                    params = {}
                else:
                    params = self.build_parameters(call, func)
                    if hasattr(func, "get_additional_params"):
                        params.update(func.get_additional_params())
            else:
                raise Exception(f"Unable to find function {call.func.id}")

            if func == time.sleep:
                invoke = sfn.Wait(
                    self.cdk_stack,
                    self.state_name("Wait"),
                    time=sfn.WaitTime.duration(Duration.seconds(call.args[0].value)),
                )
                return_vars = []
            elif (
                func == event
                and len(call.args) > 0
                and isinstance(call.args[0], ast.Call)
            ):
                return self._build_func_call(
                    call.args[0], result_path=result_path, invoke_event_=True
                )
            elif (
                func == await_token
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
                    input_path="$.register",
                    result_path=result_path,
                    integration_pattern=sfn.IntegrationPattern.RUN_JOB,
                    input=sfn.TaskInput.from_object(params),
                )
                return_vars = list(func.output.keys())
                result_prefix = ".Output"
            elif func in service_operations:
                invoke = func.builder(
                    self.cdk_stack, self.state_name(func.step_name), **params
                )
                if hasattr(func, "additional_policies") and func.additional_policies:
                    self.fts.additional_policies.extend(func.additional_policies)
                return_vars = func.return_vars
                result_prefix = ""
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
        self,
        call: ast.Call,
        assign: Union[ast.Assign, ast.AnnAssign] = None,
        dataclass_=None,
    ) -> (List[sfn.IChainable], Callable):
        if self._is_intrinsic_function(call):
            if not assign:
                raise Exception(
                    f"Call to intrinsic function {call.func.id} must be assigned to a value"
                )
            val, name = self._intrinsic_function(call)
            target = (
                assign.targets[0] if isinstance(assign, ast.Assign) else assign.target
            )
            if isinstance(target, ast.Name):
                result_target = target.id
            else:
                raise Exception(
                    f"Invalid intrinsic function target {target} for call {call.func.id}"
                )

            register = sfn.Pass(
                self.cdk_stack,
                self.state_name(f"Register {name}"),
                input_path="$.register",
                result_path="$.register",
                parameters=self.build_register_assignment({result_target: val}),
            )
            return [register], register.next
        else:
            invoke, return_vars, name, result_prefix = self._build_func_call(
                call, "$.register.out"
            )
            chain = [invoke]
            next_ = invoke.next
            context_target = None
            CONTEXT_TARGET_NAMES = {"__execution_arn": "ExecutionArn"}

            if assign:
                # Get the result variable names
                target = (
                    assign.targets[0]
                    if isinstance(assign, ast.Assign)
                    else assign.target
                )
                if isinstance(target, ast.Name):
                    result_targets = [target.id]
                elif isinstance(target, ast.Tuple) and all(
                    isinstance(t, ast.Name) for t in target.elts
                ):
                    result_targets = [n.id for n in target.elts]
                    if result_targets[0] in CONTEXT_TARGET_NAMES:
                        context_target = result_targets[0]
                        result_targets = result_targets[1:]
                else:
                    raise Exception(
                        f"Unexpected result target of type {type(assign.target)}"
                    )
                if dataclass_:
                    # print("dataclass assignment")
                    value = self.build_dataclass_default_structure(dataclass_)
                    for key, val in zip(value.keys(), return_vars):
                        value[key] = JsonPath.string_at(f"$.out.{val}")
                    self.validate_dataclass_values(value)
                    # print("Mapped object")
                    # print(value)
                    # TODO Fix this hack that assumes a non-annotated assignment
                    # print(self.build_register_assignment({target.id: value}))
                    register = sfn.Pass(
                        self.cdk_stack,
                        self.state_name(f"Register {call.func.id}"),
                        input_path="$.register",
                        result_path="$.register",
                        parameters=self.build_register_assignment({target.id: value}),
                    )
                    invoke.next(register)
                    chain.append(register)
                    next_ = register.next
                else:
                    result_params = {
                        v: JsonPath.string_at(f"$.out{result_prefix}.{r}")
                        for v, r in zip(result_targets, return_vars)
                    }
                    if context_target:
                        result_params[context_target] = JsonPath.string_at(
                            f"$.out.{CONTEXT_TARGET_NAMES[context_target]}"
                        )
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

    def _return_value(self, value: Union[ast.expr, ast.stmt]):
        if isinstance(value, ast.Name):
            return JsonPath.string_at(f"$.register.{value.id}")
        elif isinstance(value, ast.Constant):
            return value.value
        elif isinstance(value, ast.Call) and self._is_intrinsic_function(value):
            val, name = self._intrinsic_function(value)
            return val
        elif (
            isinstance(value, ast.Subscript)
            and isinstance(value.value, ast.Name)
            and isinstance(value.slice, ast.Constant)
        ):
            return JsonPath.string_at(
                f"$.register.{value.value.id}.{value.slice.value}"
            )
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

    def evaluate_path(self, stmt: ast.Subscript) -> str:
        if isinstance(stmt.slice, ast.Constant):
            if isinstance(stmt.slice.value, int):
                slce = f"[{stmt.slice.value}]"
            else:
                slce = f".{stmt.slice.value}"
        else:
            raise Exception("Subscript slice that's not a Constant")
        if isinstance(stmt.value, ast.Name):
            return stmt.value.id + slce
        elif isinstance(stmt.value, ast.Subscript):
            return self.evaluate_path(stmt.value) + slce
        else:
            raise Exception("Unexpected Subscript value")

    def generate_value_repr(self, arg_value, gen_jsonpath=True):
        if isinstance(arg_value, ast.Name):
            # if arg_value.id not in self.variables:
            #    raise Exception(f"Undefined variable {arg_value.id}")
            expr = f"$.{arg_value.id}"
            return JsonPath.string_at(expr) if gen_jsonpath else expr
        elif isinstance(arg_value, ast.Constant):
            # TODO: Investigate this more, it appears that values of None are getting dropped altogether rather than using null
            return arg_value.value if arg_value.value is not None else ""
        elif isinstance(arg_value, ast.List):
            expr = [self.generate_value_repr(val, False) for val in arg_value.elts]
            return JsonPath.array(*expr) if gen_jsonpath else expr
        elif isinstance(arg_value, ast.Subscript):
            expr = "$." + self.evaluate_path(arg_value)
            return JsonPath.string_at(expr) if gen_jsonpath else expr
        elif isinstance(arg_value, ast.Call) and self._is_intrinsic_function(arg_value):
            path, name = self._intrinsic_function(arg_value)
            return path
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
        elif (
            isinstance(arg_value, ast.Call)
            and isinstance(arg_value.func, ast.Attribute)
            and (
                (
                    isinstance(arg_value.func.value, ast.Name)
                    and arg_value.func.value.id == "JsonPath"
                )
                or (
                    isinstance(arg_value.func.value, ast.Attribute)
                    and arg_value.func.value.attr == "JsonPath"
                )
            )
        ):
            return getattr(JsonPath, arg_value.func.attr)(
                *[self.generate_value_repr(arg, gen_jsonpath) for arg in arg_value.args]
            )
        elif (
            isinstance(arg_value, ast.Attribute)
            and isinstance(arg_value.value, ast.Name)
            and arg_value.value.id == "self"
        ):
            s = self.fts.get_frame_value(arg_value.value.id)
            var = s.__getattribute__(arg_value.attr)
            return var
        elif isinstance(arg_value, ast.Call) and (
            (
                isinstance(arg_value.func, ast.Attribute)
                and isinstance(arg_value.func.value, ast.Name)
                and arg_value.func.value.id == "JsonPath"
            )
        ):
            # TODO: Handle multi arg inputs
            arg = self.generate_value_repr(arg_value.args[0], gen_jsonpath=True)
            return getattr(JsonPath, arg_value.func.attr)(arg)
        else:
            print(ast.dump(arg_value, indent=2))
            raise Exception(f"Unexpected argument: {ast.dump(arg_value)}")

    def build_dataclass_default_structure(self, dc):
        dc_fields = dataclasses.fields(dc)
        value = {}
        for f in dc_fields:
            if f.default is dataclasses.MISSING:
                if f.default_factory is dataclasses.MISSING:
                    value[f.name] = Ellipsis
                elif f.default_factory is list:
                    value[f.name] = []
                else:
                    raise Exception("Unsupported dataclass default factory")
            else:
                value[f.name] = self.generate_value_repr(ast.Constant(f.default))
        return value

    @staticmethod
    def validate_dataclass_values(values: Dict):
        for key, value in values.items():
            if value is Ellipsis:
                raise Exception(f"Missing required value for attribute: {key}")


class MapScope(SFNScope):
    def __init__(self, parent_scope: SFNScope):
        super(MapScope, self).__init__(parent_scope.fts)
        self.variables = parent_scope.variables.copy()
        self.scoped_variables = set()
        self._updated_vars = set()
        self.parent_scope = parent_scope

    def _added_var(self, var: str):
        self.scoped_variables.add(var)

    def _updated_var(self, var: str):
        self._updated_vars.add(var)

    def build_entry_step(self, entry_var: str, index_var: str = None):
        values = {entry_var: JsonPath.string_at(f"$.{entry_var}")}
        if index_var:
            values[index_var] = JsonPath.string_at(f"$.{index_var}")
        return sfn.Pass(
            self.cdk_stack,
            self.state_name("Register loop value"),
            result_path="$.register",
            parameters=self.build_register_assignment(values, "register."),
        )

    @property
    def updated_vars(self):
        return [v for v in self._updated_vars if v not in self.scoped_variables]


class ChildScope(SFNScope):
    def __init__(self, parent_scope: SFNScope):
        super(ChildScope, self).__init__(parent_scope.fts)
        self.variables = parent_scope.variables.copy()
        self.scoped_variables = []
        self.parent_scope = parent_scope

    def _added_var(self, var: str):
        self.scoped_variables.append(var)
        if self.parent_scope:
            self.parent_scope._added_var(var)


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
                if i and callable(i):
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
            key=lambda s: int(s.id[s.id.rindex("[") :].split("]")[0].split(":")[1])
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


def get_call_args(call: ast.Call) -> (List, Mapping):
    args = []
    kwargs = {}
    for arg in call.args:
        args.append(arg)
    for kw in call.keywords:
        kwargs[kw.arg] = kw.value
    return args, kwargs
