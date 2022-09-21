import dis
import inspect
from collections import deque
from typing import Dict, Set, List, Callable, Mapping, Any, Deque, Union
from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    Duration,
    Stack,
)
from aws_cdk.aws_stepfunctions import IntegrationPattern, JsonPath
from util import shortid


def state_machine(cdk_stack: Stack, sfn_name: str, express=False):
    def decorator(func):
        fts = FunctionToSteps(cdk_stack, func)
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
    def __init__(self, cdk_stack: Stack, func: Callable):
        self.cdk_stack = cdk_stack
        self.func = func

        dis.dis(self.func)
        self.instructions: Deque[dis.Instruction]
        self.instructions = deque(dis.get_instructions(self.func))
        # print(list(self.instructions))
        self.instructions.reverse()

        self.frame: Deque[dis.Instruction]
        self.frame = deque()

        self.next_: Union[Callable, List[Callable], None]
        self.next_ = None

        self.req_params, self.opt_params = self._get_parameters()
        self.variables: Set
        self.variables = set(self.req_params)

    def build_sfn_definition(self):

        # The first step will always be to put the inputs on the register
        start = sfn.Pass(
            self.cdk_stack,
            f"RegisterInput {shortid()}",
            result_path=JsonPath.string_at("$.register"),
        )
        self.next_ = start.next

        # For optional parameters we'll check if they are present and default them if they aren't
        self._add_optional_parameter_steps(self.opt_params)

        while len(self.instructions) > 0:
            inst = self.instructions.pop()

            # Handle single value variable assignment
            if inst.opcode == 125 and len(self.frame) == 1:
                self._set_variable(inst)
                self.frame.clear()

            # Handle function invocation
            elif inst.opcode == 131:
                self._invoke_function(inst)
                self.frame.clear()

            # Handle if statement
            elif inst.opcode == 114:  # POP_JUMP_IF_FALSE
                self._handle_if(inst)
                self.frame.clear()

            # Handle else
            elif inst.opcode == 110:  # JUMP_FORWARD
                self.frame.clear()

            else:
                self.frame.append(inst)
        return start

    def _get_parameters(self) -> (List[str], Mapping[str, Any]):
        sig = inspect.signature(self.func)
        req_params = [
            p.name for p in sig.parameters.values() if p.default == inspect._empty
        ]
        opt_params = {
            p.name: p.default
            for p in sig.parameters.values()
            if p.default != inspect._empty
        }
        return req_params, opt_params

    def _add_optional_parameter_steps(self, opt_params: Mapping[str, Any]):
        for name, value in opt_params.items():
            assign = sfn.Pass(
                self.cdk_stack,
                f"Assign {name} default {shortid()}",
                input_path=JsonPath.string_at("$.register"),
                result_path=JsonPath.string_at("$.register"),
                parameters=self.append_to_register_params({name: value}),
            )
            choice = sfn.Choice(self.cdk_stack, f"Has {name}")
            choice.when(sfn.Condition.is_not_present(f"$.register.{name}"), assign)
            self.next_step(choice, [choice.otherwise, assign.next])

    def _set_variable(self, inst):
        assign = sfn.Pass(
            self.cdk_stack,
            f"Assign {inst.argval} {shortid()}",
            input_path=JsonPath.string_at("$.register"),
            result_path=JsonPath.string_at("$.register"),
            parameters=self.append_to_register_params(
                {inst.argval: self.frame[0].argval}
            ),
        )
        self.next_step(assign, assign.next)

    def _handle_if(self, inst):
        if len(self.frame) == 1:
            arg = self.frame[0].argval
            # TODO: Handle a wider range of non-boolean single value conditions
            condition = sfn.Condition.boolean_equals(f"$.{arg}", True)
            name = f"Is {arg} {shortid()}"
        else:
            pass

    def _invoke_function(self, inst):
        # TODO: Add check that the function getting loaded (first step) is of the type we're interested in
        nxt = self.instructions.pop()
        response_values = []
        # If unpack sequence
        if nxt.opcode == 92:
            for i in range(nxt.argval):
                response_values.append(self.instructions.pop().argval)
        elif nxt.opcode == 125:
            response_values.append(nxt.argval)
        else:
            raise Exception(f"Unexpected action after function invocation {nxt.opcode}")
        func_name = self.frame.popleft().argval
        args = [i.argval for i in self.frame]
        id_ = shortid()
        invoke = sfn.Pass(
            self.cdk_stack,
            f"Call {func_name} {id_}",
            result_path=JsonPath.string_at("$.register.out"),
            parameters={a: JsonPath.string_at(f"$.register.{a}") for a in args},
            result=sfn.Result.from_object({v: v for v in response_values}),
        )
        register = sfn.Pass(
            self.cdk_stack,
            f"Register {func_name} {id_}",
            input_path=JsonPath.string_at("$.register"),
            result_path=JsonPath.string_at("$.register"),
            parameters=self.append_to_register_params(
                {v: JsonPath.string_at(f"$.out.{v}") for v in response_values},
            ),
        )
        invoke.next(register)
        self.next_step(invoke, register.next)

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

    def next_step(self, start, end):
        if isinstance(self.next_, list):
            for i in self.next_:
                i(start)
        else:
            self.next_(start)
        self.next_ = end
