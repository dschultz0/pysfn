import inspect
import os
from dataclasses import dataclass, field, asdict
from typing import List, Mapping, Union, Callable, Any
from aws_cdk import (
    aws_lambda as lmbda,
    Duration,
    Stack,
)
from aws_cdk.aws_stepfunctions import IntegrationPattern, JsonPath
from util import shortid


@dataclass
class LambdaDefinition:
    func: lmbda.Function
    args: Mapping[str, Any]
    return_annotation: Mapping[str, Any]

    def get_result_mapping(self, result_vars):
        return {
            v: JsonPath.string_at(f"$.out.Payload.{r}")
            for v, r in zip(result_vars, self.return_annotation.keys())
        }


@dataclass
class LauncherDefinition:
    func: Callable
    name: str = None
    args: Mapping[str, Any] = field(init=False)
    return_annotation: Mapping[str, Any] = field(init=False)

    def __post_init__(self):
        if self.name is None:
            # TODO: Need to assign a better default name...
            self.name = self.func.__name__
        arg_spec = inspect.getfullargspec(self.func)
        self.args = {a: arg_spec.annotations.get(a) for a in arg_spec.args}
        self.return_annotation = arg_spec.annotations.get("return")

    @property
    def module(self):
        return self.func.__module__

    @property
    def function_name(self):
        return self.func.__name__

    def to_config(self):
        return (
            "{"
            + f'"function": {self.module}.{self.function_name}, '
            + f'"args": {list(self.args.keys())}'
            + "}"
        )

    def get_result_mapping(self, result_vars):
        return {
            v: JsonPath.string_at(f"$.out.Payload.arg{i}")
            for i, v in enumerate(result_vars)
        }


class PythonLambda:
    PYTHON_2_7 = lmbda.Runtime.PYTHON_2_7
    PYTHON_3_6 = lmbda.Runtime.PYTHON_3_6
    PYTHON_3_7 = lmbda.Runtime.PYTHON_3_7
    PYTHON_3_8 = lmbda.Runtime.PYTHON_3_8
    PYTHON_3_9 = lmbda.Runtime.PYTHON_3_9

    def __init__(
        self,
        stack: Stack,
        id_: str,
        path: str,
        role,
        runtime,
        timeout_minutes,
        memory_mb,
        layers=None,
        environment=None,
        name=None,
    ):
        self.functions = {}
        self.stack = stack
        self.id_ = id_
        self.path = path
        self.role = role
        self.runtime = runtime
        self.timeout_minutes = timeout_minutes
        self.memory_size = int(memory_mb * 1024)
        self.layers = (
            [resolve_layer(layer, stack) for layer in layers] if layers else None
        )
        self.environment = environment
        self.name = name if name else id_
        self.lmbda = None

    def register(self, func: Callable, name: str = None):
        definition = LauncherDefinition(func, name)
        if definition.name in self.functions:
            raise Exception(f"Multiple functions with the same name: {definition.name}")
        self.functions[definition.name] = definition
        func.get_lambda = lambda: self.lmbda
        func.get_additional_params = lambda: {"launcher_target": definition.name}
        func.definition = definition
        return func

    def create_construct(self):
        module_name = f"{self.id_.lower().replace(' ', '_')}_pysfn_launcher"
        file_path = os.path.join(self.path, module_name + ".py")
        modules = set()
        launch_code = ["def launch(event, context):", "    launchers = {"]
        for name, definition in self.functions.items():
            modules.add(definition.module)
            launch_code.append(f"        '{name}': {definition.to_config()},")
        # TODO: Modify the launcher to appropriately provide the kw args and handle responses
        launch_code.extend(
            [
                "    }",
                "    print(event)",
                "    definition = launchers[event['launcher_target']]",
                "    kwargs = {a: event[a] for a in definition['args'] if a in event}",
                "    print(kwargs)",
                "    result = definition['function'](**kwargs)",
                "    if not isinstance(result, Mapping):",
                "        if isinstance(result, tuple):",
                "            result = {f'arg{i}': r for i, r in enumerate(result)}",
                "        else:",
                "            result = {'arg0': result}",
                "    print(result)",
                "    return result",
                "",
            ]
        )
        import_code = [f"import {m}" for m in modules] + ["from typing import Mapping"]
        code = import_code + ["", ""] + launch_code
        with open(file_path, "w") as fp:
            fp.write("\n".join(code))
        self.lmbda = lmbda.Function(
            self.stack,
            self.id_,
            function_name=self.name,
            code=lmbda.Code.from_asset(self.path),
            handler=f"{module_name}.launch",
            runtime=self.runtime,
            role=self.role,
            timeout=Duration.minutes(self.timeout_minutes),
            layers=self.layers,
            memory_size=self.memory_size,
            environment=self.environment,
        )
        return self.lmbda


def function_for_lambda(
    lmbda_func: lmbda.Function,
    inputs: Union[List[str], Mapping],
    output: Mapping[str, Any],
):
    # TODO: Update this to push the function signature and annotations into the wrapper
    def pseudo_function(*args, **kwargs):
        return None

    if isinstance(inputs, List):
        inputs = {a: None for a in inputs}
    pseudo_function.get_lambda = lambda: lmbda_func
    pseudo_function.definition = LambdaDefinition(lmbda_func, inputs, output)

    return pseudo_function


def resolve_layer(layer, stack):
    if isinstance(layer, str):
        return lmbda.LayerVersion.from_layer_version_arn(
            stack, f"{layer.split(':')[-2]}{shortid()}", layer,
        )
    else:
        return layer
