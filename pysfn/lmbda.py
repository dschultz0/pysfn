import os
import pathlib
import shutil
from dataclasses import dataclass
from typing import List, Mapping, Union, Callable, Any, Optional, Iterable, Type
from aws_cdk import (
    aws_lambda as lmbda,
    Duration,
    Stack,
)
import shortuuid
from .function import gather_function_attributes


OPERATION_KEYWORD = "pysfn_operation"


@dataclass
class LambdaDefinition:
    func: lmbda.Function
    input: Mapping[str, Any]
    output: Mapping[str, Any]

    def get_return_vars(self) -> Iterable[str]:
        if self.output is None:
            return arg_iter()
        else:
            return iter(self.output.keys())


@dataclass
class LauncherFunction:
    func: Callable
    launcher_name: str
    name: str
    module: str
    input: Mapping[str, Type]
    output: Mapping[str, Type]

    def to_config(self):
        return (
            "{"
            + f'"function": {self.module}.{self.name}, '
            + f'"args": {list(self.input.keys())}, '
            + f'"return_args": {list(self.output.keys())}'
            + "}"
        )


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
        self.build_path = pathlib.Path(
            os.getcwd(), "build", id_.lower().replace(" ", "_")
        )

    def register(
        self,
        func: Callable,
        name: str = None,
        return_vars: Optional[Union[List[str], Mapping[str, Type]]] = None,
    ):
        f_attrs = gather_function_attributes(
            func, None, return_vars, src_dir=os.path.split(self.path)[1]
        )
        definition = LauncherFunction(
            func=func,
            launcher_name=name,
            name=f_attrs.name,
            module=f_attrs.module,
            input=f_attrs.input,
            output=f_attrs.output,
        )
        if definition.name in self.functions:
            raise Exception(f"Multiple functions with the same name: {definition.name}")
        self.functions[definition.name] = definition
        # TODO: Throw an error if the create_construct() method hasn't been called before calling this
        func.get_lambda = lambda: self.lmbda
        func.get_additional_params = lambda: {OPERATION_KEYWORD: definition.name}
        func.definition = definition
        return func

    def create_construct(self):
        module_name = "pysfn_launcher"
        file_path = pathlib.Path(self.build_path, module_name + ".py")
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
                f"    definition = launchers[event['{OPERATION_KEYWORD}']]",
                "    kwargs = {a: event[a] for a in definition['args'] if a in event}",
                "    print(kwargs)",
                "    result = definition['function'](**kwargs)",
                "    return_args = definition.get('return_args', [])",
                "    if isinstance(result, tuple):",
                "        result = {k: v for k, v in zip(return_args, result)}",
                "    elif return_args:",
                "        result = {return_args[0]: result}",
                "    print(result)",
                "    return result",
                "",
            ]
        )
        import_code = [f"import {m}" for m in modules] + ["from typing import Mapping"]
        code = import_code + ["", ""] + launch_code
        if self.build_path.exists():
            shutil.rmtree(self.build_path)
        self.build_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(self.path, self.build_path)

        with open(file_path, "w") as fp:
            fp.write("\n".join(code))
        self.lmbda = lmbda.Function(
            self.stack,
            self.id_,
            function_name=self.name,
            code=lmbda.Code.from_asset(str(self.build_path)),
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
            stack, f"{layer.split(':')[-2]}{shortuuid.uuid()[:8]}", layer,
        )
    else:
        return layer


def arg_iter():
    i = 0
    while True:
        yield f"arg{i}"
        i += 1
