import inspect
import os
from dataclasses import dataclass, field
from typing import List, Mapping, Union, Callable, Any, Optional, Iterable
from aws_cdk import (
    aws_lambda as lmbda,
    Duration,
    Stack,
)
import shortuuid
from .util import get_function_return_vars
from aws_cdk.aws_stepfunctions import JsonPath


@dataclass
class LambdaDefinition:
    func: lmbda.Function
    args: Mapping[str, Any]
    return_annotation: Mapping[str, Any]
    return_vars: List[str] = field(init=False)

    def __post_init__(self):
        self.return_vars = (
            list(self.return_annotation.keys()) if self.return_annotation else None
        )

    def get_return_vars(self) -> Iterable[str]:
        if self.return_vars is None:
            return arg_iter()
        else:
            return iter(self.return_vars)


@dataclass
class LauncherDefinition:
    func: Callable
    name: str = None
    src_dir: str = None
    return_vars: Union[None, List[str]] = None
    args: Mapping[str, Any] = field(init=False)
    return_annotation: Mapping[str, Any] = field(init=False)

    def __post_init__(self):
        if self.name is None:
            # TODO: Need to assign a better default name...
            self.name = self.func.__name__
        arg_spec = inspect.getfullargspec(self.func)
        self.args = {a: arg_spec.annotations.get(a) for a in arg_spec.args}

        # Build the return_annotation values based on three sources if any or all are present
        ret_annotation = arg_spec.annotations.get("return")
        ret_annotation = (
            list(ret_annotation)
            if isinstance(ret_annotation, tuple)
            else (None if ret_annotation is None else [ret_annotation])
        )
        ret_args = get_function_return_vars(self.func)
        ret_vars = self.return_vars
        if ret_vars:
            self.return_annotation = self._build_return_annotation(
                ret_vars, ret_annotation
            )
        elif ret_args:
            self.return_annotation = self._build_return_annotation(
                ret_args, ret_annotation
            )
        elif ret_annotation:
            self.return_vars = [f"arg{i}" for i in range(len(ret_annotation))]
            self.return_annotation = {
                k: v for k, v in zip(self.return_vars, ret_annotation)
            }
        else:
            self.return_vars = None
            self.return_annotation = {}

    def get_return_vars(self) -> Iterable[str]:
        if self.return_vars is None:
            return arg_iter()
        else:
            return self.return_vars

    def _build_return_annotation(self, var_list, annotated_types):
        if annotated_types:
            if len(var_list) == len(annotated_types):
                return {k: v for k, v in zip(var_list, annotated_types)}
            else:
                raise Exception(
                    f"Mismatch between the number of return values and the return annotation in function {self.name}"
                )
        else:
            return {k: None for k in var_list}

    @property
    def module(self):
        # TODO: Explore a better alternative to this approach for ensuring valid import path
        module_parts = self.func.__module__.split(".")
        if module_parts[0] == self.src_dir:
            return ".".join(module_parts[1:])
        else:
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

    def register(
        self, func: Callable, name: str = None, return_vars: Optional[List[str]] = None
    ):
        definition = LauncherDefinition(
            func, name, os.path.split(self.path)[1], return_vars
        )
        if definition.name in self.functions:
            raise Exception(f"Multiple functions with the same name: {definition.name}")
        self.functions[definition.name] = definition
        # TODO: Throw an error if the create_construct() method hasn't been called before calling this
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
                "    if isinstance(result, tuple):",
                "        result = {f'arg{i}': r for i, r in enumerate(result)}",
                "    else:",
                "        result = {'arg0': result}",
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


def shortid():
    return shortuuid.uuid()[:8]


def arg_iter():
    i = 0
    while True:
        yield f"arg{i}"
        i += 1
