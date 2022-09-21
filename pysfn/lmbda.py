from typing import List, Mapping, Union, Callable
from aws_cdk import (
    aws_dynamodb as dynamodb,
    aws_iam as iam,
    aws_lambda as lmbda,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    Duration,
    Stack,
)
from aws_cdk.aws_stepfunctions import IntegrationPattern, JsonPath
from util import shortid


class PythonLambda:
    PYTHON_2_7 = lmbda.Runtime.PYTHON_2_7
    PYTHON_3_6 = lmbda.Runtime.PYTHON_3_6
    PYTHON_3_7 = lmbda.Runtime.PYTHON_3_7
    PYTHON_3_8 = lmbda.Runtime.PYTHON_3_8
    PYTHON_3_9 = lmbda.Runtime.PYTHON_3_9

    def __init__(
        self,
        stack,
        id_,
        path,
        role,
        runtime,
        timeout_minutes,
        memory_mb,
        layers=None,
        environment=None,
        name=None,
    ):
        self.functions = {}

        # In all likelihood we'll be moving this to a final step that will be invoked after
        # all of the methods have been registered
        self.lmbda = lmbda.Function(
            stack,
            id_,
            function_name=name if name else id_,
            code=lmbda.Code.from_asset(path),
            handler="root.launcher",  # A bit of work here to support a multi-target launcher
            runtime=runtime,
            role=role,
            timeout=Duration.minutes(timeout_minutes),
            layers=[resolve_layer(l, stack) for l in layers] if layers else None,
            memory_size=int(memory_mb * 1024),
            environment=environment,
        )

    def register(self, func: Callable, name: str = None):
        if name is None:
            # TODO: Need to assign a better default name...
            name = func.__name__
        if name in self.functions:
            raise Exception(f"Multiple functions with the same name: {name}")
        self.functions[name] = func

        # TODO: Update this to push the function signature and annotations into the wrapper
        def wrapped_function(*args, **kwargs):
            return func(*args, **kwargs)

        wrapped_function.lmbda = self.lmbda
        wrapped_function.function_name = name
        return wrapped_function


def function_for_lambda(
    lmbda_func: lmbda.Function,
    inputs: Union[List[str], Mapping],
    output: Union[List[str], Mapping],
):
    # TODO: Update this to push the function signature and annotations into the wrapper
    def pseudo_function(*args, **kwargs):
        return None

    pseudo_function.lmbda = lmbda_func

    return pseudo_function


def resolve_layer(layer, stack):
    if isinstance(layer, str):
        return lmbda.LayerVersion.from_layer_version_arn(
            stack, f"{layer.split(':')[-2]}{shortid()}", layer,
        )
    else:
        return layer
