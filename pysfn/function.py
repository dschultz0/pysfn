import ast
import inspect
from typing import Callable, Type, Mapping, Union, List
from dataclasses import dataclass
from itertools import zip_longest, islice


def get_function_ast(func: Callable):
    # Retrieve the source code for the function with any indent removed
    src_code = inspect.getsource(func).split("\n")
    indent = len(src_code[0]) - len(src_code[0].lstrip())
    src_code = [c[indent:] for c in src_code]

    # Build the AST
    return ast.parse("\n".join(src_code))


@dataclass
class FunctionAttributes:
    func: Callable
    tree: ast.expr
    name: str
    module: str
    input: Mapping[str, Type]
    output: Mapping[str, Type]


def gather_function_attributes(
    func,
    in_: Union[List[str], Mapping[str, Type]] = None,
    out_: Union[List[str], Mapping[str, Type]] = None,
    src_dir: str = None,
):
    tree = get_function_ast(func)
    arg_spec = inspect.getfullargspec(func)

    # TODO: Explore a better alternative to this approach for ensuring valid import path
    module_parts = func.__module__.split(".")
    if module_parts[0] == src_dir:
        module = ".".join(module_parts[1:])
    else:
        module = func.__module__

    # Build inputs
    args = {a: arg_spec.annotations.get(a) for a in arg_spec.args}
    if in_ and len(in_) != len(args):
        raise Exception("Input argument count mismatch")
    if isinstance(in_, List):
        input_vars = {k: arg_spec.annotations.get(k) for k in in_}
    elif isinstance(in_, Mapping):
        input_vars = in_
    elif in_ is None:
        input_vars = args
    else:
        raise Exception("Invalid in_ parameter")

    # Build return vars
    return_annotation = arg_spec.annotations.get("return")
    if return_annotation is not None and not isinstance(return_annotation, tuple):
        return_annotation = (return_annotation,)
    return_vars = get_function_return_var_names(tree, func.__name__)
    if (out_ and len(return_vars) != len(out_)) or (
        out_ and return_annotation and len(out_) != len(return_annotation)
    ):
        raise Exception("out_ does not match return")
    if isinstance(out_, List):
        if return_annotation:
            output_vars = {k: v for k, v in zip(out_, return_annotation)}
        else:
            output_vars = {k: None for k in out_}
    elif isinstance(out_, Mapping):
        output_vars = out_
    elif out_ is None:
        if return_vars is None or len(return_vars) == 0:
            output_vars = {}
        else:
            return_vars = [v if v else f"arg{i}" for i, v in enumerate(return_vars)]
            if return_annotation:
                output_vars = {
                    k: v
                    for k, v in zip_longest(
                        return_vars, islice(return_annotation, len(return_vars))
                    )
                }
            else:
                output_vars = {k: None for k in return_vars}
    else:
        raise Exception("Invalid out_ parameter")

    return FunctionAttributes(
        func, tree, func.__name__, module, input_vars, output_vars
    )


def find_returns(node: ast.expr):
    rets = []
    if isinstance(node, ast.Return):
        return [node]
    if hasattr(node, "body"):
        for n in node.body:
            res = find_returns(n)
            if res:
                rets.extend(res)
    if hasattr(node, "handlers"):
        for n in node.handlers:
            res = find_returns(n)
            if res:
                rets.extend(res)
    return rets


def get_function_return_var_names(tree: ast.expr, name: str):
    rets = find_returns(tree)
    names = None
    for ret in rets:
        if ret and isinstance(ret.value, ast.Name):
            if names is not None and len(names) != 1:
                raise Exception(f"Different return lengths in {name}")
            names = [ret.value.id]
        elif ret and isinstance(ret.value, ast.Constant):
            if names is not None and len(names) != 1:
                raise Exception(f"Different return lengths in {name}")
            names = [None]
        elif ret and isinstance(ret.value, ast.Tuple):
            if names is not None and len(names) != len(ret.value.elts):
                raise Exception(f"Different return lengths in {name}")
            if names is None:
                names = [None for _ in range(len(ret.value.elts))]
            for i, elt in enumerate(ret.value.elts):
                if isinstance(elt, ast.Name):
                    names[i] = elt.id
        else:
            names = [None]
    return names
