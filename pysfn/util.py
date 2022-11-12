import inspect
import ast
from typing import Callable


def get_function_ast(func: Callable):
    # Retrieve the source code for the function with any indent removed
    src_code = inspect.getsource(func).split("\n")
    indent = len(src_code[0]) - len(src_code[0].lstrip())
    src_code = [c[indent:] for c in src_code]

    # Build the AST
    return ast.parse("\n".join(src_code))


def find_return(node: ast.expr):
    if isinstance(node, ast.Return):
        return node
    elif hasattr(node, "body"):
        for n in node.body:
            res = find_return(n)
            if res:
                return res
    else:
        return None


def get_function_return_vars(func: Callable):
    tree = get_function_ast(func)
    ret = find_return(tree)
    if ret and isinstance(ret.value, ast.Name):
        return [ret.value.id]
    elif (
        ret
        and isinstance(ret.value, ast.Tuple)
        and all([isinstance(e, ast.Name) for e in ret.value.elts])
    ):
        return [e.id for e in ret.value.elts]
    else:
        return None
