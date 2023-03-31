import ast
from aws_cdk import aws_stepfunctions as sfn
from typing import Dict, Tuple, Callable

# Limited comparisons types for now...
comparator_map: Dict[Tuple, Tuple] = {
    (ast.Eq, str): (sfn.Condition.string_equals, "=="),
    (ast.Eq, int): (sfn.Condition.number_equals, "=="),
    (ast.Gt, int): (sfn.Condition.number_greater_than, ">"),
    (ast.Gt, float): (sfn.Condition.number_greater_than, ">"),
    (ast.Lt, int): (sfn.Condition.number_less_than, "<"),
    (ast.Lt, float): (sfn.Condition.number_less_than, "<"),
}


def build_condition(test):
    if isinstance(test, ast.Name):
        # We'll want to check the var type to create appropriate conditions based on the type if defined
        return (
            if_value(test.id, None),
            f"If {test.id}",
        )
    elif (
        isinstance(test, ast.Subscript)
        and isinstance(test.value, ast.Name)
        and isinstance(test.slice, ast.Constant)
    ):
        var_name = f"{test.value.id}.{test.slice.value}"
        return (
            if_value(var_name, None),
            f"If {var_name}",
        )
    elif isinstance(test, ast.Compare):
        # Assuming that the variable is on the left side as well
        var_name = get_var_name(test.left)
        if var_name and len(test.ops) == 1 and len(test.comparators) == 1:
            comparator = test.comparators[0]
            if isinstance(comparator, ast.Constant):
                comp_op: Callable
                label: str
                comp_op, label = comparator_map.get(
                    (type(test.ops[0]), type(comparator.value)), (None, None)
                )
                if comp_op:
                    return (
                        comp_op(f"$.register.{var_name}", comparator.value),
                        f"If {var_name}{label}'{comparator.value}'",
                    )
    elif isinstance(test, ast.Call) and isinstance(test.func, ast.Attribute):
        attr = test.func
        if attr.attr == "startswith":
            var_name = get_var_name(attr.value)
            starts_with = None
            if len(test.args) == 1:
                arg = test.args[0]
                if isinstance(arg, ast.Constant):
                    starts_with = f"{arg.value}*"
            if var_name and starts_with:
                param = f"$.register.{var_name}"
                condition = sfn.Condition.and_(
                    sfn.Condition.is_present(param),
                    sfn.Condition.is_string(param),
                    sfn.Condition.string_matches(param, starts_with),
                )
                return condition, f"If {var_name} starts with {starts_with}"

    raise Exception(f"Unhandled test: {ast.dump(test)}")


def if_value(name, var_type=None):
    param = f"$.register.{name}"
    if isinstance(var_type, bool):
        return sfn.Condition.boolean_equals(param, True)
    elif isinstance(var_type, str):
        return sfn.Condition.and_(
            sfn.Condition.is_present(param),
            sfn.Condition.is_not_null(param),
            sfn.Condition.not_(sfn.Condition.string_equals(param, "")),
        )
    elif isinstance(var_type, int) or isinstance(var_type, float):
        return sfn.Condition.and_(
            sfn.Condition.is_present(param),
            sfn.Condition.is_not_null(param),
            sfn.Condition.not_(sfn.Condition.number_equals(param, 0)),
        )
    else:
        return sfn.Condition.and_(
            sfn.Condition.is_present(param),
            sfn.Condition.is_not_null(param),
            sfn.Condition.or_(
                sfn.Condition.and_(
                    sfn.Condition.is_boolean(param),
                    sfn.Condition.boolean_equals(param, True),
                ),
                sfn.Condition.and_(
                    sfn.Condition.is_string(param),
                    sfn.Condition.not_(sfn.Condition.string_equals(param, "")),
                ),
                sfn.Condition.and_(
                    sfn.Condition.is_numeric(param),
                    sfn.Condition.not_(sfn.Condition.number_equals(param, 0)),
                ),
                sfn.Condition.is_present(f"{param}[0]"),
            ),
        )


def get_var_name(value: ast.expr):
    var_name = None
    if isinstance(value, ast.Name):
        var_name = value.id
    elif (
        isinstance(value, ast.Subscript)
        and isinstance(value.value, ast.Name)
        and isinstance(value.slice, ast.Constant)
    ):
        var_name = f"{value.value.id}.{value.slice.value}"
    return var_name
