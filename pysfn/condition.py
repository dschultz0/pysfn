import ast
from aws_cdk import aws_stepfunctions as sfn


def build_condition(test):
    if isinstance(test, ast.Name):
        # We'll want to check the var type to create appropriate conditions based on the type if defined
        return (
            if_value(test.id, None),
            f"If {test.id}",
        )
    elif isinstance(test, ast.Compare):
        # Just going to handle simple comparison to string values for now
        if (
            isinstance(test.left, ast.Name)
            and len(test.ops) == 1
            and isinstance(test.ops[0], ast.Eq)
            and len(test.comparators) == 1
            and isinstance(test.comparators[0], ast.Constant)
        ):
            return (
                sfn.Condition.string_equals(
                    f"$.register.{test.left.id}", test.comparators[0].value
                ),
                f"If {test.left.id}=='{test.comparators[0].value}'",
            )
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
