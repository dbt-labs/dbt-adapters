from typing import Union


def evaluate_bool_str(value: str) -> bool:
    value = value.strip().lower()
    if value == "true":
        return True
    elif value == "false":
        return False
    else:
        raise ValueError(f"Invalid boolean string value: {value}")


def evaluate_bool(value: Union[str, bool]) -> bool:
    if not value:
        return False
    if isinstance(value, bool):
        return value
    elif isinstance(value, str):
        return evaluate_bool_str(value)
    else:
        raise TypeError(
            f"Invalid type for boolean evaluation, "
            f"expecting boolean or str, recieved: {type(value)}"
        )
