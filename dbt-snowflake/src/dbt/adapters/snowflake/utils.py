from typing import Optional, Union

from dbt_common.exceptions import DbtValidationError


def set_boolean(name: str, value: Union[bool, str], default: Optional[bool] = None) -> bool:
    if value is None:
        return default
    elif isinstance(value, bool):
        return value
    elif isinstance(value, str) and value.lower() in ["true", "false"]:
        return value.lower() == "true"
    else:
        raise DbtValidationError(
            f"Unexpected value for {name}"
            f"    Received: {value}"
            "    Expected: a boolean or boolean-like string"
        )
