from typing import Any, Dict, List

from dbt_common.behavior_flags import RawBehaviorFlag
from dbt_common.exceptions import DbtInternalError
import pytest


@pytest.fixture
def config_extra() -> Dict[str, Any]:
    config = {
        "flags": {
            "unregistered_flag": True,
            "default_false_user_false_flag": False,
            "default_false_user_true_flag": True,
            "default_true_user_false_flag": False,
            "default_true_user_true_flag": True,
        }
    }
    return config


@pytest.fixture
def behavior_flags() -> List[RawBehaviorFlag]:
    return [
        {"name": "default_false_user_false_flag", "default": False},
        {"name": "default_false_user_true_flag", "default": False},
        {"name": "default_false_user_skip_flag", "default": False},
        {"name": "default_true_user_false_flag", "default": True},
        {"name": "default_true_user_true_flag", "default": True},
        {"name": "default_true_user_skip_flag", "default": True},
    ]


def test_register_behavior_flags(adapter):
    with pytest.raises(DbtInternalError):
        assert adapter.behavior.unregistered_flag
    assert not adapter.behavior.default_false_user_false_flag
    assert adapter.behavior.default_false_user_true_flag
    assert not adapter.behavior.default_false_user_skip_flag
    assert not adapter.behavior.default_true_user_false_flag
    assert adapter.behavior.default_true_user_true_flag
    assert adapter.behavior.default_true_user_skip_flag
