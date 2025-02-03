from typing import Any, Dict, List

from dbt.adapters.base.impl import DEFAULT_BASE_BEHAVIOR_FLAGS
from dbt_common.behavior_flags import BehaviorFlag
from dbt_common.exceptions import DbtBaseException
import pytest


@pytest.fixture
def flags() -> Dict[str, Any]:
    return {
        "unregistered_flag": True,
        "default_false_user_false_flag": False,
        "default_false_user_true_flag": True,
        "default_true_user_false_flag": False,
        "default_true_user_true_flag": True,
    }


@pytest.fixture
def behavior_flags() -> List[BehaviorFlag]:
    return [
        {
            "name": "default_false_user_false_flag",
            "default": False,
            "docs_url": "https://docs.com",
        },
        {
            "name": "default_false_user_true_flag",
            "default": False,
            "description": "This is a false flag.",
        },
        {
            "name": "default_false_user_skip_flag",
            "default": False,
            "description": "This is a true flag.",
        },
        {
            "name": "default_true_user_false_flag",
            "default": True,
            "description": "This is fake news.",
        },
        {
            "name": "default_true_user_true_flag",
            "default": True,
            "docs_url": "https://moar.docs.com",
        },
        {
            "name": "default_true_user_skip_flag",
            "default": True,
            "description": "This is a true flag.",
        },
    ]


def test_register_behavior_flags(adapter):
    # make sure that users cannot add arbitrary flags to this collection
    with pytest.raises(DbtBaseException):
        assert adapter.behavior.unregistered_flag

    # check the values of the valid behavior flags
    assert not adapter.behavior.default_false_user_false_flag
    assert adapter.behavior.default_false_user_true_flag
    assert not adapter.behavior.default_false_user_skip_flag
    assert not adapter.behavior.default_true_user_false_flag
    assert adapter.behavior.default_true_user_true_flag
    assert adapter.behavior.default_true_user_skip_flag


def test_behaviour_flags_property_empty(adapter_default_behaviour_flags):
    assert adapter_default_behaviour_flags._behavior_flags == []


def test_behavior_property_has_defaults(adapter_default_behaviour_flags):
    for flag in DEFAULT_BASE_BEHAVIOR_FLAGS:
        assert hasattr(adapter_default_behaviour_flags.behavior, flag["name"])
