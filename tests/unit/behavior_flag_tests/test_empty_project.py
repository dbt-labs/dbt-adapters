from types import SimpleNamespace
from typing import Any, Dict, List

from dbt_common.behavior_flags import BehaviorFlag
from dbt_common.exceptions import DbtBaseException
import pytest

from dbt.adapters.contracts.connection import AdapterRequiredConfig, QueryComment

from tests.unit.fixtures.credentials import CredentialsStub


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
def config(flags) -> AdapterRequiredConfig:
    raw_config = {
        "credentials": CredentialsStub("test_database", "test_schema"),
        "profile_name": "test_profile",
        "target_name": "test_target",
        "threads": 4,
        "project_name": "test_project",
        "query_comment": QueryComment(),
        "cli_vars": {},
        "target_path": "path/to/nowhere",
        "log_cache_events": False,
    }
    return SimpleNamespace(**raw_config)


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
    assert not adapter.behavior.default_false_user_true_flag
    assert not adapter.behavior.default_false_user_skip_flag
    assert adapter.behavior.default_true_user_false_flag
    assert adapter.behavior.default_true_user_true_flag
    assert adapter.behavior.default_true_user_skip_flag
