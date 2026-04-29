from dataclasses import dataclass
from typing import Optional

import pytest

from dbt.adapters.catalogs import (
    get_catalog_config,
    register_catalog_config,
)
from dbt.adapters.catalogs._v2_registry import _REGISTRY
from dbt_common.dataclass_schema import dbtClassMixin


@dataclass
class _FakeConfig(dbtClassMixin):
    required_field: str
    optional_field: Optional[int] = None


@pytest.fixture(autouse=True)
def _isolate_registry():
    snapshot = dict(_REGISTRY)
    yield
    _REGISTRY.clear()
    _REGISTRY.update(snapshot)


def test_register_and_lookup():
    register_catalog_config("fake_type", "fake_platform", _FakeConfig)
    assert get_catalog_config("fake_type", "fake_platform") is _FakeConfig


def test_lookup_missing_returns_none():
    assert get_catalog_config("nonexistent", "nonexistent") is None


def test_register_overwrites_existing():
    @dataclass
    class _OtherConfig(dbtClassMixin):
        x: str

    register_catalog_config("fake_type", "fake_platform", _FakeConfig)
    register_catalog_config("fake_type", "fake_platform", _OtherConfig)
    assert get_catalog_config("fake_type", "fake_platform") is _OtherConfig


def test_keyed_by_both_type_and_platform():
    register_catalog_config("type_a", "platform_x", _FakeConfig)
    assert get_catalog_config("type_a", "platform_x") is _FakeConfig
    assert get_catalog_config("type_a", "platform_y") is None
    assert get_catalog_config("type_b", "platform_x") is None
