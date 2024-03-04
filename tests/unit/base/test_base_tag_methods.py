from unittest.mock import Mock, patch

import pytest

from dbt.adapters.contracts.metadata import RelationTag

from dbt.adapters.base import BaseAdapter
from dbt.adapters.clients.tags import _RELATION_TAGS


@pytest.fixture
def connection_manager():
    mock_connection_manager = Mock()
    mock_connection_manager.TYPE = "base"
    return mock_connection_manager


@patch.multiple(BaseAdapter, __abstractmethods__=set())
def test_base_impl_get_relation_tags(connection_manager):
    BaseAdapter.ConnectionManager = Mock()
    adapter = BaseAdapter(config=Mock(), mp_context=Mock())
    _RELATION_TAGS.append(RelationTag(name='name', value='value'))
    assert adapter.get_relation_tags() == [RelationTag(name='name', value='value')]


@patch.multiple(BaseAdapter, __abstractmethods__=set())
def test_base_impl_set_relation_tags(connection_manager):
    BaseAdapter.ConnectionManager = Mock()
    adapter = BaseAdapter(config=Mock(), mp_context=Mock())
    adapter.set_relation_tag('name', 'value')
    assert adapter.get_relation_tags() == [RelationTag(name='name', value='value')]
