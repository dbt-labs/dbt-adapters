from unittest.mock import MagicMock

import pytest

from dbt.adapters.bigquery.impl import BigQueryAdapter


@pytest.fixture
def adapter():
    return BigQueryAdapter(MagicMock(), MagicMock())


class TestCalculateFreshnessFromMetadataBatch:
    def test_empty_sources_returns_early_without_querying(self, adapter, mocker):
        execute_macro = mocker.patch.object(adapter, "execute_macro")

        adapter_responses, freshness_responses = adapter.calculate_freshness_from_metadata_batch(
            sources=[]
        )

        assert adapter_responses == []
        assert freshness_responses == {}
        execute_macro.assert_not_called()
