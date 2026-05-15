from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import agate
import pytest

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.redshift.impl import RedshiftAdapter

LAST_MODIFIED_TIME = datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc)


def macro_return(rows, response=None):
    table = agate.Table(
        rows,
        column_names=["table_name", "last_modified_time"],
        column_types=[agate.Text(), agate.DateTime()],
    )
    return SimpleNamespace(response=response or MagicMock(), table=table)


def relation(database, schema, identifier):
    return BaseRelation.create(database=database, schema=schema, identifier=identifier)


@pytest.fixture
def adapter(mocker):
    a = RedshiftAdapter(mocker.MagicMock(), mocker.MagicMock())
    a.config.credentials.datasharing = True
    return a


class TestCalculateFreshnessFromMetadataBatch:
    def test_delegates_to_super_when_show_apis_disabled(self, adapter, mocker):
        adapter.config.credentials.datasharing = False
        super_mock = mocker.patch(
            "dbt.adapters.sql.SQLAdapter.calculate_freshness_from_metadata_batch",
            return_value=([], {}),
        )
        sources = [relation("db", "schema", "table1")]

        adapter.calculate_freshness_from_metadata_batch(sources)

        super_mock.assert_called_once_with(sources, None)

    @pytest.mark.parametrize(
        "table_name,rows",
        [
            pytest.param(
                "mytable",
                [("mytable", LAST_MODIFIED_TIME)],
                id="single_row",
            ),
            pytest.param(
                "mytable",
                [
                    ("mytable", LAST_MODIFIED_TIME),
                    ("other_table", LAST_MODIFIED_TIME),
                ],
                id="extra_rows_ignored",
            ),
        ],
    )
    def test_single_source_freshness(self, adapter, mocker, table_name, rows):
        source = relation("mydb", "myschema", table_name)
        mock_response = MagicMock()
        execute = mocker.patch.object(
            adapter,
            "execute_macro",
            return_value=macro_return(rows, response=mock_response),
        )
        adapter_responses, freshness = adapter.calculate_freshness_from_metadata_batch([source])
        assert list(freshness.keys()) == [source]
        assert freshness[source]["max_loaded_at"] == LAST_MODIFIED_TIME
        assert adapter_responses == [mock_response]
        assert execute.call_args.kwargs.get("needs_conn") is True

    @pytest.mark.parametrize(
        "sources",
        [
            pytest.param(
                [
                    relation("db", "schema1", "table1"),
                    relation("db", "schema2", "table2"),
                ],
                id="multiple_schemas",
            ),
            pytest.param(
                [
                    relation("current_db", "schema", "table1"),
                    relation("other_db", "schema", "table2"),
                ],
                id="cross_database",
            ),
        ],
    )
    def test_multiple_sources_freshness(self, adapter, mocker, sources):
        s1, s2 = sources
        execute = mocker.patch.object(
            adapter,
            "execute_macro",
            side_effect=[
                macro_return([(s1.identifier, LAST_MODIFIED_TIME)]),
                macro_return([(s2.identifier, LAST_MODIFIED_TIME)]),
            ],
        )

        _, freshness = adapter.calculate_freshness_from_metadata_batch(sources)

        assert execute.call_count == 2
        assert freshness[s1]["max_loaded_at"] == LAST_MODIFIED_TIME
        assert freshness[s2]["max_loaded_at"] == LAST_MODIFIED_TIME
