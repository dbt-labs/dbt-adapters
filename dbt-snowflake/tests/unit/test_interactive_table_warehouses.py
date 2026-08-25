import os
import re
from unittest import mock

import agate
import pytest
from jinja2 import Environment, FileSystemLoader

from dbt.adapters.snowflake.impl import SnowflakeAdapter
from dbt.adapters.snowflake.relation import SnowflakeRelation
from dbt_common.exceptions import DbtRuntimeError

MACROS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../../src/dbt/include/snowflake/macros")
)


# -- describe_interactive_table_warehouses (Python) -------------------------


def _show_warehouses_result(rows):
    """Build an agate.Table mimicking a `SHOW WAREHOUSES` result set.

    Column types are forced to Text, matching the pattern in
    test_describe_interactive_table.py: agate's default type inference misreads
    small tables of string-ish values in ways production never hits, since real
    SHOW results are always normalized to Text before any comparison happens.
    """
    keys = list(rows[0].keys())
    column_types = [agate.Text()] * len(keys)
    data = [[row.get(k) for k in keys] for row in rows]
    return agate.Table(data, keys, column_types=column_types)


def _relation(identifier="orders", database="my_db", schema="my_schema"):
    return SnowflakeRelation.create(database=database, schema=schema, identifier=identifier)


def _fake_adapter(show_table):
    fake_response = mock.Mock(code="SUCCESS")
    fake_adapter = mock.Mock()
    fake_adapter.execute.return_value = (fake_response, show_table)
    return fake_adapter


def _describe_warehouses(fake_adapter, relation):
    return SnowflakeAdapter.describe_interactive_table_warehouses(fake_adapter, relation)


def test_show_warehouses_is_queried_unscoped():
    show_table = _show_warehouses_result(
        [{"name": "IW1", "type": "INTERACTIVE", "tables": "MY_DB.MY_SCHEMA.ORDERS"}]
    )
    fake_adapter = _fake_adapter(show_table)

    _describe_warehouses(fake_adapter, _relation())

    fake_adapter.execute.assert_called_once_with("show warehouses", fetch=True)


def test_matches_only_interactive_type_and_this_table():
    """Mirrors v2's `interactive_warehouses_attached_to_matches_only_interactive_type_and_this_table`:
    IW2 is INTERACTIVE but doesn't list this table; STD_WH lists it but isn't INTERACTIVE."""
    show_table = _show_warehouses_result(
        [
            {
                "name": "IW1",
                "type": "INTERACTIVE",
                "tables": "MY_DB.MY_SCHEMA.ORDERS,MY_DB.MY_SCHEMA.OTHER",
            },
            {"name": "IW2", "type": "INTERACTIVE", "tables": "MY_DB.MY_SCHEMA.OTHER"},
            {"name": "STD_WH", "type": "STANDARD", "tables": "MY_DB.MY_SCHEMA.ORDERS"},
        ]
    )

    result = _describe_warehouses(_fake_adapter(show_table), _relation(identifier="orders"))

    assert result == ["IW1"]


def test_case_insensitive_match():
    show_table = _show_warehouses_result(
        [{"name": "IW1", "type": "INTERACTIVE", "tables": "my_db.my_schema.orders"}]
    )

    result = _describe_warehouses(_fake_adapter(show_table), _relation(identifier="ORDERS"))

    assert result == ["IW1"]


def test_no_matching_warehouse_returns_empty_list():
    show_table = _show_warehouses_result(
        [{"name": "IW1", "type": "INTERACTIVE", "tables": "MY_DB.MY_SCHEMA.OTHER"}]
    )

    result = _describe_warehouses(_fake_adapter(show_table), _relation(identifier="orders"))

    assert result == []


def test_execute_failure_raises_dbt_runtime_error():
    fake_response = mock.Mock(code="ERROR")
    fake_adapter = mock.Mock()
    fake_adapter.execute.return_value = (fake_response, mock.Mock())

    with pytest.raises(DbtRuntimeError):
        _describe_warehouses(fake_adapter, _relation())


# -- snowflake__sync_interactive_warehouses (Jinja macro) --------------------


class TestSyncInteractiveWarehouses:
    def setup_method(self):
        self.jinja_env = Environment(
            loader=FileSystemLoader(MACROS_DIR),
            extensions=["jinja2.ext.do"],
        )
        self.statements = []
        self.config_value = None
        self.current_value = []

        def fake_statement(
            name=None, fetch_result=False, auto_begin=True, language="sql", caller=None
        ):
            sql = caller()
            self.statements.append((name, re.sub(r"\s+", " ", sql.strip())))
            return ""

        config = mock.Mock()
        config.get = lambda key, default=None, **kwargs: self.config_value

        adapter = mock.Mock()
        adapter.describe_interactive_table_warehouses = lambda relation: self.current_value

        self.default_context = {
            "statement": fake_statement,
            "config": config,
            "adapter": adapter,
        }

    def _run(self, relation="my_db.my_schema.my_table"):
        template = self.jinja_env.get_template(
            "relations/interactive_table/warehouses.sql", globals=self.default_context
        )
        template.module.snowflake__sync_interactive_warehouses(relation)

    def test_nothing_configured_and_nothing_attached_emits_no_statements(self):
        self.config_value = None
        self.current_value = []

        self._run()

        assert self.statements == []

    def test_attach_only(self):
        self.config_value = ["IW1"]
        self.current_value = []

        self._run()

        assert self.statements == [
            (
                "attach_interactive_warehouse_1",
                "alter warehouse IW1 add tables (my_db.my_schema.my_table)",
            )
        ]

    def test_detach_only(self):
        self.config_value = []
        self.current_value = ["IW1"]

        self._run()

        assert self.statements == [
            (
                "detach_interactive_warehouse_1",
                "alter warehouse IW1 drop tables (my_db.my_schema.my_table)",
            )
        ]

    def test_detach_only_when_config_unset(self):
        self.config_value = None
        self.current_value = ["IW1"]

        self._run()

        assert self.statements == [
            (
                "detach_interactive_warehouse_1",
                "alter warehouse IW1 drop tables (my_db.my_schema.my_table)",
            )
        ]

    def test_attach_and_detach_in_the_same_run(self):
        self.config_value = ["IW1"]
        self.current_value = ["IW2"]

        self._run()

        assert self.statements == [
            (
                "attach_interactive_warehouse_1",
                "alter warehouse IW1 add tables (my_db.my_schema.my_table)",
            ),
            (
                "detach_interactive_warehouse_1",
                "alter warehouse IW2 drop tables (my_db.my_schema.my_table)",
            ),
        ]

    def test_case_insensitive_match_does_not_detach_then_reattach(self):
        self.config_value = ["my_wh"]
        self.current_value = ["MY_WH"]

        self._run()

        assert self.statements == [
            (
                "attach_interactive_warehouse_1",
                "alter warehouse my_wh add tables (my_db.my_schema.my_table)",
            )
        ]

    def test_partial_attach_reattaches_existing_and_attaches_new(self):
        """Desired has one warehouse already attached and one new one; current has
        one no-longer-desired warehouse. All three cases fire in a single run:
        the already-attached warehouse still gets an (idempotent) attach statement,
        proving attach is unconditional, while only the no-longer-desired warehouse
        gets detached."""
        self.config_value = ["IW1", "IW2"]
        self.current_value = ["IW1", "IW3"]

        self._run()

        assert self.statements == [
            (
                "attach_interactive_warehouse_1",
                "alter warehouse IW1 add tables (my_db.my_schema.my_table)",
            ),
            (
                "attach_interactive_warehouse_2",
                "alter warehouse IW2 add tables (my_db.my_schema.my_table)",
            ),
            (
                "detach_interactive_warehouse_1",
                "alter warehouse IW3 drop tables (my_db.my_schema.my_table)",
            ),
        ]

    def test_string_config_is_treated_as_single_element_list(self):
        self.config_value = "IW1"
        self.current_value = []

        self._run()

        assert self.statements == [
            (
                "attach_interactive_warehouse_1",
                "alter warehouse IW1 add tables (my_db.my_schema.my_table)",
            )
        ]
