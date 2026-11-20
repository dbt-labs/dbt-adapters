import re

import pytest

from tests.functional.utils import run_dbt_and_capture


# psycopg2 registers no typecaster for `money` (OID 790) or `uuid` (OID 2950), so before
# TYPE_OID_TO_DATA_TYPE the adapter could not name either during contract enforcement: it
# emitted TypeCodeNotFound and compared `unknown type_code 790`. Both are PostgreSQL
# built-ins, so they are now named from the table and no event is emitted. A type PostgreSQL
# assigns an OID to per database (an enum, a domain, an extension type) still takes the
# placeholder path; the last class covers it.

UNRECOGNIZED_DEBUG_MSG = "was not recognized"


def mismatch_row(column, definition_type, contract_type):
    """The row `raise_contract_error` renders for a mismatched column, whatever the padding."""
    return re.compile(
        rf"\| {re.escape(column)}\s+\| {re.escape(definition_type)}\s+"
        rf"\| {re.escape(contract_type)}\s+\| data type mismatch"
    )


my_numeric_model_sql = """
select
  12.34 as price
"""

my_money_model_sql = """
select
  cast('12.34' as money) as price
"""

model_schema_money_yml = """
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: price
        data_type: money
"""

model_schema_numeric_yml = """
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: price
        data_type: numeric
"""

my_uuid_model_sql = """
select
  cast('00000000-0000-0000-0000-000000000000' as uuid) as id
"""

model_schema_uuid_yml = """
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: uuid
"""

model_schema_text_yml = """
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: text
"""


class TestModelContractBuiltinTypeTheDriverDoesNotRegister:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_money_model_sql,
            "schema.yml": model_schema_money_yml,
        }

    def test_nonstandard_data_type(self, project):
        _, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert UNRECOGNIZED_DEBUG_MSG not in logs


class TestModelContractBuiltinTypeActualMismatch:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_money_model_sql,
            "schema.yml": model_schema_numeric_yml,
        }

    def test_nonstandard_data_type(self, project):
        _, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=False)
        # The model's column is named `money`, not `unknown type_code 790`.
        assert mismatch_row("price", "money", "DECIMAL").search(logs)
        assert UNRECOGNIZED_DEBUG_MSG not in logs


class TestModelContractBuiltinTypeExpectedMismatch:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_numeric_model_sql,
            "schema.yml": model_schema_money_yml,
        }

    def test_nonstandard_data_type(self, project):
        _, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=False)
        assert mismatch_row("price", "DECIMAL", "money").search(logs)
        assert UNRECOGNIZED_DEBUG_MSG not in logs


class TestModelContractUuid:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_uuid_model_sql,
            "schema.yml": model_schema_uuid_yml,
        }

    def test_uuid_contract_is_enforced_without_a_type_code_event(self, project):
        _, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert UNRECOGNIZED_DEBUG_MSG not in logs


class TestModelContractUuidMismatch:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_uuid_model_sql,
            "schema.yml": model_schema_text_yml,
        }

    def test_uuid_against_a_text_contract_fails_by_name(self, project):
        _, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=False)
        # `text` is a type psycopg2 does register, so it keeps the driver's name, which
        # Column.translate_type relabels from STRING to TEXT.
        assert mismatch_row("id", "uuid", "TEXT").search(logs)
        assert UNRECOGNIZED_DEBUG_MSG not in logs


my_enum_model_sql = """
select
  cast('small' as {{ target.schema }}.shirt_size) as size
"""

model_schema_enum_yml = """
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: size
        data_type: {schema}.shirt_size
"""


class TestModelContractUserDefinedTypeStillTakesThePlaceholderPath:
    @pytest.fixture(scope="class")
    def models(self, unique_schema):
        return {
            "my_model.sql": my_enum_model_sql,
            "schema.yml": model_schema_enum_yml.format(schema=unique_schema),
        }

    def test_user_defined_type(self, project):
        # An enum's OID is assigned when it is created, so it is in no table. Both sides of
        # the comparison still resolve to the same `unknown type_code <oid>`, so a correct
        # contract passes, and the event still says why the message would be unhelpful.
        project.run_sql(f"create type {project.test_schema}.shirt_size as enum ('small')")
        _, logs = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert UNRECOGNIZED_DEBUG_MSG in logs
