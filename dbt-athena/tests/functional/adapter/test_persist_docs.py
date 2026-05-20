import pytest

from dbt.tests.adapter.persist_docs import fixtures
from dbt.tests.adapter.persist_docs.test_persist_docs import (
    BasePersistDocsBase,
)
from dbt.tests.util import run_dbt, run_dbt_and_capture


class TestPersistDocsColumnMissing(BasePersistDocsBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+persist_docs": {
                        "columns": True,
                    },
                }
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"missing_column.sql": fixtures._MODELS__MISSING_COLUMN}

    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": fixtures._PROPERITES__SCHEMA_MISSING_COL}

    def test_missing_column(self, project):
        _, logs = run_dbt_and_capture(["run"])
        assert (
            "The following columns are specified in the schema but are not present in the database: column_that_does_not_exist"
            in logs
        )


# DIAGNOSTIC: reproduces the quoted-column case-sensitivity scenario that fails
# in CI. Fully self-contained (no fixtures from dbt-tests-adapter base classes)
# so this test branch only triggers the athena CI path filter.
_DIAG_MODEL = """
{{ config(materialized='table') }}
select 1 as mycol
"""

_DIAG_SCHEMA = """
version: 2
models:
  - name: diag_quoted_case_sensitive
    columns:
      - name: MyCol
        description: "case-sensitive name; must not silently match mycol"
        quote: true
"""


class TestDiagnosticQuotedColumnMacroLoad:
    """Diagnostic-only: drives validate_doc_columns with a quoted-column model,
    then dumps the captured dbt logs so we can see what macro actually ran in CI."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+persist_docs": {
                        "columns": True,
                    },
                }
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"diag_quoted_case_sensitive.sql": _DIAG_MODEL}

    @pytest.fixture(scope="class")
    def properties(self):
        return {"schema.yml": _DIAG_SCHEMA}

    def test_diag_quoted_column_macro_load(self, project):
        run_dbt(["seed"])
        _, logs = run_dbt_and_capture(["run"])
        print("===== DIAGNOSTIC CAPTURED DBT LOGS =====")
        print(logs)
        print("===== END =====")
        # Intentionally no assertion: we only want the captured stdout from the
        # hatch.toml diagnostic step + this print so we can inspect which macro
        # is actually loaded in CI.
