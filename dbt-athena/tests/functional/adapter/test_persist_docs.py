import pytest

from dbt.tests.adapter.persist_docs import fixtures
from dbt.tests.adapter.persist_docs.test_persist_docs import (
    BasePersistDocsBase,
)
from dbt.tests.util import run_dbt_and_capture


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
