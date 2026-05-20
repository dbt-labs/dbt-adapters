import pytest

from dbt.tests.adapter.persist_docs import fixtures
from dbt.tests.adapter.persist_docs.test_persist_docs import (
    BasePersistDocsAllColumnsMissing,
    BasePersistDocsBase,
    BasePersistDocsQuotedColumnCaseSensitive,
    BasePersistDocsQuotedDescriptionNotAppliedOnMismatch,
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
        return {"schema.yml": fixtures._PROPERTIES__SCHEMA_MISSING_COL}

    def test_missing_column(self, project):
        _, logs = run_dbt_and_capture(["run"])
        assert (
            "The following columns are specified in the schema but are not present in the database: column_that_does_not_exist"
            in logs
        )


class TestPersistDocsAllColumnsMissing(BasePersistDocsAllColumnsMissing):
    pass


class TestPersistDocsQuotedColumnCaseSensitive(BasePersistDocsQuotedColumnCaseSensitive):
    pass


class TestPersistDocsQuotedDescriptionNotAppliedOnMismatch(
    BasePersistDocsQuotedDescriptionNotAppliedOnMismatch
):
    @pytest.mark.skip(
        reason=(
            "Athena/Glue folds column names to lowercase at the catalog layer, "
            "so a case-mismatched physical column is physically unreachable. "
            "Case-sensitivity logic is covered by TestPersistDocsQuotedColumnCaseSensitive. "
            "Docs: https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html"
        )
    )
    def test_quoted_description_not_applied_on_case_mismatch(self, project):
        pass
