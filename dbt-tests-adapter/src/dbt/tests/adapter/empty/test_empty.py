from dbt.tests.util import relation_from_name, run_dbt
import pytest

from dbt.tests.adapter.empty import _models


class BaseTestEmpty:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "raw_source.csv": _models.raw_source_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_input.sql": _models.model_input_sql,
            "ephemeral_model_input.sql": _models.ephemeral_model_input_sql,
            "model.sql": _models.model_sql,
            "sources.yml": _models.schema_sources_yml,
        }

    def assert_row_count(self, project, relation_name: str, expected_row_count: int):
        relation = relation_from_name(project.adapter, relation_name)
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == expected_row_count

    def test_run_with_empty(self, project):
        # create source from seed
        run_dbt(["seed"])

        # run without empty - 3 expected rows in output - 1 from each input
        run_dbt(["run"])
        self.assert_row_count(project, "model", 3)

        # run with empty - 0 expected rows in output
        run_dbt(["run", "--empty"])
        self.assert_row_count(project, "model", 0)


class BaseTestEmptyInlineSourceRef(BaseTestEmpty):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": _models.model_inline_sql,
            "sources.yml": _models.schema_sources_yml,
        }

    def test_run_with_empty(self, project):
        # create source from seed
        run_dbt(["seed"])
        run_dbt(["run", "--empty", "--debug"])
        self.assert_row_count(project, "model", 0)


class TestEmpty(BaseTestEmpty):
    """
    Though we don't create these classes anymore, we need to keep this one in case an adapter wanted to import the test as-is to automatically run it.
    We should consider adding a deprecation warning that suggests moving this into the concrete adapter and importing `BaseTestEmpty` instead.
    """

    pass


class MetadataWithEmptyFlag:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_seed.csv": _models.SEED}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": _models.SCHEMA,
            "control.sql": _models.CONTROL,
            "get_columns_in_relation.sql": _models.GET_COLUMNS_IN_RELATION,
            "alter_column_type.sql": _models.ALTER_COLUMN_TYPE,
            "alter_relation_comment.sql": _models.ALTER_RELATION_COMMENT,
            "alter_column_comment.sql": _models.ALTER_COLUMN_COMMENT,
            "alter_relation_add_remove_columns.sql": _models.ALTER_RELATION_ADD_REMOVE_COLUMNS,
            "truncate_relation.sql": _models.TRUNCATE_RELATION,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])

    @pytest.mark.parametrize(
        "model",
        [
            "control",
            "get_columns_in_relation",
            "alter_column_type",
            "alter_relation_comment",
            "alter_column_comment",
            "alter_relation_add_remove_columns",
            "truncate_relation",
        ],
    )
    def test_run(self, project, model):
        run_dbt(["run", "--empty", "--select", model])
