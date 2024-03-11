import pytest

# TODO: update to new dbt-artifacts once available
from dbt.artifacts.schemas.results import RunStatus
from dbt.tests.adapter.basic import files
from dbt.tests.util import run_dbt, check_relations_equal, relation_from_name


class BaseIncremental:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "incremental"}

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental.sql": files.incremental_sql, "schema.yml": files.schema_base_yml}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"base.csv": files.seeds_base_csv, "added.csv": files.seeds_added_csv}

    @pytest.fixture(autouse=True)
    def clean_up(self, project):
        yield
        with project.adapter.connection_named("__test"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(relation)

    pass

    def test_incremental(self, project):
        # seed command
        results = run_dbt(["seed"])
        assert len(results) == 2

        # base table rowcount
        relation = relation_from_name(project.adapter, "base")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 10

        # added table rowcount
        relation = relation_from_name(project.adapter, "added")
        result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
        assert result[0] == 20

        # run command
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: base"])
        assert len(results) == 1

        # check relations equal
        check_relations_equal(project.adapter, ["base", "incremental"])

        # change seed_name var
        # the "seed_name" var changes the seed identifier in the schema file
        results = run_dbt(["run", "--vars", "seed_name: added"])
        assert len(results) == 1

        # check relations equal
        check_relations_equal(project.adapter, ["added", "incremental"])

        # get catalog from docs generate
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == 3
        assert len(catalog.sources) == 1


class BaseIncrementalNotSchemaChange:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"name": "incremental"}

    @pytest.fixture(scope="class")
    def models(self):
        return {"incremental_not_schema_change.sql": files.incremental_not_schema_change_sql}

    def test_incremental_not_schema_change(self, project):
        # Schema change is not evaluated on first run, so two are needed
        run_dbt(["run", "--select", "incremental_not_schema_change"])
        run_result = (
            run_dbt(["run", "--select", "incremental_not_schema_change"]).results[0].status
        )

        assert run_result == RunStatus.Success


class Testincremental(BaseIncremental):
    pass


class TestBaseIncrementalNotSchemaChange(BaseIncrementalNotSchemaChange):
    pass
