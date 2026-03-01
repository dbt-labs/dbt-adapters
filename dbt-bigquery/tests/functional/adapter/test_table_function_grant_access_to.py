import time

import pytest

from dbt.tests.util import run_dbt


MY_TVF_SQL = """
{{ config(
    grant_access_to=[
      {'project': 'dbt-test-env', 'dataset': dataset_name},
    ]
) }}
SELECT x, x * 2 AS double_x
FROM UNNEST(GENERATE_ARRAY(1, max_value)) AS x
""".strip()

MY_TVF_YML = """
functions:
  - name: my_tvf
    description: Table function for testing grant_access_to
    config:
      type: table
    arguments:
      - name: max_value
        data_type: INT64
    returns:
      data_type: TABLE
"""


def get_schema_name(base_schema_name: str) -> str:
    return f"{base_schema_name}_tvf_grant_access"


def tvf_sql_with_dataset(dataset: str) -> str:
    return MY_TVF_SQL.replace("dataset_name", f"'{dataset}'")


class TestTableFunctionGrantAccessTo:
    @pytest.fixture(scope="class")
    def setup_grant_schema(
        self,
        project,
        unique_schema,
    ):
        with project.adapter.connection_named("__test_grants"):
            relation = project.adapter.Relation.create(
                database=project.database,
                schema=get_schema_name(unique_schema),
                identifier="grant_access",
            )
            project.adapter.create_schema(relation)
            yield relation

    @pytest.fixture(scope="class")
    def teardown_grant_schema(
        self,
        project,
        unique_schema,
    ):
        yield
        with project.adapter.connection_named("__test_grants"):
            relation = project.adapter.Relation.create(
                database=project.database, schema=get_schema_name(unique_schema)
            )
            project.adapter.drop_schema(relation)

    @pytest.fixture(scope="class")
    def functions(self, unique_schema):
        dataset = get_schema_name(unique_schema)
        return {
            "my_tvf.sql": tvf_sql_with_dataset(dataset),
            "my_tvf.yml": MY_TVF_YML,
        }

    def test_table_function_grant_access_succeeds(
        self, project, setup_grant_schema, teardown_grant_schema, unique_schema
    ):
        # First run: creates TVF and grants access
        results = run_dbt(["run"])
        assert len(results) == 1

        with project.adapter.connection_named("__test_grants"):
            client = project.adapter.connections.get_thread_connection().handle
            dataset_name = get_schema_name(unique_schema)
            dataset_id = "{}.{}".format("dbt-test-env", dataset_name)
            bq_dataset = client.get_dataset(dataset_id)

            authorized_routine_names = []
            for access_entry in bq_dataset.access_entries:
                if access_entry.entity_type != "routine":
                    continue
                authorized_routine_names.append(access_entry.entity_id["routineId"])

            assert "my_tvf" in authorized_routine_names

        # Second run: validates idempotency (remove-then-add pattern)
        time.sleep(5)
        results = run_dbt(["run"])
        assert len(results) == 1

        with project.adapter.connection_named("__test_grants"):
            client = project.adapter.connections.get_thread_connection().handle
            bq_dataset = client.get_dataset(dataset_id)

            authorized_routine_names = []
            for access_entry in bq_dataset.access_entries:
                if access_entry.entity_type != "routine":
                    continue
                authorized_routine_names.append(access_entry.entity_id["routineId"])

            assert "my_tvf" in authorized_routine_names
