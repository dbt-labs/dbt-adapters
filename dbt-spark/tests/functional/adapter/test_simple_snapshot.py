import pytest

from dbt.tests.adapter.simple_snapshot.fixtures import (
    create_multi_key_seed_sql,
    create_multi_key_snapshot_expected_sql,
    invalidate_multi_key_sql,
    model_seed_sql,
    populate_multi_key_snapshot_expected_sql,
    ref_snapshot_sql,
    seed_multi_key_insert_sql,
    snapshots_multi_key_yml,
    update_multi_key_sql,
)
from dbt.tests.util import check_relations_equal, run_dbt


def spark_sql(sql: str) -> str:
    return sql.replace("TEXT", "STRING").replace("text", "string")


@pytest.mark.skip_profile("apache_spark", "spark_session")
class TestSnapshotMultiUniqueKeySpark:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "seed.sql": model_seed_sql,
            "ref_snapshot.sql": ref_snapshot_sql,
            "snapshots.yml": snapshots_multi_key_yml,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"snapshots": {"+file_format": "delta"}}

    def test_multi_column_unique_key(self, project):
        project.run_sql(spark_sql(create_multi_key_seed_sql))
        project.run_sql(spark_sql(create_multi_key_snapshot_expected_sql))
        project.run_sql(seed_multi_key_insert_sql)
        project.run_sql(spark_sql(populate_multi_key_snapshot_expected_sql))

        results = run_dbt(["snapshot"])
        assert len(results) == 1

        for statement in spark_sql(invalidate_multi_key_sql).split(";"):
            if statement.strip():
                project.run_sql(statement)
        project.run_sql(spark_sql(update_multi_key_sql))

        results = run_dbt(["snapshot"])
        assert len(results) == 1

        check_relations_equal(
            project.adapter,
            ["snapshot_actual", "snapshot_expected"],
            compare_snapshot_cols=True,
        )
