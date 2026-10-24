import pytest

from dbt.tests.util import relation_from_name, run_dbt


class TestSnapshotCheckColsCase:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "case_snapshot.sql": """
{% snapshot case_snapshot %}
{{ config(strategy='check', unique_key='id', check_cols=['dice_change_hash'],
          target_database=database, target_schema=schema) }}
select 1 as id,
       '{{ var("hash_value", "original") }}' as DICE_CHANGE_HASH,
       '{{ var("watermark", "first") }}' as DICE_CHANGE_SOURCE_WATERMARK
{% endsnapshot %}
""",
        }

    def test_check_cols_uses_bigquery_column_case(self, project):
        relation = relation_from_name(project.adapter, "case_snapshot")

        run_dbt(["snapshot"])
        run_dbt(["snapshot", "--vars", "watermark: second"])
        assert project.run_sql(f"select count(*) from {relation}", fetch="one")[0] == 1

        run_dbt(["snapshot", "--vars", "{watermark: third, hash_value: changed}"])
        assert project.run_sql(f"select count(*) from {relation}", fetch="one")[0] == 2
