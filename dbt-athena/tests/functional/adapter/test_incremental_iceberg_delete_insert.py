"""
Functional tests for the delete+insert incremental strategy on Iceberg tables.

The strategy must behave like dbt's default delete+insert (delete every target row whose
unique_key is in the new data, honouring incremental_predicates on the target, then insert
the new rows) and, on Athena, do it in a single Iceberg commit.
"""

import pytest

from dbt.tests.adapter.incremental.test_incremental_predicates import (
    BaseIncrementalPredicates,
)
from dbt.tests.adapter.incremental.test_incremental_unique_id import (
    BaseIncrementalUniqueKey,
    models__duplicated_unary_unique_key_list_sql,
    models__empty_str_unique_key_sql,
    models__empty_unique_key_list_sql,
    models__expected__one_str__overwrite_sql,
    models__expected__unique_key_list__inplace_overwrite_sql,
    models__no_unique_key_sql,
    models__nontyped_trinary_unique_key_list_sql,
    models__not_found_unique_key_list_sql,
    models__not_found_unique_key_sql,
    models__str_unique_key_sql,
    models__trinary_unique_key_list_sql,
    models__unary_unique_key_list_sql,
    seeds__add_new_rows_sql,
    seeds__duplicate_insert_sql,
    seeds__seed_csv,
)
from dbt.tests.util import check_relations_equal, run_dbt
from tests.functional.adapter.test_incremental_iceberg import replace_cast_date


class TestIcebergDeleteInsertUniqueKey(BaseIncrementalUniqueKey):
    """dbt's standard unique-key suite, which other adapters run with delete+insert."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+table_type": "iceberg", "+incremental_strategy": "delete+insert"}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "trinary_unique_key_list.sql": models__trinary_unique_key_list_sql,
            "nontyped_trinary_unique_key_list.sql": models__nontyped_trinary_unique_key_list_sql,
            "unary_unique_key_list.sql": models__unary_unique_key_list_sql,
            "not_found_unique_key.sql": models__not_found_unique_key_sql,
            "empty_unique_key_list.sql": models__empty_unique_key_list_sql,
            "no_unique_key.sql": models__no_unique_key_sql,
            "empty_str_unique_key.sql": models__empty_str_unique_key_sql,
            "str_unique_key.sql": models__str_unique_key_sql,
            "duplicated_unary_unique_key_list.sql": models__duplicated_unary_unique_key_list_sql,
            "not_found_unique_key_list.sql": models__not_found_unique_key_list_sql,
            "expected": {
                "one_str__overwrite.sql": replace_cast_date(
                    models__expected__one_str__overwrite_sql
                ),
                "unique_key_list__inplace_overwrite.sql": replace_cast_date(
                    models__expected__unique_key_list__inplace_overwrite_sql
                ),
            },
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "duplicate_insert.sql": replace_cast_date(seeds__duplicate_insert_sql),
            "seed.csv": seeds__seed_csv,
            "add_new_rows.sql": replace_cast_date(seeds__add_new_rows_sql),
        }


class TestIcebergDeleteInsertIncrementalPredicates(BaseIncrementalPredicates):
    """The standard predicates suite: `id != 2` keeps the old id=2 row, as in the default."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+table_type": "iceberg",
                "+incremental_predicates": ["id != 2"],
                "+incremental_strategy": "delete+insert",
            }
        }


class TestIcebergDeleteInsertPredicatesAlias(BaseIncrementalPredicates):
    """`predicates` is accepted as an alias of `incremental_predicates`, as in dbt core."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+table_type": "iceberg",
                "+predicates": ["id != 2"],
                "+incremental_strategy": "delete+insert",
            }
        }


models__scope_replace_sql = """
{{ config(
    materialized='incremental',
    table_type='iceberg',
    incremental_strategy='delete+insert',
    unique_key='day_id',
) }}

{% if not is_incremental() %}
select 1 as day_id, 'a' as event_id, 10 as value
union all select 1, 'b', 20
union all select 1, 'c', 30
union all select 2, 'x', 90
{% else %}
-- day 1 is reprocessed: fewer rows, new ids. Day 3 is new. Day 2 is untouched.
select 1 as day_id, 'a2' as event_id, 11 as value
union all select 3, 'z', 70
{% endif %}
"""

seeds__expected_scope_replace_csv = """day_id,event_id,value
1,a2,11
2,x,90
3,z,70
"""


class TestIcebergDeleteInsertReplacesWholeKeyInOneCommit:
    """unique_key names what a run replaces wholesale, not a row key: every old row of a
    reprocessed key goes, rows of other keys stay, and it all lands as one snapshot."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"scope_replace.sql": models__scope_replace_sql}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expected_scope_replace.csv": seeds__expected_scope_replace_csv}

    def _snapshot_count(self, project):
        sql = f'select count(*) from "{project.test_schema}"."scope_replace$snapshots"'
        return project.run_sql(sql, fetch="one")[0]

    def test__replace_key_single_commit(self, project):
        run_dbt(["seed"])
        run_dbt(["run", "--select", "scope_replace"])
        before = self._snapshot_count(project)

        results = run_dbt(["run", "--select", "scope_replace"])
        assert len(results) == 1

        assert self._snapshot_count(project) == before + 1
        check_relations_equal(project.adapter, ["scope_replace", "expected_scope_replace"])


models__scope_replace_force_batch_sql = models__scope_replace_sql.replace(
    "unique_key='day_id',",
    "unique_key='day_id',\n    partitioned_by=['day_id'],\n    force_batch=true,",
)


class TestIcebergDeleteInsertForceBatch(TestIcebergDeleteInsertReplacesWholeKeyInOneCommit):
    """force_batch takes the batched path (DELETE, then per-partition INSERTs): same rows,
    more than one commit."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"scope_replace.sql": models__scope_replace_force_batch_sql}

    def test__replace_key_single_commit(self, project):
        run_dbt(["seed"])
        run_dbt(["run", "--select", "scope_replace"])
        before = self._snapshot_count(project)
        run_dbt(["run", "--select", "scope_replace"])
        assert self._snapshot_count(project) > before + 1
        check_relations_equal(project.adapter, ["scope_replace", "expected_scope_replace"])
