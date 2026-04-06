"""
Unit tests for merge_select_exclude_columns:
- Verify column filtering in schema change detection
- Verify generated MERGE SQL excludes specified columns from INSERT/UPDATE
"""

import os
import re
from unittest import mock

import jinja2


_INCREMENTAL_DIR = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir,
        "src",
        "dbt",
        "include",
        "athena",
        "macros",
        "materializations",
        "models",
        "incremental",
    )
)


class MockRelation:
    def __init__(self, name):
        self._name = name

    def __str__(self):
        return self._name


class MockColumn:
    def __init__(self, name, dtype="varchar"):
        self.name = name
        self.column = name
        self.quoted = f'"{name}"'
        self.data_type = dtype
        self.dtype = dtype


def _normalize(sql):
    return " ".join(sql.strip().split())


# --- check_for_schema_changes tests ---


def _load_patched_on_schema_change_sql():
    with open(os.path.join(_INCREMENTAL_DIR, "on_schema_change.sql")) as f:
        src = f.read()
    src = src.replace(
        "{{ return(changes_dict) }}",
        "{% do _result.update(changes_dict) %}",
    )
    return src


def _render_check_for_schema_changes(
    source_columns,
    target_columns,
    merge_select_exclude_columns=None,
    incremental_strategy="merge",
):
    result = {}

    cfg = {"incremental_strategy": incremental_strategy}
    if merge_select_exclude_columns:
        cfg["merge_select_exclude_columns"] = merge_select_exclude_columns

    def mock_get_columns(relation):
        if "source" in str(relation):
            return source_columns
        return target_columns

    def diff_columns(left, right):
        right_names = {c.column.lower() for c in right}
        return [c for c in left if c.column.lower() not in right_names]

    def diff_column_data_types(left, right):
        right_map = {c.column.lower(): c.dtype for c in right}
        return [
            c
            for c in left
            if c.column.lower() in right_map and c.dtype != right_map[c.column.lower()]
        ]

    context = {
        "adapter": mock.Mock(),
        "config": mock.Mock(),
        "log": lambda *args, **kwargs: "",
        "diff_columns": diff_columns,
        "diff_column_data_types": diff_column_data_types,
        "_result": result,
    }
    context["adapter"].get_columns_in_relation = mock_get_columns
    context["config"].get = lambda key, *args, **kwargs: cfg.get(
        key, kwargs.get("default", args[0] if args else None)
    )

    patched_src = _load_patched_on_schema_change_sql()
    env = jinja2.Environment(
        loader=jinja2.DictLoader({"on_schema_change.sql": patched_src}),
        extensions=["jinja2.ext.do"],
    )
    tpl = env.get_template("on_schema_change.sql", globals=context)
    tpl.module.athena__check_for_schema_changes(
        MockRelation("source_relation"),
        MockRelation("target_relation"),
    )
    return result


class TestCheckForSchemaChanges:
    def test_excluded_column_filtered_and_no_schema_change(self):
        source = [MockColumn("id"), MockColumn("msg"), MockColumn("_is_deleted")]
        target = [MockColumn("id"), MockColumn("msg")]
        result = _render_check_for_schema_changes(
            source, target, merge_select_exclude_columns=["_is_deleted"]
        )
        source_names = [c.column for c in result["source_columns"]]
        assert source_names == ["id", "msg"]
        assert result["schema_changed"] is False

    def test_schema_change_detected_without_exclude(self):
        source = [MockColumn("id"), MockColumn("msg"), MockColumn("_is_deleted")]
        target = [MockColumn("id"), MockColumn("msg")]
        result = _render_check_for_schema_changes(source, target)
        assert result["schema_changed"] is True
        assert len(result["source_not_in_target"]) == 1

    def test_no_filtering_for_non_merge_strategy(self):
        source = [MockColumn("id"), MockColumn("msg"), MockColumn("_is_deleted")]
        target = [MockColumn("id"), MockColumn("msg")]
        result = _render_check_for_schema_changes(
            source,
            target,
            merge_select_exclude_columns=["_is_deleted"],
            incremental_strategy="append",
        )
        assert result["schema_changed"] is True

    def test_case_insensitive_exclude(self):
        source = [MockColumn("id"), MockColumn("msg"), MockColumn("_Is_Deleted")]
        target = [MockColumn("id"), MockColumn("msg")]
        result = _render_check_for_schema_changes(
            source, target, merge_select_exclude_columns=["_is_deleted"]
        )
        assert result["schema_changed"] is False

    def test_no_change_when_schemas_match(self):
        source = [MockColumn("id"), MockColumn("msg")]
        target = [MockColumn("id"), MockColumn("msg")]
        result = _render_check_for_schema_changes(source, target)
        assert result["schema_changed"] is False


# --- iceberg_merge SQL tests ---


class MockAdapter:
    def __init__(self, columns, query_result):
        self._columns = columns
        self._query_result = query_result
        self.last_sql = None

    def get_columns_in_relation(self, relation):
        return self._columns

    def run_query_with_partitions_limit_catching(self, sql):
        self.last_sql = str(sql).strip()
        return self._query_result

    def is_list(self, x):
        return isinstance(x, list)


def _load_patched_merge_sql():
    with open(os.path.join(_INCREMENTAL_DIR, "merge.sql")) as f:
        src = f.read()
    src = src.replace(
        "get_merge_update_columns(merge_update_columns, merge_exclude_columns,"
        " dest_columns_wo_keys)",
        "dest_columns_wo_keys",
    )
    src = re.sub(
        r"\{%-?\s*macro get_update_statement\(.*?%\}.*?\{%-?\s*endmacro\s*-?%\}",
        (
            "{%- macro get_update_statement(col, rule, is_last) -%}"
            '{{ col.quoted }} = src.{{ col.quoted }}{{ "" if is_last else "," }}'
            "{%- endmacro -%}"
        ),
        src,
        flags=re.DOTALL,
    )
    return src


def _render_iceberg_merge(dest_columns, delete_condition=None):
    adapter = MockAdapter(dest_columns, "OK")

    context = {
        "adapter": adapter,
        "config": mock.Mock(),
        "log": lambda *args, **kwargs: "",
        "run_query": lambda sql: "",
        "exceptions": mock.Mock(),
        "process_schema_changes": lambda *a, **kw: dest_columns,
        "get_partition_batches": lambda *a, **kw: [],
    }
    context["config"].get = lambda key, *args, **kwargs: kwargs.get(
        "default", args[0] if args else None
    )

    patched_src = _load_patched_merge_sql()
    env = jinja2.Environment(
        loader=jinja2.DictLoader({"merge.sql": patched_src}),
        extensions=["jinja2.ext.do"],
    )
    merge_tpl = env.get_template("merge.sql", globals=context)

    merge_tpl.module.iceberg_merge(
        on_schema_change="ignore",
        tmp_relation=MockRelation("db.schema.tbl__dbt_tmp"),
        target_relation=MockRelation("db.schema.tbl"),
        unique_key="id",
        incremental_predicates=None,
        existing_relation=MockRelation("db.schema.tbl"),
        delete_condition=delete_condition,
        update_condition=None,
        insert_condition=None,
        force_batch=False,
    )
    return adapter


class TestIcebergMergeWithExcludedColumns:
    def test_merge_sql_excludes_column_from_insert_and_update(self):
        """Simulates merge after merge_select_exclude_columns filtered _is_deleted from dest_columns."""
        dest_columns = [MockColumn("id"), MockColumn("msg")]
        adapter = _render_iceberg_merge(dest_columns, delete_condition="src._is_deleted = true")
        assert _normalize(adapter.last_sql) == (
            "merge into db.schema.tbl as target"
            " using db.schema.tbl__dbt_tmp as src"
            " on (target.id = src.id )"
            " when matched and (src._is_deleted = true) then delete"
            ' when matched then update set"msg" = src."msg"'
            ' when not matched then insert ("id", "msg")'
            ' values (src."id", src."msg")'
        )

    def test_merge_sql_without_exclude(self):
        """All source columns including _is_deleted appear in INSERT/UPDATE."""
        dest_columns = [MockColumn("id"), MockColumn("msg"), MockColumn("_is_deleted")]
        adapter = _render_iceberg_merge(dest_columns, delete_condition="src._is_deleted = true")
        assert _normalize(adapter.last_sql) == (
            "merge into db.schema.tbl as target"
            " using db.schema.tbl__dbt_tmp as src"
            " on (target.id = src.id )"
            " when matched and (src._is_deleted = true) then delete"
            ' when matched then update set"msg" = src."msg","_is_deleted" = src."_is_deleted"'
            ' when not matched then insert ("id", "msg", "_is_deleted")'
            ' values (src."id", src."msg", src."_is_deleted")'
        )
