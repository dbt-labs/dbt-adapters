from types import SimpleNamespace

from dbt_common.behavior_flags import BehaviorFlagRendered
from dbt.adapters.bigquery.impl import BIGQUERY_NOOP_ALTER_RELATION_COMMENT
import jinja2


def _load_bigquery_adapters_macros(adapter):
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader("src/dbt/include/bigquery/macros"),
        extensions=["jinja2.ext.do"],
    )
    # alter_relation_comment now emits DDL via run_query; capture what it is handed
    # and expose it on the module as `captured_sql` for assertions.
    captured_sql = []
    env.globals["run_query"] = captured_sql.append
    template = env.get_template("adapters.sql")
    module = template.make_module({"adapter": adapter})
    module.captured_sql = captured_sql
    return module


def test_alter_relation_comment_noop_when_behavior_flag_true():
    adapter = SimpleNamespace(
        behavior=SimpleNamespace(
            bigquery_noop_alter_relation_comment=BehaviorFlagRendered(
                BIGQUERY_NOOP_ALTER_RELATION_COMMENT,
                {BIGQUERY_NOOP_ALTER_RELATION_COMMENT["name"]: True},
            )
        ),
    )
    assert adapter.behavior.bigquery_noop_alter_relation_comment.no_warn is True
    relation = SimpleNamespace(type="table", render=lambda: "`db`.`sch`.`ident`")

    macros = _load_bigquery_adapters_macros(adapter)
    macros.bigquery__alter_relation_comment(relation, "desc")

    assert macros.captured_sql == []


def test_alter_relation_comment_emits_ddl_when_behavior_flag_false():
    adapter = SimpleNamespace(
        behavior=SimpleNamespace(
            bigquery_noop_alter_relation_comment=BehaviorFlagRendered(
                BIGQUERY_NOOP_ALTER_RELATION_COMMENT, {}
            )
        ),
    )
    assert adapter.behavior.bigquery_noop_alter_relation_comment.no_warn is False

    # relation.type "materialized_view" maps to the "materialized view" SQL keyword.
    for relation_type, keyword in [
        ("table", "table"),
        ("view", "view"),
        ("materialized_view", "materialized view"),
    ]:
        relation = SimpleNamespace(type=relation_type, render=lambda: "`db`.`sch`.`ident`")
        macros = _load_bigquery_adapters_macros(adapter)
        macros.bigquery__alter_relation_comment(relation, "desc")

        assert len(macros.captured_sql) == 1
        assert (
            macros.captured_sql[0].strip()
            == f'alter {keyword} `db`.`sch`.`ident` set options(description="desc");'
        )
