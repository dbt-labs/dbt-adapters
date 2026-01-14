from types import SimpleNamespace
from unittest import mock

import jinja2


def _load_bigquery_adapters_macros(adapter):
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader("src/dbt/include/bigquery/macros"),
        extensions=["jinja2.ext.do"],
    )
    template = env.get_template("adapters.sql")
    return template.make_module({"adapter": adapter})


def test_alter_relation_comment_noop_when_behavior_flag_true():
    adapter = SimpleNamespace(
        behavior=SimpleNamespace(bigquery_noop_alter_relation_comment=True),
        update_table_description=mock.Mock(),
    )
    relation = SimpleNamespace(database="db", schema="sch", identifier="ident")

    macros = _load_bigquery_adapters_macros(adapter)
    macros.bigquery__alter_relation_comment(relation, "desc")

    adapter.update_table_description.assert_not_called()


def test_alter_relation_comment_calls_update_when_behavior_flag_false():
    adapter = SimpleNamespace(
        behavior=SimpleNamespace(bigquery_noop_alter_relation_comment=False),
        update_table_description=mock.Mock(),
    )
    relation = SimpleNamespace(database="db", schema="sch", identifier="ident")

    macros = _load_bigquery_adapters_macros(adapter)
    macros.bigquery__alter_relation_comment(relation, "desc")

    adapter.update_table_description.assert_called_once_with("db", "sch", "ident", "desc")
