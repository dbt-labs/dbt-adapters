from argparse import Namespace
import dataclasses
from multiprocessing import get_context
from unittest import TestCase, mock

import agate
from dbt.config import RuntimeConfig

from dbt.adapters.base import AdapterPlugin, BaseAdapter, BaseRelation
from dbt.adapters.contracts.relation import Path

from tests.unit.utils import inject_adapter


class TestGetCatalog(TestCase):
    """
    Migrated from `dbt-postgres/unit/test_adapter.py::TestPostgresAdapter`
    """
    def setUp(self):
        args = Namespace(
            which="blah",
            single_threaded=False,
            vars={},
            profile_dir="/dev/null",
        )
        self.config = RuntimeConfig.from_parts(args=args)
        self.mp_context = get_context("spawn")
        self._adapter = None

    @property
    def adapter(self):
        if self._adapter is None:
            self._adapter = BaseAdapter(self.config, self.mp_context)
            inject_adapter(self._adapter, AdapterPlugin)
        return self._adapter

    @mock.patch.object(BaseAdapter, "execute_macro")
    @mock.patch.object(BaseAdapter, "_get_catalog_relations")
    def test_get_catalog_various_schemas(self, mock_get_relations, mock_execute):
        self.catalog_test(mock_get_relations, mock_execute, False)

    @mock.patch.object(BaseAdapter, "execute_macro")
    @mock.patch.object(BaseAdapter, "_get_catalog_relations")
    def test_get_filtered_catalog(self, mock_get_relations, mock_execute):
        self.catalog_test(mock_get_relations, mock_execute, True)

    def catalog_test(self, mock_get_relations, mock_execute, filtered=False):
        column_names = ["table_database", "table_schema", "table_name"]
        relations = [
            BaseRelation(path=Path(database="dbt", schema="foo", identifier="bar")),
            BaseRelation(path=Path(database="dbt", schema="FOO", identifier="baz")),
            BaseRelation(path=Path(database="dbt", schema=None, identifier="bar")),
            BaseRelation(path=Path(database="dbt", schema="quux", identifier="bar")),
            BaseRelation(path=Path(database="dbt", schema="skip", identifier="bar")),
        ]
        rows = list(map(lambda x: dataclasses.astuple(x.path), relations))
        mock_execute.return_value = agate.Table(rows=rows, column_names=column_names)

        mock_get_relations.return_value = relations

        relation_configs = []
        used_schemas = {("dbt", "foo"), ("dbt", "quux")}

        if filtered:
            catalog, exceptions = self.adapter.get_filtered_catalog(
                relation_configs, used_schemas, set([relations[0], relations[3]])
            )
        else:
            catalog, exceptions = self.adapter.get_catalog(relation_configs, used_schemas)

        tupled_catalog = set(map(tuple, catalog))
        if filtered:
            self.assertEqual(tupled_catalog, {rows[0], rows[3]})
        else:
            self.assertEqual(tupled_catalog, {rows[0], rows[1], rows[3]})

        self.assertEqual(exceptions, [])
