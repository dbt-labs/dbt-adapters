import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock
from dbt.adapters.bigquery.catalogs import BigLakeCatalogIntegration
from dbt.adapters.contracts.relation import RelationConfig


class TestBigLakeCatalogIntegration(unittest.TestCase):
    def setUp(self):
        self.integration = BigLakeCatalogIntegration(
            config=SimpleNamespace(
                name="test_biglake_catalog_integration",
                external_volume="test_external_volume",
                catalog_type="biglake",
                catalog_name="test_catalog_name",
                table_format="test_table_format",
                file_format="test_file_format",
            )
        )
        self.integration.external_volume = "test_external_volume"

    def test_storage_uri_no_inputs(self):
        model = MagicMock(spec=RelationConfig)
        model.config = {"has": "a_value"}
        model.schema = "test_schema"
        model.name = "test_model_name"

        expected_uri = "test_external_volume/_dbt/test_schema/test_model_name"
        result = self.integration._calculate_storage_uri(model)
        self.assertEqual(expected_uri, result)

    def test_storage_uri_base_location_root(self):
        model = MagicMock(spec=RelationConfig)
        model.config = {"base_location_root": "foo"}
        model.schema = "test_schema"
        model.name = "test_model_name"

        expected_uri = "test_external_volume/foo/test_schema/test_model_name"
        result = self.integration._calculate_storage_uri(model)
        self.assertEqual(expected_uri, result)

    def test_storage_uri_base_location_subpath(self):
        model = MagicMock(spec=RelationConfig)
        model.config = {"base_location_subpath": "bar"}
        model.schema = "test_schema"
        model.name = "test_model_name"

        expected_uri = "test_external_volume/_dbt/test_schema/test_model_name/bar"
        result = self.integration._calculate_storage_uri(model)
        self.assertEqual(expected_uri, result)

    def test_storage_uri_base_location_root_and_subpath(self):
        model = MagicMock(spec=RelationConfig)
        model.config = {"base_location_root": "foo", "base_location_subpath": "bar"}
        model.schema = "test_schema"
        model.name = "test_model_name"

        expected_uri = "test_external_volume/foo/test_schema/test_model_name/bar"
        result = self.integration._calculate_storage_uri(model)
        self.assertEqual(expected_uri, result)

    def test_storage_uri_from_model_config(self):
        model = MagicMock(spec=RelationConfig)
        model.config = {"storage_uri": "custom_storage_uri"}
        model.schema = "test_schema"
        model.name = "test_model_name"

        expected_uri = "custom_storage_uri"
        result = self.integration._calculate_storage_uri(model)
        self.assertEqual(expected_uri, result)
