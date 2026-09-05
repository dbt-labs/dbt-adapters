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

    def test_external_volume_from_model_config(self):
        """Model-level external_volume takes precedence over integration-level."""
        model = MagicMock(spec=RelationConfig)
        model.config = {"external_volume": "model_level_volume"}

        result = self.integration._get_external_volume(model)
        self.assertEqual("model_level_volume", result)

    def test_external_volume_fallback_to_integration(self):
        """Falls back to integration-level external_volume when not set on model."""
        model = MagicMock(spec=RelationConfig)
        model.config = {}

        result = self.integration._get_external_volume(model)
        self.assertEqual("test_external_volume", result)

    def test_build_relation_with_model_external_volume(self):
        """Model-level external_volume is used in build_relation."""
        model = MagicMock(spec=RelationConfig)
        model.config = {"external_volume": "gs://model-bucket"}
        model.schema = "test_schema"
        model.name = "test_model"

        relation = self.integration.build_relation(model)
        self.assertEqual("gs://model-bucket", relation.external_volume)
        self.assertEqual("gs://model-bucket/_dbt/test_schema/test_model", relation.storage_uri)
