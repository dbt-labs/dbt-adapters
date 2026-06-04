from types import SimpleNamespace

import pytest

from dbt.adapters.redshift import constants
from dbt.adapters.redshift.catalogs import GlueCatalogIntegration, GlueCatalogRelation


def _config(**kwargs) -> SimpleNamespace:
    base = dict(
        name="my_glue_catalog",
        catalog_name="my_glue_catalog",
        catalog_type=constants.GLUE_CATALOG_TYPE,
        table_format=constants.ICEBERG_TABLE_FORMAT,
        external_volume=None,
        file_format=None,
        adapter_properties={},
    )
    base.update(kwargs)
    return SimpleNamespace(**base)


def _model(config: dict) -> SimpleNamespace:
    return SimpleNamespace(config=config)


def test_integration_class_attributes():
    assert GlueCatalogIntegration.catalog_type == constants.GLUE_CATALOG_TYPE
    assert GlueCatalogIntegration.table_format == constants.ICEBERG_TABLE_FORMAT
    assert GlueCatalogIntegration.allows_writes is True


def test_build_relation_reads_model_config():
    integration = GlueCatalogIntegration(_config())
    model = _model(
        {
            "partition_by": ["day(event_ts)", "identity(region)"],
            "table_properties": {"compression_type": "zstd"},
            "external_volume": "s3://my-bucket/prefix/",
        }
    )

    relation = integration.build_relation(model)

    assert isinstance(relation, GlueCatalogRelation)
    assert relation.table_format == constants.ICEBERG_TABLE_FORMAT
    assert relation.catalog_name == "my_glue_catalog"
    assert relation.partition_by == ["day(event_ts)", "identity(region)"]
    assert relation.table_properties == {"compression_type": "zstd"}
    assert relation.external_volume == "s3://my-bucket/prefix/"


def test_build_relation_falls_back_to_integration_external_volume():
    integration = GlueCatalogIntegration(_config(external_volume="s3://default-volume/"))
    relation = integration.build_relation(_model({}))

    assert relation.external_volume == "s3://default-volume/"
    assert relation.partition_by is None
    assert relation.table_properties is None


def test_build_relation_location_alias():
    integration = GlueCatalogIntegration(_config())
    relation = integration.build_relation(_model({"location": "s3://aliased/"}))
    assert relation.external_volume == "s3://aliased/"


@pytest.mark.parametrize("partition_by", ["day(event_ts)", ["day(event_ts)"]])
def test_partition_by_accepts_string_or_list(partition_by):
    integration = GlueCatalogIntegration(_config())
    relation = integration.build_relation(_model({"partition_by": partition_by}))
    assert relation.partition_by == partition_by
