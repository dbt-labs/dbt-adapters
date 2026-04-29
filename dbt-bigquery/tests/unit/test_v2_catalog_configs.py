import pytest

from dbt.adapters.bigquery.catalogs._v2 import BiglakeMetastoreBigqueryConfig
from dbt.adapters.catalogs import get_catalog_config
from dbt_common.exceptions import DbtValidationError


def test_biglake_registered():
    assert get_catalog_config("biglake_metastore", "bigquery") is BiglakeMetastoreBigqueryConfig


def test_biglake_minimal_valid():
    cfg = BiglakeMetastoreBigqueryConfig(external_volume="gs://my-bucket", file_format="parquet")
    assert cfg.external_volume == "gs://my-bucket"


def test_biglake_with_base_location_root():
    cfg = BiglakeMetastoreBigqueryConfig(
        external_volume="gs://my-bucket", file_format="parquet", base_location_root="root"
    )
    assert cfg.base_location_root == "root"


def test_biglake_blank_external_volume():
    with pytest.raises(DbtValidationError, match="external_volume.*non-empty"):
        BiglakeMetastoreBigqueryConfig(external_volume="  ", file_format="parquet")


def test_biglake_external_volume_must_be_gs():
    with pytest.raises(DbtValidationError, match="gs://"):
        BiglakeMetastoreBigqueryConfig(external_volume="s3://my-bucket", file_format="parquet")


def test_biglake_file_format_must_be_parquet():
    with pytest.raises(DbtValidationError, match="file_format must be 'parquet'"):
        BiglakeMetastoreBigqueryConfig(external_volume="gs://my-bucket", file_format="orc")


def test_biglake_blank_base_location_root():
    with pytest.raises(DbtValidationError, match="base_location_root.*blank"):
        BiglakeMetastoreBigqueryConfig(
            external_volume="gs://my-bucket", file_format="parquet", base_location_root="   "
        )


def test_biglake_validate_rejects_unknown_keys():
    with pytest.raises(Exception, match="Additional properties"):
        BiglakeMetastoreBigqueryConfig.validate(
            {"external_volume": "gs://x", "file_format": "parquet", "extra": "bad"}
        )
