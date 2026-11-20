import pytest

from dbt.adapters.exceptions import DuplicateAliasError
from dbt.adapters.spark.connections import SparkConnectionMethod, SparkCredentials
from dbt_common.exceptions import DbtConfigError


def test_credentials_server_side_parameters_keys_and_values_are_strings() -> None:
    credentials = SparkCredentials(
        host="localhost",
        method=SparkConnectionMethod.THRIFT,  # type:ignore
        database="tests",
        schema="tests",
        server_side_parameters={"spark.configuration": "10"},
    )
    assert credentials.server_side_parameters["spark.configuration"] == "10"


def test_credentials_server_side_parameters_ansi_disabled_cannot_be_overridden() -> None:
    credentials = SparkCredentials(
        host="localhost",
        method=SparkConnectionMethod.THRIFT,  # type:ignore
        database="tests",
        schema="tests",
        server_side_parameters={"spark.sql.ansi.enabled": "true"},
    )
    assert credentials.server_side_parameters["spark.sql.ansi.enabled"] == "false"


def test_credentials_server_side_parameters_ansi_disabled_default() -> None:
    credentials = SparkCredentials(
        host="localhost",
        method=SparkConnectionMethod.THRIFT,  # type:ignore
        database="tests",
        schema="tests",
    )
    assert credentials.server_side_parameters["spark.sql.ansi.enabled"] == "false"


def test_credentials_reject_duplicate_catalog_and_database() -> None:
    with pytest.raises(DuplicateAliasError):
        SparkCredentials.from_dict(
            {
                "host": "localhost",
                "method": "thrift",
                "catalog": "catalog",
                "database": "database",
                "schema": "analytics",
            }
        )


@pytest.mark.parametrize("key", ["catalog", "database"])
@pytest.mark.parametrize("value", ["", "   "])
def test_credentials_reject_blank_catalog(key: str, value: str) -> None:
    with pytest.raises(DbtConfigError, match="Catalog cannot be empty"):
        SparkCredentials.from_dict(
            {
                "host": "localhost",
                "method": "thrift",
                key: value,
                "schema": "analytics",
            }
        )
