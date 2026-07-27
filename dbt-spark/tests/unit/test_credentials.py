from dbt.adapters.spark.connections import SparkConnectionMethod, SparkCredentials


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
