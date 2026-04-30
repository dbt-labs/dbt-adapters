import importlib.metadata
from functools import lru_cache
from typing import Any, Dict

from botocore import config

from dbt.adapters.athena.constants import (
    DEFAULT_CALCULATION_TIMEOUT,
    DEFAULT_POLLING_INTERVAL,
    DEFAULT_SPARK_COORDINATOR_DPU_SIZE,
    DEFAULT_SPARK_EXECUTOR_DPU_SIZE,
    DEFAULT_SPARK_MAX_CONCURRENT_DPUS,
    DEFAULT_SPARK_PROPERTIES,
    LOGGER,
)


@lru_cache()
def get_boto3_config(num_retries: int) -> config.Config:
    return config.Config(
        user_agent_extra="dbt-athena/" + importlib.metadata.version("dbt-athena"),
        retries={"max_attempts": num_retries, "mode": "standard"},
    )


class AthenaSparkSessionConfig:
    """
    A helper class to manage Athena Spark Session Configuration.
    """

    def __init__(self, config: Dict[str, Any], **session_kwargs: Any) -> None:
        self.config = config
        self.session_kwargs = session_kwargs

    @property
    def spark_engine_version(self) -> str:
        return str(self.config.get("spark_engine_version", ""))

    @property
    def is_spark_connect(self) -> bool:
        """True when the model requests Apache Spark 3.5+ via Spark Connect."""
        return self.spark_engine_version == "3.5"

    def set_timeout(self) -> int:
        """
        Get the timeout value.

        This function retrieves the timeout value from the parsed model's configuration. If the timeout value
        is not defined, it falls back to the default timeout value. If the retrieved timeout value is less than or
        equal to 0, a ValueError is raised as timeout must be a positive integer.

        Returns:
            int: The timeout value in seconds.

        Raises:
            ValueError: If the timeout value is not a positive integer.

        """
        timeout = self.config.get("timeout", DEFAULT_CALCULATION_TIMEOUT)
        if not isinstance(timeout, int):
            raise TypeError("Timeout must be an integer")
        if timeout <= 0:
            raise ValueError("Timeout must be a positive integer")
        LOGGER.debug(f"Setting timeout: {timeout}")
        return timeout

    def get_polling_interval(self) -> Any:
        """
        Get the polling interval for the configuration.

        Returns:
            Any: The polling interval value.

        Raises:
            KeyError: If the polling interval is not found in either `self.config`
                or `self.session_kwargs`.
        """
        try:
            return self.config["polling_interval"]
        except KeyError:
            try:
                return self.session_kwargs["polling_interval"]
            except KeyError:
                return DEFAULT_POLLING_INTERVAL

    def set_polling_interval(self) -> float:
        """
        Set the polling interval for the configuration.

        Returns:
            float: The polling interval value.

        Raises:
            ValueError: If the polling interval is not a positive integer.
        """
        polling_interval = self.get_polling_interval()
        if (
            not (isinstance(polling_interval, float) or isinstance(polling_interval, int))
            or polling_interval <= 0
        ):
            raise ValueError(
                f"Polling_interval must be a positive number. Got: {polling_interval}"
            )
        LOGGER.debug(f"Setting polling_interval: {polling_interval}")
        return float(polling_interval)

    def set_engine_config(self) -> Dict[str, Any]:
        """Set the engine configuration.

        Returns:
            Dict[str, Any]: The engine configuration.

        Raises:
            TypeError: If the engine configuration is not of type dict.
            KeyError: If the keys of the engine configuration dictionary do not match the expected format.
        """
        table_type = self.config.get("table_type", "hive")
        spark_encryption = self.config.get("spark_encryption", False)
        spark_cross_account_catalog = self.config.get("spark_cross_account_catalog", False)
        spark_requester_pays = self.config.get("spark_requester_pays", False)

        default_spark_properties: Dict[str, str] = dict(
            **(
                DEFAULT_SPARK_PROPERTIES.get(table_type, {})
                if table_type.lower() in ["iceberg", "hudi", "delta_lake"]
                else {}
            ),
            **DEFAULT_SPARK_PROPERTIES.get("spark_encryption", {}) if spark_encryption else {},
            **(
                DEFAULT_SPARK_PROPERTIES.get("spark_cross_account_catalog", {})
                if spark_cross_account_catalog
                else {}
            ),
            **(
                DEFAULT_SPARK_PROPERTIES.get("spark_requester_pays", {})
                if spark_requester_pays
                else {}
            ),
        )

        # Apache Spark 3.5+ does not accept CoordinatorDpuSize,
        # DefaultExecutorDpuSize, or SparkProperties in EngineConfiguration.
        # Spark properties must be supplied via Classifications instead.
        # https://docs.aws.amazon.com/athena/latest/ug/notebooks-spark-getting-started.html
        user_engine_config = self.config.get("engine_config", None) or {}
        provided_spark_properties = user_engine_config.pop("SparkProperties", None)
        if provided_spark_properties:
            default_spark_properties.update(provided_spark_properties)

        if self.is_spark_connect:
            engine_config = self._build_spark_connect_engine_config(
                default_spark_properties, user_engine_config
            )
        else:
            engine_config = self._build_calculations_engine_config(
                default_spark_properties, user_engine_config
            )

        if not isinstance(engine_config, dict):
            raise TypeError("Engine configuration has to be of type dict")

        expected_keys = {
            "CoordinatorDpuSize",
            "MaxConcurrentDpus",
            "DefaultExecutorDpuSize",
            "SparkProperties",
            "AdditionalConfigs",
            "Classifications",
        }

        if set(engine_config.keys()) - expected_keys:
            raise KeyError(
                f"The engine configuration keys provided do not match the expected athena engine keys: {expected_keys}"
            )

        if engine_config["MaxConcurrentDpus"] == 1:
            raise KeyError("The lowest value supported for MaxConcurrentDpus is 2")
        LOGGER.debug(f"Setting engine configuration: {engine_config}")
        return engine_config

    @staticmethod
    def _build_calculations_engine_config(
        spark_properties: Dict[str, str],
        user_engine_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Engine configuration for Spark 3.x (Calculations API)."""
        engine_config: Dict[str, Any] = {
            "CoordinatorDpuSize": DEFAULT_SPARK_COORDINATOR_DPU_SIZE,
            "MaxConcurrentDpus": DEFAULT_SPARK_MAX_CONCURRENT_DPUS,
            "DefaultExecutorDpuSize": DEFAULT_SPARK_EXECUTOR_DPU_SIZE,
            "SparkProperties": spark_properties,
        }
        engine_config.update(user_engine_config)
        # Defaults + user overrides are both stored in SparkProperties;
        # ensure the merged view wins over any SparkProperties pre-merged
        # into user_engine_config upstream.
        engine_config["SparkProperties"] = spark_properties
        return engine_config

    @staticmethod
    def _build_spark_connect_engine_config(
        spark_properties: Dict[str, str],
        user_engine_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Engine configuration for Apache Spark 3.5 (Spark Connect).

        Spark 3.5 rejects ``CoordinatorDpuSize``, ``DefaultExecutorDpuSize``,
        and ``SparkProperties`` — the latter must be supplied via
        ``Classifications`` with name ``spark-defaults``.
        """
        engine_config: Dict[str, Any] = {
            "MaxConcurrentDpus": DEFAULT_SPARK_MAX_CONCURRENT_DPUS,
        }
        engine_config.update(user_engine_config)
        for rejected in ("CoordinatorDpuSize", "DefaultExecutorDpuSize", "SparkProperties"):
            engine_config.pop(rejected, None)

        if spark_properties:
            classifications = engine_config.get("Classifications", [])
            merged = {k: str(v) for k, v in spark_properties.items()}
            existing = next(
                (c for c in classifications if c.get("Name") == "spark-defaults"), None
            )
            if existing:
                existing.setdefault("Properties", {}).update(merged)
            else:
                classifications.append({"Name": "spark-defaults", "Properties": merged})
            engine_config["Classifications"] = classifications
        return engine_config
