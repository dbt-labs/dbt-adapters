from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import dbt_common.exceptions
from dbt.adapters.relation_configs import RelationConfigChange
from dbt.adapters.contracts.relation import RelationConfig
from dbt_common.dataclass_schema import dbtClassMixin, ValidationError
from google.cloud.bigquery.table import Table as BigQueryTable


@dataclass
class PartitionConfig(dbtClassMixin):
    field: str
    data_type: str = "date"
    granularity: str = "day"
    range: Optional[Dict[str, Any]] = None
    time_ingestion_partitioning: bool = False
    copy_partitions: bool = False
    copy_partitions_concurrency: Optional[int] = None

    PARTITION_DATE = "_PARTITIONDATE"
    PARTITION_TIME = "_PARTITIONTIME"

    # Bounded by BigQuery's partitioned-table metadata rate limit: 50
    # modifications per 10 seconds per destination table, which includes copy
    # jobs. Transient overshoot is retried with backoff, but a wide-open pool
    # would just trade wall-clock for rate-limit churn.
    MAX_COPY_PARTITIONS_CONCURRENCY = 32

    _PARTITION_ID_FORMATS = {
        "hour": "%Y%m%d%H",
        "day": "%Y%m%d",
        "month": "%Y%m",
        "year": "%Y",
    }

    def render_partition_id(self, partition: Any) -> str:
        """The partition decorator suffix for `table$<partition_id>`, as used by
        copy_partitions. Ports the formatting previously inlined in the
        bq_copy_partitions macro."""
        if self.data_type == "int64":
            return str(partition)
        return partition.strftime(self._PARTITION_ID_FORMATS[self.granularity])

    @classmethod
    def validate_copy_partitions_concurrency(cls, value: Any) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int):
            raise dbt_common.exceptions.base.DbtValidationError(
                f"`copy_partitions_concurrency` must be an integer between 1 and "
                f"{cls.MAX_COPY_PARTITIONS_CONCURRENCY}, got {value!r}"
            )
        if not 1 <= value <= cls.MAX_COPY_PARTITIONS_CONCURRENCY:
            raise dbt_common.exceptions.base.DbtValidationError(
                f"`copy_partitions_concurrency` must be between 1 and "
                f"{cls.MAX_COPY_PARTITIONS_CONCURRENCY} (BigQuery rate-limits partition "
                f"modifications per destination table), got {value}"
            )
        return value

    def data_type_for_partition(self):
        """Return the data type of partitions for replacement.
        When time_ingestion_partitioning is enabled, the data type supported are date & timestamp.
        """
        if not self.time_ingestion_partitioning:
            return self.data_type

        return "date" if self.data_type == "date" else "timestamp"

    def reject_partition_field_column(self, columns: List[Any]) -> List[str]:
        return [c for c in columns if not c.name.upper() == self.field.upper()]

    def data_type_should_be_truncated(self):
        """Return true if the data type should be truncated instead of cast to the data type."""
        return not (
            self.data_type == "int64" or (self.data_type == "date" and self.granularity == "day")
        )

    def time_partitioning_field(self) -> str:
        """Return the time partitioning field name based on the data type.
        The default is _PARTITIONTIME, but for date it is _PARTITIONDATE
        else it will fail statements for type mismatch."""
        if self.data_type == "date":
            return self.PARTITION_DATE
        else:
            return self.PARTITION_TIME

    def insertable_time_partitioning_field(self) -> str:
        """Return the insertable time partitioning field name based on the data type.
        Practically, only _PARTITIONTIME works so far.
        The function is meant to keep the call sites consistent as it might evolve."""
        return self.PARTITION_TIME

    def render(self, alias: Optional[str] = None):
        column: str = (
            self.field if not self.time_ingestion_partitioning else self.time_partitioning_field()
        )
        if alias:
            column = f"{alias}.{column}"

        if self.data_type_should_be_truncated():
            return f"{self.data_type}_trunc({column}, {self.granularity})"
        else:
            return column

    def render_wrapped(self, alias: Optional[str] = None):
        """Render the partition field normalized to partition boundaries.

        For time-based partitions, wraps the column in the appropriate cast
        to ensure it matches the partition data type.

        For int64 range partitions, normalizes values to their partition start
        boundary using: value - MOD(value - range_start, range_interval).
        This prevents generating excessively large arrays of distinct values
        when computing partitions for replacement in insert_overwrite.
        """
        # int64 range partitions: normalize to partition start boundary
        if self.data_type == "int64" and self.range is not None:
            column = self.render(alias)
            start = self.range["start"]
            interval = self.range["interval"]
            return f"({column} - MOD({column} - {start}, {interval}))"

        # time-based: wrap with cast if not already truncated
        if (
            self.data_type in ("date", "timestamp", "datetime")
            and not self.data_type_should_be_truncated()
            and not (
                self.time_ingestion_partitioning and self.data_type == "date"
            )  # _PARTITIONDATE is already a date
        ):
            return f"{self.data_type}({self.render(alias)})"
        else:
            return self.render(alias)

    @classmethod
    def parse(cls, raw_partition_by) -> Optional["PartitionConfig"]:
        if raw_partition_by is None:
            return None
        try:
            cls.validate(raw_partition_by)
            config = cls.from_dict(
                {
                    key: (value.lower() if isinstance(value, str) else value)
                    for key, value in raw_partition_by.items()
                }
            )
            cls.validate_copy_partitions_concurrency(config.copy_partitions_concurrency)
            return config
        except ValidationError as exc:
            raise dbt_common.exceptions.base.DbtValidationError(
                "Could not parse partition config"
            ) from exc
        except TypeError:
            raise dbt_common.exceptions.CompilationError(
                f"Invalid partition_by config:\n"
                f"  Got: {raw_partition_by}\n"
                f'  Expected a dictionary with "field" and "data_type" keys'
            )

    @classmethod
    def parse_model_node(cls, relation_config: RelationConfig) -> Dict[str, Any]:
        """
        Parse model node into a raw config for `PartitionConfig.parse`

        - Note:
            This doesn't currently collect `time_ingestion_partitioning` and `copy_partitions`
            because this was built for materialized views, which do not support those settings.
        """
        config_dict: Dict[str, Any] = relation_config.config.extra.get(  # type:ignore
            "partition_by"
        )
        if "time_ingestion_partitioning" in config_dict:
            del config_dict["time_ingestion_partitioning"]
        if "copy_partitions" in config_dict:
            del config_dict["copy_partitions"]
        if "copy_partitions_concurrency" in config_dict:
            del config_dict["copy_partitions_concurrency"]
        return config_dict

    @classmethod
    def parse_bq_table(cls, table: BigQueryTable) -> Dict[str, Any]:
        """
        Parse the BQ Table object into a raw config for `PartitionConfig.parse`

        - Note:
            This doesn't currently collect `time_ingestion_partitioning` and `copy_partitions`
            because this was built for materialized views, which do not support those settings.
        """
        if time_partitioning := table.time_partitioning:
            field_types = {field.name: field.field_type.lower() for field in table.schema}
            config_dict = {
                "field": time_partitioning.field,
                "data_type": field_types[time_partitioning.field],
                "granularity": time_partitioning.type_,
            }

        elif range_partitioning := table.range_partitioning:
            config_dict = {
                "field": range_partitioning.field,
                "data_type": "int64",
                "range": {
                    "start": range_partitioning.range_.start,
                    "end": range_partitioning.range_.end,
                    "interval": range_partitioning.range_.interval,
                },
            }

        else:
            config_dict = {}

        return config_dict


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class BigQueryPartitionConfigChange(RelationConfigChange):
    context: Optional[Any] = None

    @property
    def requires_full_refresh(self) -> bool:
        return True
