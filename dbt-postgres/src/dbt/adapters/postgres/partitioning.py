"""Native PostgreSQL declarative partitioning support (issue #679).

https://www.postgresql.org/docs/current/ddl-partitioning.html
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from dateutil.relativedelta import relativedelta

from dbt_common.dataclass_schema import ValidationError, dbtClassMixin
from dbt_common.exceptions import DbtRuntimeError


PARTITION_METHODS = ("range", "list", "hash")
RANGE_GRANULARITIES = ("hour", "day", "week", "month", "year")

# Upper bound on auto-generated range partitions per build. Guards against, e.g.,
# `granularity='hour'` over a multi-year range emitting tens of thousands of
# `CREATE TABLE` statements in a single request.
MAX_AUTO_PARTITIONS = 10000

_GRANULARITY_NAME_FMT = {
    "year": "%Y",
    "month": "%Y%m",
    "week": "%Y%m%d",
    "day": "%Y%m%d",
    "hour": "%Y%m%d%H",
}


def _as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    return datetime.fromisoformat(str(value))


def _floor_to_granularity(dt: datetime, granularity: str) -> datetime:
    if granularity == "year":
        return dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    if granularity == "month":
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if granularity == "week":
        floored = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return floored - timedelta(days=floored.weekday())
    if granularity == "day":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    # hour
    return dt.replace(minute=0, second=0, microsecond=0)


def _granularity_step(granularity: str):
    return {
        "year": relativedelta(years=1),
        "month": relativedelta(months=1),
        "week": timedelta(weeks=1),
        "day": timedelta(days=1),
        "hour": timedelta(hours=1),
    }[granularity]


def compute_partition_bounds(minimum: Any, maximum: Any, granularity: str) -> List[Dict[str, str]]:
    """
    Compute the range partitions needed to cover [minimum, maximum] at the given
    granularity. Returns a list of dicts: {"name": suffix, "from": literal, "to": literal},
    where the literals are quoted SQL timestamps for a `FOR VALUES FROM (..) TO (..)` clause.
    """
    if minimum is None or maximum is None:
        return []

    current = _floor_to_granularity(_as_datetime(minimum), granularity)
    end = _as_datetime(maximum)
    step = _granularity_step(granularity)

    bounds: List[Dict[str, str]] = []
    while current <= end:
        if len(bounds) >= MAX_AUTO_PARTITIONS:
            raise DbtRuntimeError(
                f"partition_by would auto-create more than {MAX_AUTO_PARTITIONS} '{granularity}' "
                f"partitions for the range [{minimum}, {maximum}]. Use a coarser granularity "
                "or declare explicit `partitions`."
            )
        nxt = current + step
        bounds.append(
            {
                "name": "p" + current.strftime(_GRANULARITY_NAME_FMT[granularity]),
                "from": "'" + current.strftime("%Y-%m-%d %H:%M:%S") + "'",
                "to": "'" + nxt.strftime("%Y-%m-%d %H:%M:%S") + "'",
            }
        )
        current = nxt
    return bounds


@dataclass
class PostgresPartitionConfig(dbtClassMixin):
    """
    Native PostgreSQL declarative partitioning config (issue #679).

    - fields: one or more columns/expressions that make up the partition key
    - method: `range`, `list`, or `hash`
    - granularity: for `range`, drives auto-management of partitions (bounds + names);
      one of `hour`, `day`, `week`, `month`, `year`
    - default_partition: create a DEFAULT partition to catch rows outside every partition
    - partitions: explicit static partition definitions, e.g.
        range: {"name": "p2024", "from": "'2024-01-01'", "to": "'2025-01-01'"}
        list:  {"name": "p_us", "values": ["'us'"]}
        hash:  {"name": "p0", "modulus": 2, "remainder": 0}
    """

    fields: List[str]
    method: str = "range"
    granularity: Optional[str] = None
    default_partition: bool = True
    partitions: Optional[List[Dict[str, Any]]] = None

    @property
    def render(self) -> str:
        """The `PARTITION BY ...` key clause, e.g. `range (created_at)`."""
        return f"{self.method} ({', '.join(self.fields)})"

    def _validate(self) -> None:
        if not self.fields:
            raise DbtRuntimeError(
                "partition_by requires at least one column in `fields`, but none were provided"
            )
        if self.method not in PARTITION_METHODS:
            raise DbtRuntimeError(
                f"Invalid partition_by method '{self.method}'. "
                f"Supported methods are: {', '.join(PARTITION_METHODS)}"
            )
        if self.granularity is not None and self.granularity not in RANGE_GRANULARITIES:
            raise DbtRuntimeError(
                f"Invalid partition_by granularity '{self.granularity}'. "
                f"Supported granularities are: {', '.join(RANGE_GRANULARITIES)}"
            )
        if self.granularity is not None and self.method != "range":
            raise DbtRuntimeError(
                "partition_by `granularity` is only supported for the `range` method"
            )

    @classmethod
    def parse(cls, raw_partition_by: Any) -> Optional["PostgresPartitionConfig"]:
        if raw_partition_by is None:
            return None
        try:
            cls.validate(raw_partition_by)
            partition_by: "PostgresPartitionConfig" = cls.from_dict(raw_partition_by)
        except ValidationError as exc:
            raise DbtRuntimeError(f"Could not parse partition_by config: {exc}")
        except TypeError:
            raise DbtRuntimeError(f"partition_by must be a dict, but got: {raw_partition_by}")
        partition_by._validate()
        return partition_by
