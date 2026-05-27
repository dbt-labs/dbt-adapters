import dataclasses
from typing import Any, Optional

from dbt_common.record import Record, Recorder


@dataclasses.dataclass
class CursorGetStatsParams:
    connection_name: str


@dataclasses.dataclass
class CursorGetStatsResult:
    """Captures the stats from SnowflakeCursor.stats (QueryResultStats).

    These stats are available in snowflake-connector-python >= 4.2.0 and provide
    detailed DML operation information.
    """

    num_rows_inserted: Optional[int]
    num_rows_deleted: Optional[int]
    num_rows_updated: Optional[int]
    num_dml_duplicates: Optional[int]


class StatsProxy:
    """A proxy object that mimics the QueryResultStats interface for replay mode."""

    def __init__(self, result: Optional[CursorGetStatsResult]) -> None:
        self._result = result

    @property
    def num_rows_inserted(self) -> Optional[int]:
        return self._result.num_rows_inserted if self._result else None

    @property
    def num_rows_deleted(self) -> Optional[int]:
        return self._result.num_rows_deleted if self._result else None

    @property
    def num_rows_updated(self) -> Optional[int]:
        return self._result.num_rows_updated if self._result else None

    @property
    def num_dml_duplicates(self) -> Optional[int]:
        return self._result.num_dml_duplicates if self._result else None


@Recorder.register_record_type
class CursorGetStatsRecord(Record):
    params_cls = CursorGetStatsParams
    result_cls = CursorGetStatsResult
    group = "Database"

    @classmethod
    def _record_result(cls, result: Any) -> Optional[CursorGetStatsResult]:
        """Convert the native stats object to our result dataclass."""
        if result is None:
            return None
        return CursorGetStatsResult(
            num_rows_inserted=getattr(result, "num_rows_inserted", None),
            num_rows_deleted=getattr(result, "num_rows_deleted", None),
            num_rows_updated=getattr(result, "num_rows_updated", None),
            num_dml_duplicates=getattr(result, "num_dml_duplicates", None),
        )

    @classmethod
    def _replay_result(cls, result: Optional[CursorGetStatsResult]) -> Optional[StatsProxy]:
        """Convert our result dataclass back to a stats-like object for replay."""
        if result is None:
            return None
        return StatsProxy(result)
