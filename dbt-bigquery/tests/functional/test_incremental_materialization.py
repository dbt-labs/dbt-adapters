import json
import pytest
import re
from dbt.tests.util import run_dbt, run_dbt_and_capture

# This is a short term hack, we need to go back
# and make adapter implementations of:
# https://github.com/dbt-labs/dbt-core/pull/6330


_COMMENT_RE = re.compile(r'/\*\s*{[^}]*"dbt_version"\s*:\s*"[^"]+"[^}]*}\s*\*/')

_INCREMENTAL_MODEL = """
{{
    config(
        materialized="incremental",
    )
}}

{% if not is_incremental() %}

    select
        10 as id, cast('2020-01-01 01:00:00' as datetime) as date_hour
    union all select
        30 as id, cast('2020-01-01 02:00:00' as datetime) as date_hour

{% else %}

    select
        20 as id, cast('2020-01-01 01:00:00' as datetime) as date_hour
    union all select
        40 as id, cast('2020-01-01 02:00:00' as datetime) as date_hour

{% endif %}
-- Test Comment To Prevent Recurrence of
--     https://github.com/dbt-labs/dbt-core/issues/6485
"""

INCREMENTAL_MODEL_COPY_PARTITIONS = """
{{
  config(
      materialized='incremental',
      incremental_strategy='insert_overwrite',
      partition_by={
          'field': '_partition',
          'granularity': 'day',
          'data_type': 'timestamp',
          'time_ingestion_partitioning': True,
          'copy_partitions': True,
      },
      on_schema_change='append_new_columns'
  )
}}
SELECT
  timestamp_trunc(current_timestamp(), day) AS _partition,
  'some value'                               AS col1
"""


class BaseIncrementalModelConfig:
    @pytest.fixture(scope="class")
    def models(self):
        return {"test_incremental.sql": _INCREMENTAL_MODEL}


class TestIncrementalModel(BaseIncrementalModelConfig):
    def test_incremental_model_succeeds(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1
        results = run_dbt(["run"])
        assert len(results) == 1


class TestAllQueriesHaveDbtComment:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_incremental_model.sql": INCREMENTAL_MODEL_COPY_PARTITIONS}

    def _extract_executed_sql(self, raw_logs: str) -> list[str]:
        """
        Return every SQL script that dbt 1.4+ actually sent to BigQuery.

        In JSON logs each statement is logged by an event whose `data`
        payload is a dict containing a key `"sql"`.
        """
        scripts: list[str] = []
        for line in raw_logs.splitlines():
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                continue

            data = parsed.get("data")
            if isinstance(data, dict) and "sql" in data:
                sql = str(data["sql"]).strip()
                if sql:
                    scripts.append(sql)
        return scripts

    def _has_structured_comment(self, sql: str) -> bool:
        """True iff the first non-blank line is the structured dbt comment."""
        first_line = sql.lstrip().splitlines()[0]
        return bool(_COMMENT_RE.fullmatch(first_line))

    def test_every_query_has_comment(self, project):
        run_dbt(["run"])
        _, raw_logs = run_dbt_and_capture(["--debug", "--log-format=json", "run"])

        executed_sqls = self._extract_executed_sql(raw_logs)
        assert executed_sqls, "No SQL was captured from the dbt logs"

        missing = [sql for sql in executed_sqls if not self._has_structured_comment(sql)]

        assert not missing, (
            f"{len(missing)} queries are missing structured dbt comments.\n\n"
            + "\n\n---\n\n".join(missing)
        )
