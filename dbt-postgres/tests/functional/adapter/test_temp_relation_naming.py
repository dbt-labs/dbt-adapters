import pytest
import re
from pathlib import Path
from dbt.tests.util import run_dbt


_MODEL_INCREMENTAL = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns'
    )
}}

select
    1 as id,
    'value' as name
"""


class TestTempRelationNaming:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_incremental_model.sql": _MODEL_INCREMENTAL}

    def test_incremental_with_schema_change_creates_temp_relation(self, project):
        results = run_dbt(["run"])
        assert len(results) == 1

        results = run_dbt(["run", "--log-level", "debug", "--log-format", "text"])
        assert len(results) == 1

        log_dir = Path(project.project_root) / "logs"
        if log_dir.exists():
            log_files = sorted(
                log_dir.glob("dbt.log*"), key=lambda p: p.stat().st_mtime, reverse=True
            )
            if log_files:
                log_content = log_files[0].read_text()
                temp_table_pattern = r"__dbt_tmp\d{12}\b"
                matches = re.findall(temp_table_pattern, log_content)

                assert len(matches) > 0, (
                    f"Expected temp table with pattern '{temp_table_pattern}' "
                    f"(12 digits from %H%M%S%f format)"
                )

                for match in matches:
                    suffix = match.replace("__dbt_tmp", "")
                    assert len(suffix) == 12 and suffix.isdigit()

        results = run_dbt(["run"])
        assert len(results) == 1
