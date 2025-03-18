import pytest

from pathlib import Path

from dbt.tests.util import run_dbt, run_dbt_and_capture, write_file


_SEEDS_SIMPLE_SEED = """
id,value,event_date
1,foo,2025-01-01
2,bar,2025-01-01
""".lstrip()

_SEEDS_SIMPLE_SEED_UPDATE = """
id,value,event_date
1,foo,2025-01-01
2,cat,2025-03-01
""".lstrip()

_MODELS_INCREMENTAL_MODEL = """
{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='insert_overwrite',
) }}

select
    id, value, event_date
from {{ ref('seed') }}
    {% if is_incremental() %}
        where event_date > '2025-02-01'
    {% endif %}
"""

_MODELS_INCREMENTAL_MODEL_WITH_COLUMNS = """
{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='insert_overwrite',
    overwrite_columns=['id', 'value']
) }}

select
    id, value, event_date
from {{ ref('seed') }}
    {% if is_incremental() %}
        where event_date > '2025-02-01'
    {% endif %}
"""


class TestInsertOverwriteIncremental:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental.sql": _MODELS_INCREMENTAL_MODEL,
            "incremental_with_cols.sql": _MODELS_INCREMENTAL_MODEL_WITH_COLUMNS,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": _SEEDS_SIMPLE_SEED,
        }

    def test_insert_overwrite_incremental(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1

        results = run_dbt(["run"])
        assert len(results) == 2

        seed_file = project.project_root / Path("seeds") / Path("seed.csv")
        write_file(_SEEDS_SIMPLE_SEED_UPDATE, seed_file)

        results = run_dbt(["seed"])
        assert len(results) == 1

        results = run_dbt(["run"])
        assert len(results) == 2

        run_results, output = run_dbt_and_capture(
            ["show", "--inline", "select * from {{ ref('incremental') }}"]
        )
        assert run_results[0].adapter_response["rows_affected"] == 1
        assert "cat" in output
        assert "2025-03-01" in output
        assert "foo" not in output
        assert "2025-01-01" not in output
        assert run_results[0].adapter_response["code"] == "SUCCESS"

        run_results, output = run_dbt_and_capture(
            [
                "show",
                "--inline",
                "select * from {{ ref('incremental_with_cols') }} where value = 'cat'",
            ]
        )
        assert run_results[0].adapter_response["rows_affected"] == 1
        assert "cat" in output
        assert "2025-03-01" not in output and "2025-01-01" not in output
        assert run_results[0].adapter_response["code"] == "SUCCESS"
