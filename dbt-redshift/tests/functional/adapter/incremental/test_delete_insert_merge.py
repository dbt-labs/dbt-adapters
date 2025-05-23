import json
import os

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture


MODEL_SQL = """
{{ config(
    materialized='incremental',
    unique_key='id'
) }}

SELECT 1 AS id, 'foo' AS value
"""
EXPECTED_SQL_FRAGMENTS = [
    "delete from",
    "using",
    "insert into",
]
UNEXPECTED_SQL_PATTERN = r"delete from\s+\{\{\s*this\s*\}\}\s+AS\s+\w+"


@pytest.fixture(scope="class")
def models():
    return {"model.sql": MODEL_SQL}


def test_delete_insert_merge(project):
    """
    Addresses https://github.com/dbt-labs/dbt-adapters/issues/1032
    """
    # run once to create the base table
    run_dbt(["run"])

    # run a second time to trigger the incremental load
    results, logs = run_dbt_and_capture(["--debug", "run"])

    # make sure we did something
    assert len(results) == 1

    # check the logs for expected values, or lack thereof
    for fragment in EXPECTED_SQL_FRAGMENTS:
        assert fragment in logs
    assert UNEXPECTED_SQL_PATTERN not in logs
