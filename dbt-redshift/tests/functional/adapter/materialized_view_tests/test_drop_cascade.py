"""
This test addresses this bug: https://github.com/dbt-labs/dbt-redshift/issues/642

Redshift did not initially support DROP CASCADE for materialized views,
or at least did not document that they did. Now that they do, we should
use DROP CASCADE instead of DROP.
"""

from dbt.tests.util import run_dbt
import pytest


SEED = """
id
1
""".strip()


PARENT_MATERIALIZED_VIEW = """
{{ config(
    materialized='materialized_view',
    on_configuration_change='apply',
) }}

select * from {{ ref('my_seed') }}
"""


CHILD_MATERIALIZED_VIEW = """
{{ config(
    materialized='materialized_view',
    on_configuration_change='apply',
) }}

select * from {{ ref('parent_mv') }}
"""


@pytest.fixture(scope="class")
def seeds():
    return {"my_seed.csv": SEED}


@pytest.fixture(scope="class")
def models():
    return {
        "parent_mv.sql": PARENT_MATERIALIZED_VIEW,
        "child_mv.sql": CHILD_MATERIALIZED_VIEW,
    }


def test_drop_cascade(project):
    run_dbt(["seed"])
    run_dbt(["run"])
    # this originally raised an error when it should not have
    run_dbt(["run", "--full-refresh"])
