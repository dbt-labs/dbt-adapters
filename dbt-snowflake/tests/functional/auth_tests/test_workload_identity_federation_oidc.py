"""
Functional tests for Snowflake Workload Identity Federation (WIF) with OIDC authentication.
Prerequisites for testing WIF with OIDC:

1. **Create a Snowflake User with OIDC Auth**

  Create a service user in Snowflake with WIF enabled:
  ```sql
  CREATE USER <username>
    TYPE = SERVICE
    WORKLOAD_IDENTITY = (
      TYPE = OIDC,
      ISSUER = 'https://token.actions.githubusercontent.com',
      SUBJECT = 'repo:<REPO_OWNER>/dbt-adapters:ref:refs/heads/main',
      OIDC_AUDIENCE_LIST = ('snowflakecomputing.com')
    );
  ```

2. **Create a GitHub Actions that generates the OIDC token and runs the test **

  ```yaml

name: Run Snowflake Workload Identity Federation (WIF) Test
on:
  workflow_dispatch:
  push:
    branches: [ main ]

permissions:
  contents: read
  id-token: write

jobs:
  run-snowflake:
    runs-on: ubuntu-latest
    env:
      SNOWFLAKE_TEST_ACCOUNT: <ACCOUNT_ID>
      SNOWFLAKE_TEST_DATABASE: <DB_NAME>
      SNOWFLAKE_TEST_WAREHOUSE: <WH_NAME>
      SNOWFLAKE_TEST_ROLE: <ROLE_NAME>
      SNOWFLAKE_TEST_WIF_USER: <USERNAME>

    steps:
      - uses: actions/checkout@v4

      - name: Get OIDC token for Snowflake
        id: oidc
        uses: actions/github-script@v7
        with:
          script: |
            const token = await core.getIDToken('snowflakecomputing.com');
            core.setOutput('id_token', token);

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - uses: pypa/hatch@install

      - run: hatch run setup
        working-directory: ./dbt-snowflake

      - run: hatch run python -m pytest tests/functional/auth_tests/test_workload_identity_federation_oidc.py
        working-directory: ./dbt-snowflake
        env:
          ODIC_TOKEN: ${{ steps.oidc.outputs.id_token }}
  ```

"""

import os
from dbt.tests.util import run_dbt
import pytest


_MODELS__MODEL_1_SQL = """
select 1 as id, 'wif_test' as source
"""


class TestSnowflakeWorkloadIdentityFederation:
    @pytest.fixture(scope="class", autouse=True)
    def dbt_profile_target(self):
        return {
            "type": "snowflake",
            "threads": 4,
            "account": os.getenv("SNOWFLAKE_TEST_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_TEST_WIF_USER"),
            "database": os.getenv("SNOWFLAKE_TEST_DATABASE"),
            "warehouse": os.getenv("SNOWFLAKE_TEST_WAREHOUSE"),
            "role": os.getenv("SNOWFLAKE_TEST_ROLE"),
            "authenticator": "workload_identity",
            "workload_identity_provider": "oidc",
            "token": os.getenv("ODIC_TOKEN"),
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_1.sql": _MODELS__MODEL_1_SQL,
        }

    def test_snowflake_wif_basic_functionality(self, project):
        """Test basic dbt functionality with WIF authentication"""
        run_dbt()
