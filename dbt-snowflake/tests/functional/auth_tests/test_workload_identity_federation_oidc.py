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

  # TODO

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
