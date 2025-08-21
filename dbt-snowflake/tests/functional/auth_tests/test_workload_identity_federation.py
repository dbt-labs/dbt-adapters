"""
Functional tests for Snowflake Workload Identity Federation (WIF) with AWS authentication.

Prerequisites for testing WIF with AWS:

1. **AWS IAM Configuration:**
   Create an IAM role with the necessary permissions:
   ```
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Principal": {
           "Service": "ec2.amazonaws.com"  // or your specific service
         },
         "Action": "sts:AssumeRole"
       }
     ]
   }
   ```

2. **Snowflake User Configuration:**
   Create a service user in Snowflake with WIF enabled:
   ```sql
   CREATE USER <username>
     WORKLOAD_IDENTITY = (
       TYPE = AWS
       ARN = '<amazon_resource_identifier>'
     )
     TYPE = SERVICE
     DEFAULT_ROLE = <role>;
   ```
   Replace `<username>` with your desired username and `<amazon_resource_identifier>`
   with the ARN of your AWS IAM role.

3. **Environment Variables:**
   Set the following environment variables for testing:
   - SNOWFLAKE_TEST_WIF_ACCOUNT: Your Snowflake account identifier
   - SNOWFLAKE_TEST_WIF_USER: The service user created for WIF
   - SNOWFLAKE_TEST_WIF_DATABASE: Test database name
   - SNOWFLAKE_TEST_WIF_WAREHOUSE: Test warehouse name
   - SNOWFLAKE_TEST_WIF_ROLE: Role for the user (optional)
   - SNOWFLAKE_TEST_WIF_SCHEMA: Schema for testing (optional, defaults to schema in profile)

4. **AWS Environment:**
   Ensure your test environment has the necessary AWS credentials configured:
   - Either run from an EC2 instance with the appropriate IAM role attached
   - Or configure AWS credentials that can assume the IAM role
   - The role ARN should match what's configured in the Snowflake user's WORKLOAD_IDENTITY

Note: WIF authentication relies on the AWS environment to provide the necessary
credentials automatically. The test will use the AWS SDK to obtain temporary
credentials which are then used to authenticate with Snowflake.
"""

import os
from dbt.tests.util import check_relations_equal, run_dbt
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
            "authenticator": "workload_identity",
            "workload_identity_provider": "aws",
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_1.sql": _MODELS__MODEL_1_SQL,
        }

    def test_snowflake_wif_basic_functionality(self, project):
        """Test basic dbt functionality with WIF authentication"""
        run_dbt()
