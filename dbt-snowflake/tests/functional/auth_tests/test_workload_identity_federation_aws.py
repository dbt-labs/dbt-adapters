"""
Functional tests for Snowflake Workload Identity Federation (WIF) with AWS authentication.
Prerequisites for testing WIF with AWS:
1. **AWS IAM Configuration:**
   Create an IAM role that can be assumed by the EC2 service. An example trust policy below:
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
2. **EC2 Instance:**
    Launch an EC2 instance with the IAM role attached as an instance profile.
    Connect to the EC2 instance and


3. **Snowflake User Configuration:**
   Create a service user in Snowflake with WIF enabled:
   ```sql
   CREATE USER <username>
     WORKLOAD_IDENTITY = (
       TYPE = AWS
       ARN = '<amazon_iam_role_arn>'
     )
     TYPE = SERVICE
     DEFAULT_ROLE = <role>;
   ```
   Replace `<username>` with your desired username and `<amazon_iam_role_arn>`
   with the ARN of your AWS IAM role.
4. **AWS Environment:**
   This test must run from within the configured EC2 environment.
   Connect to the EC2 instance using SSH or similar.
   Clone this repository, run the setup, and execute this test e.g.
   `hatch run pytest tests/functional/auth_tests/test_workload_identity_federation_aws.py::test_snowflake_wif_basic_functionality`
5. **Environment Variables:**
   Set the following environment variables for testing:
   - SNOWFLAKE_TEST_ACCOUNT: Your Snowflake account identifier
   - SNOWFLAKE_TEST_WIF_USER: The Snowflake service user created for WIF
   - SNOWFLAKE_TEST_DATABASE: Test database name
   - SNOWFLAKE_TEST_WAREHOUSE: Test warehouse name
   - SNOWFLAKE_TEST_ROLE: Snowflake Role for the user (optional)
   - SNOWFLAKE_TEST_SCHEMA: Schema for testing (optional, defaults to schema in profile)
Note: WIF authentication relies on being in the AWS environment, so these tests can't be run locally or in the CI/CD pipeline.
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
