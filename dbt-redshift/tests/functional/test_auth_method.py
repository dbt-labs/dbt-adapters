import os

import pytest

from dbt.adapters.redshift.connections import RedshiftConnectionMethod
from dbt.tests.util import run_dbt


MY_SEED = """
id,name
1,apple
2,banana
3,cherry
""".strip()


MY_VIEW = """
select * from {{ ref("my_seed") }}
"""


class AuthMethod:

    @pytest.fixture(scope="class")
    def seeds(self):
        yield {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class")
    def models(self):
        yield {"my_view.sql": MY_VIEW}

    def test_connection(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 1


class TestDatabaseMethod(AuthMethod):
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return {
            "type": "redshift",
            "method": RedshiftConnectionMethod.DATABASE.value,
            "host": os.getenv("REDSHIFT_TEST_HOST"),
            "port": int(os.getenv("REDSHIFT_TEST_PORT")),
            "dbname": os.getenv("REDSHIFT_TEST_DBNAME"),
            "user": os.getenv("REDSHIFT_TEST_USER"),
            "pass": os.getenv("REDSHIFT_TEST_PASS"),
            "threads": 1,
            "retries": 6,
        }


class TestIAMUserMethodProfile(AuthMethod):
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return {
            "type": "redshift",
            "method": RedshiftConnectionMethod.IAM.value,
            "cluster_id": os.getenv("REDSHIFT_TEST_CLUSTER_ID"),
            "dbname": os.getenv("REDSHIFT_TEST_DBNAME"),
            "iam_profile": os.getenv("REDSHIFT_TEST_IAM_USER_PROFILE"),
            "user": os.getenv("REDSHIFT_TEST_USER"),
            "threads": 1,
            "retries": 6,
            "host": "",  # host is a required field in dbt-core
            "port": 0,  # port is a required field in dbt-core
        }


class TestIAMUserMethodExplicit(AuthMethod):
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return {
            "type": "redshift",
            "method": RedshiftConnectionMethod.IAM.value,
            "cluster_id": os.getenv("REDSHIFT_TEST_CLUSTER_ID"),
            "dbname": os.getenv("REDSHIFT_TEST_DBNAME"),
            "access_key_id": os.getenv("REDSHIFT_TEST_IAM_USER_ACCESS_KEY_ID"),
            "secret_access_key": os.getenv("REDSHIFT_TEST_IAM_USER_SECRET_ACCESS_KEY"),
            "region": os.getenv("REDSHIFT_TEST_REGION"),
            "user": os.getenv("REDSHIFT_TEST_USER"),
            "threads": 1,
            "retries": 6,
            "host": "",  # host is a required field in dbt-core
            "port": 0,  # port is a required field in dbt-core
        }


@pytest.mark.skip(
    reason="Requires us to fix by figuring out what configuration in the repo is wrong. This is blocking other PRs from getting merged, so for now, skipping."
)
class TestIAMRoleAuthProfile(AuthMethod):
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return {
            "type": "redshift",
            "method": RedshiftConnectionMethod.IAM_ROLE.value,
            "cluster_id": os.getenv("REDSHIFT_TEST_CLUSTER_ID"),
            "dbname": os.getenv("REDSHIFT_TEST_DBNAME"),
            "iam_profile": os.getenv("REDSHIFT_TEST_IAM_ROLE_PROFILE"),
            "threads": 1,
            "retries": 6,
            "host": "",  # host is a required field in dbt-core
            "port": 0,  # port is a required field in dbt-core
        }


@pytest.mark.skip(
    reason="We need to cut over to new adapters team AWS account which has infra to support this as an automated test. This will include a GHA step that renders a refresh token and loading secrets into Github secrets for the <> delimited placeholder values below"
)
class TestIamIdcAuthProfileOktaIdp(AuthMethod):
    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return {
            "type": "redshift",
            "method": "oauth_token_identity_center",
            "host": os.getenv("REDSHIFT_TEST_HOST"),
            "port": 5439,
            "dbname": "dev",
            "threads": 1,
            "token_endpoint": {
                "type": "okta",
                "request_url": "https://<subdomain>.oktapreview.com/oauth2/default/v1/token",
                "idp_auth_credentials": "<base64 creds>",
                "request_data": "grant_type=refresh_token&redirect_uri=<encoded redirect uri>&refresh_token=<a refresh token>",
            },
        }
