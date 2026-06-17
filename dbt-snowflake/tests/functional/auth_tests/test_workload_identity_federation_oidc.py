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
      -- SUBJECT is the authorization boundary: any GitHub workflow whose OIDC `sub`
      -- claim matches this string can authenticate as this Snowflake user.
      SUBJECT = 'repo:<REPO_OWNER>/<REPO>:ref:refs/heads/main',
      -- Use an ACCOUNT-SCOPED audience. The bare 'snowflakecomputing.com' default is
      -- shared across every Snowflake account, so a leaked token is replayable against
      -- any other tenant that also left the default.
      OIDC_AUDIENCE_LIST = ('https://<orgname>-<account_name>.snowflakecomputing.com')
    );
  ```

  Scoping the trust (read this). The Snowflake-side trust -- not dbt -- is the real
  authorization boundary; dbt only fetches a token. Mis-scoping SUBJECT is a single point
  of total compromise:
    - DO pin to a specific branch (`:ref:refs/heads/main`) or, better, a protected GitHub
      Environment (`:environment:prod`, gated by required reviewers / branch rules).
    - DON'T trust `:pull_request` subjects -- anyone who can open a PR (incl. forks on a
      public repo) could then authenticate as this user.
    - DON'T use org-wide / any-branch / wildcard subjects.
    - Back the service user with a least-privilege role. To revoke, run
      `ALTER USER <username> UNSET WORKLOAD_IDENTITY` (or re-point SUBJECT) -- there is no
      GitHub-side revocation for an already-minted token.

2. **GitHub Actions workflow that mints the OIDC token and runs the test**

  ```yaml
  name: Run Snowflake Workload Identity Federation (WIF) Test
  on:
    workflow_dispatch:
    push:
      branches: [ main ]

  # Grant id-token: write at the JOB level only (below), never at workflow level --
  # workflow-level would let every job in the workflow mint identity tokens.
  permissions: {}

  jobs:
    run-snowflake:
      runs-on: ubuntu-latest
      permissions:
        contents: read
        id-token: write
      env:
        SNOWFLAKE_TEST_ACCOUNT: <ACCOUNT_ID>
        SNOWFLAKE_TEST_DATABASE: <DB_NAME>
        SNOWFLAKE_TEST_WAREHOUSE: <WH_NAME>
        SNOWFLAKE_TEST_ROLE: <ROLE_NAME>
        SNOWFLAKE_TEST_WIF_USER: <USERNAME>
        # Must match OIDC_AUDIENCE_LIST on the Snowflake user (account-scoped).
        SNOWFLAKE_TEST_WIF_AUDIENCE: https://<orgname>-<account_name>.snowflakecomputing.com
      steps:
        # Pin third-party actions to a full commit SHA in production (tags shown for brevity).
        - uses: actions/checkout@v4
        - uses: actions/setup-python@v5
          with:
            python-version: '3.11'
        - uses: pypa/hatch@install
        - run: hatch run setup
          working-directory: ./dbt-snowflake
        # GitHub injects ACTIONS_ID_TOKEN_REQUEST_URL/TOKEN into this job (id-token: write);
        # both the static helper and dbt's dynamic provider mint from those automatically.
        - run: hatch run python -m pytest tests/functional/auth_tests/test_workload_identity_federation_oidc.py
          working-directory: ./dbt-snowflake
  ```

  Do NOT run this on `pull_request_target` / `workflow_run` jobs that execute untrusted
  (fork) code: such a job can mint a token for your Snowflake audience from attacker code.

"""

import os
from time import sleep

import requests
from dbt.tests.util import run_dbt
import pytest


# Audience for the minted OIDC token. Use an account-scoped value (set via env) so it
# matches the Snowflake user's OIDC_AUDIENCE_LIST; the bare default is shared across all
# Snowflake accounts and is only a convenience fallback for local testing.
_WIF_AUDIENCE = os.getenv("SNOWFLAKE_TEST_WIF_AUDIENCE", "snowflakecomputing.com")


_MODELS__MODEL_1_SQL = """
select 1 as id, 'wif_test' as source
"""


def _mint_github_oidc_token():
    """Mint a fresh GitHub OIDC token for Snowflake WIF authentication.

    GitHub OIDC tokens have a ~5 min TTL; the integration test suite runs
    longer, so a token minted at workflow start would expire before this
    test executes. Mint right before the connection instead.

    """
    url = os.getenv("ACTIONS_ID_TOKEN_REQUEST_URL")
    bearer = os.getenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN")
    if not (url and bearer):
        return os.getenv("OIDC_TOKEN")

    headers = {"Authorization": f"bearer {bearer}"}
    target = f"{url}&audience={_WIF_AUDIENCE}"
    for _ in range(5):
        result = requests.get(target, headers=headers)
        try:
            return result.json()["value"]
        except (ValueError, KeyError):
            sleep(0.1)
    raise RuntimeError(
        f"Failed to mint GitHub OIDC token after retries (status={result.status_code})"
    )


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
            "token": _mint_github_oidc_token(),
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_1.sql": _MODELS__MODEL_1_SQL,
        }

    def test_snowflake_wif_basic_functionality(self, project):
        """Test basic dbt functionality with WIF authentication"""
        run_dbt()


_HAS_GITHUB_ACTIONS_OIDC = bool(
    os.getenv("ACTIONS_ID_TOKEN_REQUEST_URL") and os.getenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN")
)


@pytest.mark.skipif(
    not _HAS_GITHUB_ACTIONS_OIDC,
    reason="requires the GitHub Actions OIDC runtime "
    "(ACTIONS_ID_TOKEN_REQUEST_URL / ACTIONS_ID_TOKEN_REQUEST_TOKEN)",
)
class TestSnowflakeWorkloadIdentityFederationDynamicToken:
    """Dynamic-token WIF: dbt mints a fresh OIDC token before each connection open.

    Unlike the static-token class above, no `token` is configured -- the
    `github_actions` provider mints one at connection-open time.
    """

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
            "workload_identity_token": {
                "provider": "github_actions",
                "audience": _WIF_AUDIENCE,
            },
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_1.sql": _MODELS__MODEL_1_SQL,
        }

    def test_snowflake_wif_dynamic_token(self, project):
        """dbt mints a fresh GitHub Actions OIDC token per connection open."""
        run_dbt()
