import os

import pytest


REDSHIFT_TEST_DBNAME_W_HYPHEN = os.getenv("REDSHIFT_TEST_DBNAME_W_HYPHEN", "")


class HyphenatedDatabaseMixin:
    """Shared fixtures for hyphenated-database tests.

    Routes models to the hyphenated database and enables datasharing so that
    the SHOW APIs code path is exercised — the path where identifier quoting
    is required for names that contain a hyphen.
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+database": REDSHIFT_TEST_DBNAME_W_HYPHEN,
            }
        }

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target):
        outputs = {"default": dbt_profile_target}
        outputs["default"]["datasharing"] = True
        return outputs
