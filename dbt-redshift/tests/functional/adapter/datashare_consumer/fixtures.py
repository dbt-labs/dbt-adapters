import os

import pytest

# Name of a database created FROM A DATASHARE on the cluster under test, i.e. one that
# svv_redshift_databases reports with database_type = 'shared'. The connection's dbname is
# pointed directly at it, which is the configuration these tests exist to cover.
#
# Setting one up (Redshift Serverless, two namespaces):
#
#   -- producer namespace, database `dev`
#   create schema shared_sch;
#   create table shared_sch.orders (id int);
#   create datashare repro_share;
#   alter datashare repro_share add schema shared_sch;
#   alter datashare repro_share add table shared_sch.orders;
#   grant usage on datashare repro_share to namespace '<consumer-namespace-guid>';
#
#   -- consumer namespace
#   create database ds_consumer from datashare repro_share of namespace '<producer-guid>';
#
# then set REDSHIFT_TEST_DATASHARE_DBNAME=ds_consumer and point the test profile's host at
# the consumer workgroup.
REDSHIFT_TEST_DATASHARE_DBNAME = os.getenv("REDSHIFT_TEST_DATASHARE_DBNAME", "")


class DatashareConsumerMixin:
    """Connect directly to a datashare consumer database, with datasharing enabled.

    This is deliberately different from CrossDatabaseMixin, which leaves the connection on
    the default database and only retargets models via `+database`. That arrangement works;
    connecting straight at the consumer database is the one that breaks, because temporary
    relations are then invisible to every catalog view.
    """

    @pytest.fixture(scope="class")
    def profiles_config_update(self, dbt_profile_target, unique_schema):
        return {
            "test": {
                "outputs": {
                    "default": {
                        **dbt_profile_target,
                        "schema": unique_schema,
                        "dbname": REDSHIFT_TEST_DATASHARE_DBNAME,
                        "datasharing": True,
                    }
                },
                "target": "default",
            }
        }
