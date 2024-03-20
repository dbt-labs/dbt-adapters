import pytest

from dbt.tests.adapter.utils import base_utils, fixture_dateadd


class BaseDateAdd(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test",
            # this is only needed for BigQuery, right?
            # no harm having it here until/unless there's an adapter that doesn't support the 'timestamp' type
            "seeds": {
                "test": {
                    "data_dateadd": {
                        "+column_types": {
                            "from_time": "timestamp",
                            "result": "timestamp",
                        },
                    },
                },
            },
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_dateadd.csv": fixture_dateadd.seeds__data_dateadd_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_dateadd.yml": fixture_dateadd.models__test_dateadd_yml,
            "test_dateadd.sql": self.interpolate_macro_namespace(
                fixture_dateadd.models__test_dateadd_sql, "dateadd"
            ),
        }


class TestDateAdd(BaseDateAdd):
    pass
