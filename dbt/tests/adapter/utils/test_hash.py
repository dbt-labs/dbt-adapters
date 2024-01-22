import pytest

from dbt.tests.adapter.utils import base_utils, fixture_hash


class BaseHash(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_hash.csv": fixture_hash.seeds__data_hash_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_hash.yml": fixture_hash.models__test_hash_yml,
            "test_hash.sql": self.interpolate_macro_namespace(
                fixture_hash.models__test_hash_sql, "hash"
            ),
        }


class TestHash(BaseHash):
    pass
