import pytest

from dbt.tests.adapter.utils import base_utils, fixture_listagg


class BaseListagg(base_utils.BaseUtils):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "data_listagg.csv": fixture_listagg.seeds__data_listagg_csv,
            "data_listagg_output.csv": fixture_listagg.seeds__data_listagg_output_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_listagg.yml": fixture_listagg.models__test_listagg_yml,
            "test_listagg.sql": self.interpolate_macro_namespace(
                fixture_listagg.models__test_listagg_sql, "listagg"
            ),
        }


class TestListagg(BaseListagg):
    pass
