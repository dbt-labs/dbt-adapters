import pytest
from dbt.tests.adapter.functions.test_udfs import UDFsBasic
from tests.functional.functions import files


class TestBigqueryUDFs(UDFsBasic):

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": files.MY_UDF_SQL,
            "price_for_xlarge.yml": files.MY_UDF_YML,
        }

    pass
