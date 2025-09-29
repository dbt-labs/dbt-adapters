import pytest
from dbt.tests.adapter.functions.files import MY_UDF_YML
from dbt.tests.adapter.functions.test_udfs import UDFsBasic
from tests.functional.functions.files import MY_UDF_SQL


class TestSnowflakeUDFs(UDFsBasic):

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": MY_UDF_SQL,
            "price_for_xlarge.yml": MY_UDF_YML,
        }

    pass
