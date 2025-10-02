from dbt.artifacts.schemas.results import RunStatus
from dbt.contracts.graph.nodes import FunctionNode
from dbt.tests.util import run_dbt
import pytest
from dbt.tests.adapter.functions.files import MY_UDF_YML
from dbt.tests.adapter.functions.test_udfs import UDFsBasic
from tests.functional.functions.files import MY_UDF_SQL


class TestRedshiftUDFs(UDFsBasic):

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": MY_UDF_SQL,
            "price_for_xlarge.yml": MY_UDF_YML,
        }
