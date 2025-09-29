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

    def test_udfs(self, project):
        result = run_dbt(["build", "--debug"])

        assert len(result.results) == 1
        node_result = result.results[0]
        assert node_result.status == RunStatus.Success
        node = node_result.node
        assert isinstance(node, FunctionNode)
        assert node_result.node.name == "price_for_xlarge"

        # TODO: use `function` instead of `ref`
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100)"])
        assert len(result.results) == 1
        # The result should have an agate table with one row and one column (and thus only one value, which is our inline selection)
        select_value = int(result.results[0].agate_table.rows[0].values()[0])
        assert select_value == 200  # the UDF should return 2x the input value (100 * 2 = 200)
