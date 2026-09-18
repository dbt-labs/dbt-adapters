from dbt.contracts.results import CatalogArtifact
from dbt.tests.util import run_dbt
import pytest

from tests.functional.adapter.catalog_tests import files


class TestCatalogNumericColumnTypes:
    """
    Regression coverage for dbt-labs/dbt-adapters#2114.

    Snowflake's information_schema.columns.data_type is bare NUMBER for every
    fixed-point numeric column. Catalog generation must compose precision and
    scale into column_type so downstream consumers can round-trip decimals.
    """

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {"my_numeric_table.sql": files.MY_NUMERIC_TABLE}

    @pytest.fixture(scope="class", autouse=True)
    def docs(self, project):
        run_dbt(["run"])
        yield run_dbt(["docs", "generate"])

    def test_numeric_columns_include_precision_and_scale(self, docs: CatalogArtifact):
        node = docs.nodes["model.test.my_numeric_table"]

        assert node.columns["RETAIL_PRICE"].type == "NUMBER(12,2)"
        assert node.columns["WHOLE_NUMBER"].type == "NUMBER(38,0)"
        # Non-numeric types keep the information_schema data_type value.
        assert node.columns["LABEL"].type == "TEXT"
