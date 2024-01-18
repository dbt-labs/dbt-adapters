import pytest

from dbt.tests.util import run_dbt
import models


class BasePySparkTests:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "pandas_df.py": models.PANDAS_MODEL,
            "pyspark_df.py": models.PYSPARK_MODEL,
            "pandas_on_spark_df.py": models.PANDAS_ON_SPARK_MODEL,
            "koalas_df.py": models.KOALAS_MODEL,
        }

    def test_different_dataframes(self, project):
        # test
        results = run_dbt(["run"])
        assert len(results) == 4
