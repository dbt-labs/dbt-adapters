import pytest

from dbt.tests.util import run_dbt
import fixtures


class BaseColumnTypes:
    @pytest.fixture(scope="class")
    def macros(self):
        return {"test_is_type.sql": fixtures.macro_test_is_type_sql}

    def run_and_test(self):
        results = run_dbt(["run"])
        assert len(results) == 1
        results = run_dbt(["test"])
        assert len(results) == 1
