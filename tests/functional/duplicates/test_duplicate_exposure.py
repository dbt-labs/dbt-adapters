from dbt.tests.util import run_dbt
from dbt_common.exceptions import CompilationError
import pytest


exposure_dupes_schema_yml = """
version: 2
exposures:
  - name: something
    type: dashboard
    owner:
      email: test@example.com
  - name: something
    type: dashboard
    owner:
      email: test@example.com

"""


class TestDuplicateExposure:
    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": exposure_dupes_schema_yml}

    def test_duplicate_exposure(self, project):
        message = "dbt found two exposures with the name"
        with pytest.raises(CompilationError) as exc:
            run_dbt(["compile"])
        assert message in str(exc.value)
