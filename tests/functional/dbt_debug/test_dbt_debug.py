import pytest
import os
import re
import yaml

from tests.functional.utils import run_dbt, run_dbt_and_capture

MODELS__MODEL_SQL = """
seled 1 as id
"""


class BaseDebug:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": MODELS__MODEL_SQL}

    @pytest.fixture(autouse=True)
    def capsys(self, capsys):
        self.capsys = capsys

    def assertGotValue(self, linepat, result):
        found = False
        output = self.capsys.readouterr().out
        for line in output.split("\n"):
            if linepat.match(line):
                found = True
                assert result in line
        if not found:
            with pytest.raises(Exception) as exc:
                msg = f"linepat {linepat} not found in stdout: {output}"
                assert msg in str(exc.value)

    def check_project(self, splitout, msg="ERROR invalid"):
        for line in splitout:
            if line.strip().startswith("dbt_project.yml file"):
                assert msg in line
            elif line.strip().startswith("profiles.yml file"):
                assert "ERROR invalid" not in line


class BaseDebugProfileVariable(BaseDebug):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"config-version": 2, "profile": '{{ "te" ~ "st" }}'}


class TestDebugPostgres(BaseDebug):
    def test_ok(self, project):
        result, log = run_dbt_and_capture(["debug"])
        assert "ERROR" not in log


class TestDebugProfileVariablePostgres(BaseDebugProfileVariable):
    pass
