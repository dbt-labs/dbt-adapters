import json

from dbt_common.exceptions import DbtRuntimeError
import pytest

from dbt.tests.__about__ import version as PACKAGE_VERSION
from dbt.tests.util import run_dbt_and_capture
import fixtures


class DefaultQueryComments:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "x.sql": fixtures.MODELS__X_SQL,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "macro.sql": fixtures.MACROS__MACRO_SQL,
        }

    def run_get_json(self, expect_pass=True):
        res, raw_logs = run_dbt_and_capture(
            ["--debug", "--log-format=json", "run"], expect_pass=expect_pass
        )

        # empty lists evaluate as False
        assert len(res) > 0
        return raw_logs


class QueryComments(DefaultQueryComments):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"query-comment": "dbt\nrules!\n"}

    def test_matches_comment(self, project):
        logs = self.run_get_json()
        assert r"/* dbt\nrules! */\n" in logs


class MacroQueryComments(DefaultQueryComments):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"query-comment": "{{ query_header_no_args() }}"}

    def test_matches_comment(self, project):
        logs = self.run_get_json()
        assert r"/* dbt macros\nare pretty cool */\n" in logs


class MacroArgsQueryComments(DefaultQueryComments):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"query-comment": "{{ return(ordered_to_json(query_header_args(target.name))) }}"}

    def test_matches_comment(self, project):
        logs = self.run_get_json()
        expected_dct = {
            "app": "dbt++",
            "dbt_version": PACKAGE_VERSION,
            "macro_version": "0.1.0",
            "message": f"blah: {project.adapter.config.target_name}",
        }
        expected = r"/* {} */\n".format(json.dumps(expected_dct, sort_keys=True)).replace(
            '"', r"\""
        )
        assert expected in logs


class MacroInvalidQueryComments(DefaultQueryComments):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"query-comment": "{{ invalid_query_header() }}"}

    def test_run_assert_comments(self, project):
        with pytest.raises(DbtRuntimeError):
            self.run_get_json(expect_pass=False)


class NullQueryComments(DefaultQueryComments):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"query-comment": None}

    def test_matches_comment(self, project):
        logs = self.run_get_json()
        assert "/*" not in logs or "*/" not in logs


class EmptyQueryComments(DefaultQueryComments):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"query-comment": ""}

    def test_matches_comment(self, project):
        logs = self.run_get_json()
        assert "/*" not in logs or "*/" not in logs
