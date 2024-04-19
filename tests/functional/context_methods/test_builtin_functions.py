import json

from dbt.tests.util import write_file
from dbt_common.exceptions import CompilationError
import pytest

from tests.functional.utils import run_dbt, run_dbt_and_capture


macros__validate_set_sql = """
{% macro validate_set() %}
    {% set set_result = set([1, 2, 2, 3, 'foo', False]) %}
    {{ log("set_result: " ~ set_result) }}
    {% set set_strict_result = set_strict([1, 2, 2, 3, 'foo', False]) %}
    {{ log("set_strict_result: " ~ set_strict_result) }}
{% endmacro %}
"""

macros__validate_zip_sql = """
{% macro validate_zip() %}
    {% set list_a = [1, 2] %}
    {% set list_b = ['foo', 'bar'] %}
    {% set zip_result = zip(list_a, list_b) | list %}
    {{ log("zip_result: " ~ zip_result) }}
    {% set zip_strict_result = zip_strict(list_a, list_b) | list %}
    {{ log("zip_strict_result: " ~ zip_strict_result) }}
{% endmacro %}
"""

macros__validate_invocation_sql = """
{% macro validate_invocation(my_variable) %}
    -- check a specific value
    {{ log("use_colors: "~ invocation_args_dict['use_colors']) }}
    -- whole dictionary (as string)
    {{ log("invocation_result: "~ invocation_args_dict) }}
{% endmacro %}
"""

macros__validate_dbt_metadata_envs_sql = """
{% macro validate_dbt_metadata_envs() %}
    {{ log("dbt_metadata_envs_result:"~ dbt_metadata_envs) }}
{% endmacro %}
"""

models__set_exception_sql = """
{% set set_strict_result = set_strict(1) %}
"""

models__zip_exception_sql = """
{% set zip_strict_result = zip_strict(1) %}
"""


def parse_json_logs(json_log_output):
    parsed_logs = []
    for line in json_log_output.split("\n"):
        try:
            log = json.loads(line)
        except ValueError:
            continue

        parsed_logs.append(log)

    return parsed_logs


def find_result_in_parsed_logs(parsed_logs, result_name):
    return next(
        (
            item["data"]["msg"]
            for item in parsed_logs
            if result_name in item["data"].get("msg", "msg")
        ),
        False,
    )


class TestContextBuiltins:
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "validate_set.sql": macros__validate_set_sql,
            "validate_zip.sql": macros__validate_zip_sql,
            "validate_invocation.sql": macros__validate_invocation_sql,
            "validate_dbt_metadata_envs.sql": macros__validate_dbt_metadata_envs_sql,
        }

    def test_builtin_set_function(self, project):
        _, log_output = run_dbt_and_capture(["--debug", "run-operation", "validate_set"])

        # The order of the set isn't guaranteed so we can't check for the actual set in the logs
        assert "set_result: " in log_output
        assert "False" in log_output
        assert "set_strict_result: " in log_output

    def test_builtin_zip_function(self, project):
        _, log_output = run_dbt_and_capture(["--debug", "run-operation", "validate_zip"])

        expected_zip = [(1, "foo"), (2, "bar")]
        assert f"zip_result: {expected_zip}" in log_output
        assert f"zip_strict_result: {expected_zip}" in log_output

    def test_builtin_invocation_args_dict_function(self, project):
        _, log_output = run_dbt_and_capture(
            [
                "--debug",
                "--log-format=json",
                "run-operation",
                "validate_invocation",
                "--args",
                "{my_variable: test_variable}",
            ]
        )

        parsed_logs = parse_json_logs(log_output)
        use_colors = result = find_result_in_parsed_logs(parsed_logs, "use_colors")
        assert use_colors == "use_colors: True"
        invocation_dict = find_result_in_parsed_logs(parsed_logs, "invocation_result")
        assert result
        # The result should include a dictionary of all flags with values that aren't None
        expected = (
            "'send_anonymous_usage_stats': False",
            "'quiet': False",
            "'print': True",
            "'cache_selected_only': False",
            "'macro': 'validate_invocation'",
            "'args': {'my_variable': 'test_variable'}",
            "'which': 'run-operation'",
            "'indirect_selection': 'eager'",
        )
        assert all(element in invocation_dict for element in expected)


class TestContextBuiltinExceptions:
    # Assert compilation errors are raised with _strict equivalents
    def test_builtin_function_exception(self, project):
        write_file(models__set_exception_sql, project.project_root, "models", "raise.sql")
        with pytest.raises(CompilationError):
            run_dbt(["compile"])

        write_file(models__zip_exception_sql, project.project_root, "models", "raise.sql")
        with pytest.raises(CompilationError):
            run_dbt(["compile"])
