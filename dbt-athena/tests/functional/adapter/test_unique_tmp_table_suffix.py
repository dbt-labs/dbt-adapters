import re

import pytest
from tests.functional.adapter.utils.parse_dbt_run_output import (
    extract_create_statement_table_names,
    extract_running_create_statements,
)

from dbt.adapters.athena.impl import AthenaAdapter
from dbt.contracts.results import RunStatus
from dbt.tests.util import run_dbt

models__unique_tmp_table_suffix_sql = """
{{ config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partitioned_by=['date_column'],
        unique_tmp_table_suffix=True
    )
}}
select
    random() as rnd,
    cast(date_column as date) as date_column
from (
    values (
        sequence(
            from_iso8601_date('{{ var('start_date') }}'),
            from_iso8601_date('{{ var('end_date') }}'),
            interval '1' day
        )
    )
) as t1(date_array)
cross join unnest(date_array) as t2(date_column)
"""


class TestUniqueTmpTableSuffix:
    @pytest.fixture(scope="class")
    def models(self):
        return {"unique_tmp_table_suffix.sql": models__unique_tmp_table_suffix_sql}

    def test__unique_tmp_table_suffix(self, project, monkeypatch, capsys):
        relation_name = "unique_tmp_table_suffix"
        model_run_result_row_count_query = (
            f"select count(*) as records from {project.test_schema}.{relation_name}"
        )
        expected_unique_table_name_re = (
            r"unique_tmp_table_suffix__dbt_tmp_"
            r"[0-9a-fA-F]{8}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{12}"
        )

        first_model_run = run_dbt(
            [
                "run",
                "--select",
                relation_name,
                "--vars",
                '{"start_date": "2024-01-01", "end_date": "2024-01-01"}',
                "--log-level",
                "debug",
                "--log-format",
                "json",
            ]
        )

        first_model_run_result = first_model_run.results[0]

        assert first_model_run_result.status == RunStatus.Success

        out, _ = capsys.readouterr()
        athena_running_create_statements = extract_running_create_statements(out, relation_name)

        assert len(athena_running_create_statements) == 1

        first_model_run_result_table_name = extract_create_statement_table_names(
            athena_running_create_statements[0]
        )[0]

        # Run statements logged output should not contain unique table suffix after first run
        assert not bool(
            re.search(expected_unique_table_name_re, first_model_run_result_table_name)
        )

        assert project.run_sql(model_run_result_row_count_query, fetch="all")[0][0] == 1

        incremental_model_run = run_dbt(
            [
                "run",
                "--select",
                relation_name,
                "--vars",
                '{"start_date": "2024-01-02", "end_date": "2024-01-02"}',
                "--log-level",
                "debug",
                "--log-format",
                "json",
            ]
        )

        incremental_model_run_result = incremental_model_run.results[0]

        assert incremental_model_run_result.status == RunStatus.Success

        out, _ = capsys.readouterr()
        athena_running_create_statements = extract_running_create_statements(out, relation_name)

        assert len(athena_running_create_statements) == 1

        incremental_model_run_result_table_name = extract_create_statement_table_names(
            athena_running_create_statements[0]
        )[0]

        # Run statements logged for subsequent incremental model runs should use unique table suffix
        assert bool(
            re.search(expected_unique_table_name_re, incremental_model_run_result_table_name)
        )

        assert first_model_run_result_table_name != incremental_model_run_result_table_name

        # Write 4 partitions with a monkeypatched expression limit to force chunking.
        # Each "(date_column='2024-01-0X')" is ~27 chars, so 2 fit per chunk (58 < 60),
        # requiring 2 Glue GetPartitions API calls for 4 partitions.
        monkeypatch.setattr(AthenaAdapter, "GET_PARTITIONS_API_EXPRESSION_MAX_LENGTH", 60)

        incremental_model_run_2 = run_dbt(
            [
                "run",
                "--select",
                relation_name,
                "--vars",
                '{"start_date": "2024-01-01", "end_date": "2024-01-04"}',
                "--log-level",
                "debug",
                "--log-format",
                "json",
            ]
        )

        incremental_model_run_result = incremental_model_run_2.results[0]

        assert incremental_model_run_result.status == RunStatus.Success

        out, _ = capsys.readouterr()
        athena_running_create_statements = extract_running_create_statements(out, relation_name)

        incremental_model_run_result_table_name_2 = extract_create_statement_table_names(
            athena_running_create_statements[0]
        )[0]

        assert incremental_model_run_result_table_name != incremental_model_run_result_table_name_2
        assert first_model_run_result_table_name != incremental_model_run_result_table_name_2
        assert project.run_sql(model_run_result_row_count_query, fetch="all")[0][0] == 4
