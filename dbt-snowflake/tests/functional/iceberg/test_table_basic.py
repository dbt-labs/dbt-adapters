import pytest

from pathlib import Path

from dbt.tests.util import run_dbt, rm_file, write_file

from tests.functional.iceberg import models


class TestIcebergTableBuilds:

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "first_table.sql": models._MODEL_BASIC_TABLE_MODEL,
            "first_table_literals.sql": models._MODEL_BASIC_TABLE_LITERALS,
            "iceberg_table.sql": models._MODEL_BASIC_ICEBERG_MODEL,
            "iceberg_builtin_table.sql": models._MODEL_BASIC_ICEBERG_BUILTIN_MODEL,
            "iceberg_tableb.sql": models._MODEL_BASIC_ICEBERG_MODEL_WITH_PATH,
            "iceberg_tablec.sql": models._MODEL_BASIC_ICEBERG_MODEL_WITH_PATH_SUBPATH,
            "table_built_on_iceberg_table.sql": models._MODEL_BUILT_ON_ICEBERG_TABLE,
            "dynamic_table.sql": models._MODEL_BASIC_DYNAMIC_TABLE_MODEL,
            "dynamic_table_builtin.sql": models._MODEL_BASIC_DYNAMIC_TABLE_ICEBERG_MODEL,
            "dynamic_tableb.sql": models._MODEL_BASIC_DYNAMIC_TABLE_MODEL_WITH_PATH,
            "dynamic_tablec.sql": models._MODEL_BASIC_DYNAMIC_TABLE_MODEL_WITH_PATH_SUBPATH,
            "dynamic_tabled.sql": models._MODEL_BASIC_DYNAMIC_TABLE_MODEL_WITH_SUBPATH,
        }

    def test_iceberg_tables_build_and_can_be_referred(self, project):
        run_results = run_dbt()
        assert len(run_results) == 12


class TestIcebergTableTypeBuildsOnExistingTable:

    @pytest.mark.parametrize(
        "start_model", [models._MODEL_TABLE_BEFORE_SWAP, models._MODEL_VIEW_BEFORE_SWAP]
    )
    def test_changing_model_types(self, project, start_model):
        model_file = project.project_root / Path("models") / Path("my_model.sql")

        write_file(start_model, model_file)
        run_results = run_dbt()
        assert len(run_results) == 1

        rm_file(model_file)
        write_file(models._MODEL_TABLE_FOR_SWAP_ICEBERG, model_file)
        run_results = run_dbt()
        assert len(run_results) == 1

        rm_file(model_file)
        write_file(start_model, model_file)
        run_results = run_dbt()
        assert len(run_results) == 1
