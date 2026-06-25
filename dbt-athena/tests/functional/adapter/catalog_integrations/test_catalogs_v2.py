"""Functional tests for catalogs.yml v2 on Athena.

Requires use_catalogs_v2 flag support in dbt-core.
"""

import re

import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture, write_config_file

# Skip if installed dbt-core doesn't support use_catalogs_v2 yet.
try:
    from dbt.contracts.project import ProjectFlags as _PF

    _has_catalogs_v2 = hasattr(_PF, "use_catalogs_v2")
except ImportError:
    _has_catalogs_v2 = False

pytestmark = pytest.mark.skipif(
    not _has_catalogs_v2,
    reason="dbt-core does not support use_catalogs_v2 yet",
)

get_detailed_table_type_sql = """
{% macro get_detailed_table_type(schema) %}
    {% if execute %}
    {% set relation = api.Relation.create(database="awsdatacatalog", schema=schema) %}
    {% set schema_tables = adapter.list_relations_without_caching(schema_relation = relation) %}
    {% for rel in schema_tables %}
        {% do log('Detailed Table Type: ' ~ rel.detailed_table_type, info=True) %}
    {% endfor %}
    {% endif %}
{% endmacro %}
"""

# No table_type config — the catalog drives the table format.
MODEL__GLUE_ICEBERG = """
{{ config(materialized='table', catalog_name='athena_glue_v2') }}
select 1 as id, 'iceberg' as name
"""

# Catalog says iceberg, but an explicit table_type='hive' must win (backward compat).
MODEL__PRECEDENCE = """
{{ config(materialized='table', catalog_name='athena_glue_v2', table_type='hive') }}
select 1 as id, 'hive' as name
"""


class TestAthenaV2GlueCatalog:
    """End-to-end: v2 glue catalog -> bridge -> GlueCatalogIntegration -> Iceberg DDL."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_catalogs_v2": True}}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"get_detailed_table_type.sql": get_detailed_table_type_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {"glue_iceberg.sql": MODEL__GLUE_ICEBERG}

    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "athena_glue_v2",
                    "type": "glue",
                    "table_format": "iceberg",
                    "config": {"athena": {"file_format": "parquet"}},
                }
            ]
        }

    def test_glue_v2_creates_iceberg_table(self, project, catalogs):
        write_config_file(catalogs, project.project_root, "catalogs.yml")
        run_results = run_dbt(["run"])
        assert len(run_results) == 1

        args_str = f'{{"schema": "{project.test_schema}"}}'
        _, stdout = run_dbt_and_capture(
            ["run-operation", "get_detailed_table_type", "--args", args_str]
        )
        table_type = re.search(r"Detailed Table Type: (\w+)", stdout).group(1)
        assert table_type == "ICEBERG"


class TestAthenaV2CatalogPrecedence:
    """An explicit table_type config overrides the catalog's table_format."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_catalogs_v2": True}}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"get_detailed_table_type.sql": get_detailed_table_type_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {"precedence.sql": MODEL__PRECEDENCE}

    @pytest.fixture
    def catalogs(self):
        return {
            "catalogs": [
                {
                    "name": "athena_glue_v2",
                    "type": "glue",
                    "table_format": "iceberg",
                    "config": {"athena": {"file_format": "parquet"}},
                }
            ]
        }

    def test_explicit_table_type_overrides_catalog(self, project, catalogs):
        write_config_file(catalogs, project.project_root, "catalogs.yml")
        run_results = run_dbt(["run"])
        assert len(run_results) == 1

        args_str = f'{{"schema": "{project.test_schema}"}}'
        _, stdout = run_dbt_and_capture(
            ["run-operation", "get_detailed_table_type", "--args", args_str]
        )
        # Catalog is iceberg, but table_type='hive' wins -> not an Iceberg table.
        # Hive tables have no `table_type` Glue parameter, so detailed_table_type is
        # empty (\\w* matches that), whereas an Iceberg table would report 'ICEBERG'.
        match = re.search(r"Detailed Table Type: (\w*)", stdout)
        assert match is not None
        assert match.group(1) != "ICEBERG"
