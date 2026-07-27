from dbt.tests.util import run_dbt
import pytest


# A saved query with an export carries a typed ``ExportConfig`` on the export node
# (not a dict). Parsing it runs the adapter's ``generate_database_name`` ->
# ``build_catalog_relation`` -> ``parse_model.catalog_name`` path, which must not
# raise ``AttributeError: 'ExportConfig' object has no attribute 'get'``.
MODEL__ORDERS = """
select 1 as id, 10 as amount, cast('2020-01-01' as datetime) as ordered_at
"""

# The semantic layer requires a time spine model with DAY (or finer) granularity.
MODEL__TIME_SPINE = """
{{ config(materialized='table') }}
select cast('2020-01-01' as date) as date_day
"""

TIME_SPINE__YML = """
models:
  - name: metricflow_time_spine
    time_spine:
      standard_granularity_column: date_day
    columns:
      - name: date_day
        granularity: day
"""

SEMANTIC_MODELS__YML = """
semantic_models:
  - name: orders
    model: ref('orders')
    defaults:
      agg_time_dimension: ordered_at
    entities:
      - name: id
        type: primary
    dimensions:
      - name: ordered_at
        type: time
        type_params:
          time_granularity: day
    measures:
      - name: total_amount
        agg: sum
        expr: amount
"""

METRICS__YML = """
metrics:
  - name: total_amount
    label: Total Amount
    type: simple
    type_params:
      measure: total_amount
"""

SAVED_QUERIES__YML = """
saved_queries:
  - name: my_saved_query
    query_params:
      metrics:
        - total_amount
      group_by:
        - "Dimension('orders__ordered_at')"
    exports:
      - name: my_export
        config:
          export_as: table
"""


class TestSavedQueryExportParse:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "orders.sql": MODEL__ORDERS,
            "metricflow_time_spine.sql": MODEL__TIME_SPINE,
            "time_spine.yml": TIME_SPINE__YML,
            "semantic_models.yml": SEMANTIC_MODELS__YML,
            "metrics.yml": METRICS__YML,
            "saved_queries.yml": SAVED_QUERIES__YML,
        }

    def test_parse_saved_query_export(self, project):
        # Regression: this used to raise AttributeError on the export's ExportConfig.
        run_dbt(["parse"])
