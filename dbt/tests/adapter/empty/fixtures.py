model_input_sql = """
select 1 as id
"""

ephemeral_model_input_sql = """
{{ config(materialized='ephemeral') }}
select 2 as id
"""

raw_source_csv = """id
3
"""


model_sql = """
select *
from {{ ref('model_input') }}
union all
select *
from {{ ref('ephemeral_model_input') }}
union all
select *
from {{ source('seed_sources', 'raw_source') }}
"""


schema_sources_yml = """
sources:
  - name: seed_sources
    schema: "{{ target.schema }}"
    tables:
      - name: raw_source
"""
