ADVANCED_INCREMENTAL = """
{{
  config(
    materialized = "incremental",
    unique_key = "id",
    persist_docs = {"relation": true}
  )
}}

select *
from {{ ref('seed') }}

{% if is_incremental() %}

    where id > (select max(id) from {{this}})

{% endif %}
"""

COMPOUND_SORT = """
{{
  config(
    materialized = "table",
    sort = 'first_name',
    sort_type = 'compound'
  )
}}

select * from {{ ref('seed') }}
"""

DISABLED = """
{{
  config(
    materialized = "view",
    enabled = False
  )
}}

select * from {{ ref('seed') }}
"""

EMPTY = """
"""


GET_AND_REF = """
{%- do adapter.get_relation(database=target.database, schema=target.schema, identifier='materialized') -%}

select * from {{ ref('materialized') }}
"""


GET_AND_REF_UPPERCASE = """
{%- do adapter.get_relation(database=target.database, schema=target.schema, identifier='MATERIALIZED') -%}

select * from {{ ref('MATERIALIZED') }}
"""


INCREMENTAL = """
{{
  config(
    materialized = "incremental"
  )
}}

select * from {{ ref('seed') }}

{% if is_incremental() %}
    where id > (select max(id) from {{this}})
{% endif %}
"""


INTERLEAVED_SORT = """
{{
  config(
    materialized = "table",
    sort = ['first_name', 'last_name'],
    sort_type = 'interleaved'
  )
}}

select * from {{ ref('seed') }}
"""


MATERIALIZED = """
{{
  config(
    materialized = "table"
  )
}}
-- ensure that dbt_utils' relation check will work
{% set relation = ref('seed') %}
{%- if not (relation is mapping and relation.get('metadata', {}).get('type', '').endswith('Relation')) -%}
    {%- do exceptions.raise_compiler_error("Macro " ~ macro ~ " expected a Relation but received the value: " ~ relation) -%}
{%- endif -%}
-- this is a unicode character: å
select * from {{ relation }}
"""


VIEW_MODEL = """
{{
  config(
    materialized = "view"
  )
}}

select * from {{ ref('seed') }}
"""
