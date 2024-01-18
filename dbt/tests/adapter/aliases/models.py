FOO_ALIAS_SQL = """

{{
    config(
        alias='foo',
        materialized='table'
    )
}}

select {{ string_literal(this.name) }} as tablename

"""

ALIAS_IN_PROJECT_SQL = """

select {{ string_literal(this.name) }} as tablename

"""

ALIAS_IN_PROJECT_WITH_OVERRIDE_SQL = """

{{ config(alias='override_alias') }}

select {{ string_literal(this.name) }} as tablename

"""

REF_FOO_ALIAS_SQL = """

{{
    config(
        materialized='table'
    )
}}

with trigger_ref as (

  -- we should still be able to ref a model by its filepath
  select * from {{ ref('foo_alias') }}

)

-- this name should still be the filename
select {{ string_literal(this.name) }} as tablename

"""

# error #
DUPE__MODEL_A_SQL = """

{{ config(alias='duped_alias') }}

select 1 as id

"""

DUPE__MODEL_B_SQL = """

{{ config(alias='duped_alias') }}

select 1 as id

"""

# dupe custom database #
DUPE_CUSTOM_DATABASE__MODEL_A_SQL = """
select {{ string_literal(this.name) }} as tablename

"""

DUPE_CUSTOM_DATABASE__MODEL_B_SQL = """
select {{ string_literal(this.name) }} as tablename

"""


# dupe custom schema #
DUPE_CUSTOM_SCHEMA__MODEL_A_SQL = """

{{ config(alias='duped_alias', schema='schema_a') }}

select {{ string_literal(this.name) }} as tablename

"""

DUPE_CUSTOM_SCHEMA__MODEL_B_SQL = """

{{ config(alias='duped_alias', schema='schema_b') }}

select {{ string_literal(this.name) }} as tablename

"""

DUPE_CUSTOM_SCHEMA__MODEL_C_SQL = """

-- no custom schema for this model
{{ config(alias='duped_alias') }}

select {{ string_literal(this.name) }} as tablename

"""
