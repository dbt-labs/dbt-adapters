dependent_sql = """

-- multiple ephemeral refs should share a cte
select * from {{ref('base')}} where gender = 'Male'
union all
select * from {{ref('base')}} where gender = 'Female'

"""

double_dependent_sql = """

-- base_copy just pulls from base. Make sure the listed
-- graph of CTEs all share the same dbt_cte__base cte
select * from {{ref('base')}} where gender = 'Male'
union all
select * from {{ref('base_copy')}} where gender = 'Female'

"""

super_dependent_sql = """
select * from {{ref('female_only')}}
union all
select * from {{ref('double_dependent')}} where gender = 'Male'

"""

base__female_only_sql = """
{{ config(materialized='ephemeral') }}

select * from {{ ref('base_copy') }} where gender = 'Female'

"""

base__base_sql = """
{{ config(materialized='ephemeral') }}

select * from {{ this.schema }}.seed

"""

base__base_copy_sql = """
{{ config(materialized='ephemeral') }}

select * from {{ ref('base') }}

"""

ephemeral_errors__dependent_sql = """
-- base copy is an error
select * from {{ref('base_copy')}} where gender = 'Male'

"""

ephemeral_errors__base__base_sql = """
{{ config(materialized='ephemeral') }}

select * from {{ this.schema }}.seed

"""

ephemeral_errors__base__base_copy_sql = """
{{ config(materialized='ephemeral') }}

{{ adapter.invalid_method() }}

select * from {{ ref('base') }}

"""

n__ephemeral_level_two_sql = """
{{
  config(
    materialized = "ephemeral",
  )
}}
select * from {{ ref('source_table') }}

"""

n__root_view_sql = """
select * from {{ref("ephemeral")}}

"""

n__ephemeral_sql = """
{{
  config(
    materialized = "ephemeral",
  )
}}
select * from {{ref("ephemeral_level_two")}}

"""

n__source_table_sql = """
{{ config(materialized='table') }}

with source_data as (

    select 1 as id
    union all
    select null as id

)

select *
from source_data

"""
