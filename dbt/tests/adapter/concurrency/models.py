invalid_sql = """
{{
  config(
    materialized = "table"
  )
}}

select a_field_that_does_not_exist from {{ this.schema }}.seed

"""

table_a_sql = """
{{
  config(
    materialized = "table"
  )
}}

select * from {{ this.schema }}.seed

"""

table_b_sql = """
{{
  config(
    materialized = "table"
  )
}}

select * from {{ this.schema }}.seed

"""

view_model_sql = """
{{
  config(
    materialized = "view"
  )
}}

select * from {{ this.schema }}.seed

"""

dep_sql = """
{{
  config(
    materialized = "table"
  )
}}

select * from {{ref('view_model')}}

"""

view_with_conflicting_cascade_sql = """
select * from {{ref('table_a')}}

union all

select * from {{ref('table_b')}}

"""

skip_sql = """
select * from {{ref('invalid')}}

"""
