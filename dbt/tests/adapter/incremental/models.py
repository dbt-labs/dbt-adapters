INCREMENTAL_SYNC_REMOVE_ONLY = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns'

    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% set string_type = dbt.type_string() %}

{% if is_incremental() %}

SELECT id,
       cast(field1 as {{string_type}}) as field1

FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

select id,
       cast(field1 as {{string_type}}) as field1,
       cast(field2 as {{string_type}}) as field2

from source_data where id <= 3

{% endif %}
"""

INCREMENTAL_IGNORE = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='ignore'
    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% if is_incremental() %}

SELECT id, field1, field2, field3, field4 FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

SELECT id, field1, field2 FROM source_data LIMIT 3

{% endif %}
"""

INCREMENTAL_SYNC_REMOVE_ONLY_TARGET = """
{{
    config(materialized='table')
}}

with source_data as (

    select * from {{ ref('model_a') }}

)

{% set string_type = dbt.type_string() %}

select id
       ,cast(field1 as {{string_type}}) as field1

from source_data
order by id
"""

INCREMENTAL_IGNORE_TARGET = """
{{
    config(materialized='table')
}}

with source_data as (

    select * from {{ ref('model_a') }}

)

select id
       ,field1
       ,field2

from source_data
"""

INCREMENTAL_FAIL = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='fail'
    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% if is_incremental()  %}

SELECT id, field1, field2 FROM source_data

{% else %}

SELECT id, field1, field3 FROm source_data

{% endif %}
"""

INCREMENTAL_SYNC_ALL_COLUMNS = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns'

    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% set string_type = dbt.type_string() %}

{% if is_incremental() %}

SELECT id,
       cast(field1 as {{string_type}}) as field1,
       cast(field3 as {{string_type}}) as field3, -- to validate new fields
       cast(field4 as {{string_type}}) AS field4 -- to validate new fields

FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

select id,
       cast(field1 as {{string_type}}) as field1,
       cast(field2 as {{string_type}}) as field2

from source_data where id <= 3

{% endif %}
"""

INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns'
    )
}}

{% set string_type = dbt.type_string() %}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% if is_incremental()  %}

SELECT id,
       cast(field1 as {{string_type}}) as field1,
       cast(field3 as {{string_type}}) as field3,
       cast(field4 as {{string_type}}) as field4
FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

SELECT id,
       cast(field1 as {{string_type}}) as field1,
       cast(field2 as {{string_type}}) as field2
FROM source_data where id <= 3

{% endif %}
"""

A = """
{{
    config(materialized='table')
}}

with source_data as (

    select 1 as id, 'aaa' as field1, 'bbb' as field2, 111 as field3, 'TTT' as field4
    union all select 2 as id, 'ccc' as field1, 'ddd' as field2, 222 as field3, 'UUU' as field4
    union all select 3 as id, 'eee' as field1, 'fff' as field2, 333 as field3, 'VVV' as field4
    union all select 4 as id, 'ggg' as field1, 'hhh' as field2, 444 as field3, 'WWW' as field4
    union all select 5 as id, 'iii' as field1, 'jjj' as field2, 555 as field3, 'XXX' as field4
    union all select 6 as id, 'kkk' as field1, 'lll' as field2, 666 as field3, 'YYY' as field4

)

select id
       ,field1
       ,field2
       ,field3
       ,field4

from source_data
"""

INCREMENTAL_APPEND_NEW_COLUMNS_TARGET = """
{{
    config(materialized='table')
}}

{% set string_type = dbt.type_string() %}

with source_data as (

    select * from {{ ref('model_a') }}

)

select id
       ,cast(field1 as {{string_type}}) as field1
       ,cast(field2 as {{string_type}}) as field2
       ,cast(CASE WHEN id <= 3 THEN NULL ELSE field3 END as {{string_type}}) AS field3
       ,cast(CASE WHEN id <= 3 THEN NULL ELSE field4 END as {{string_type}}) AS field4

from source_data
"""

INCREMENTAL_APPEND_NEW_COLUMNS = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='append_new_columns'
    )
}}

{% set string_type = dbt.type_string() %}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% if is_incremental()  %}

SELECT id,
       cast(field1 as {{string_type}}) as field1,
       cast(field2 as {{string_type}}) as field2,
       cast(field3 as {{string_type}}) as field3,
       cast(field4 as {{string_type}}) as field4
FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

SELECT id,
       cast(field1 as {{string_type}}) as field1,
       cast(field2 as {{string_type}}) as field2
FROM source_data where id <= 3

{% endif %}
"""

INCREMENTAL_SYNC_ALL_COLUMNS_TARGET = """
{{
    config(materialized='table')
}}

with source_data as (

    select * from {{ ref('model_a') }}

)

{% set string_type = dbt.type_string() %}

select id
       ,cast(field1 as {{string_type}}) as field1
       --,field2
       ,cast(case when id <= 3 then null else field3 end as {{string_type}}) as field3
       ,cast(case when id <= 3 then null else field4 end as {{string_type}}) as field4

from source_data
order by id
"""

INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE_TARGET = """
{{
    config(materialized='table')
}}

{% set string_type = dbt.type_string() %}

with source_data as (

    select * from {{ ref('model_a') }}

)

select id,
       cast(field1 as {{string_type}}) as field1,
       cast(CASE WHEN id >  3 THEN NULL ELSE field2 END as {{string_type}}) AS field2,
       cast(CASE WHEN id <= 3 THEN NULL ELSE field3 END as {{string_type}}) AS field3,
       cast(CASE WHEN id <= 3 THEN NULL ELSE field4 END as {{string_type}}) AS field4

from source_data
"""

merge_exclude_columns_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy='merge',
    merge_exclude_columns=['msg']
) }}

{% if not is_incremental() %}

-- data for first invocation of model

select 1 as id, 'hello' as msg, 'blue' as color
union all
select 2 as id, 'goodbye' as msg, 'red' as color

{% else %}

-- data for subsequent incremental update

select 1 as id, 'hey' as msg, 'blue' as color
union all
select 2 as id, 'yo' as msg, 'green' as color
union all
select 3 as id, 'anyway' as msg, 'purple' as color

{% endif %}
"""

delete_insert_incremental_predicates_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id'
) }}

{% if not is_incremental() %}

select 1 as id, 'hello' as msg, 'blue' as color
union all
select 2 as id, 'goodbye' as msg, 'red' as color

{% else %}

-- delete will not happen on the above record where id = 2, so new record will be inserted instead
select 1 as id, 'hey' as msg, 'blue' as color
union all
select 2 as id, 'yo' as msg, 'green' as color
union all
select 3 as id, 'anyway' as msg, 'purple' as color

{% endif %}
"""


trinary_unique_key_list_sql = """
-- a multi-argument unique key list should see overwriting on rows in the model
--   where all unique key fields apply

{{
    config(
        materialized='incremental',
        unique_key=['state', 'county', 'city']
    )
}}

select
    state as state,
    county as county,
    city as city,
    last_visit_date as last_visit_date
from {{ ref('seed') }}

{% if is_incremental() %}
    where last_visit_date > (select max(last_visit_date) from {{ this }})
{% endif %}

"""

nontyped_trinary_unique_key_list_sql = """
-- a multi-argument unique key list should see overwriting on rows in the model
--   where all unique key fields apply
--   N.B. needed for direct comparison with seed

{{
    config(
        materialized='incremental',
        unique_key=['state', 'county', 'city']
    )
}}

select
    state as state,
    county as county,
    city as city,
    last_visit_date as last_visit_date
from {{ ref('seed') }}

{% if is_incremental() %}
    where last_visit_date > (select max(last_visit_date) from {{ this }})
{% endif %}

"""

unary_unique_key_list_sql = """
-- a one argument unique key list should result in overwritting semantics for
--   that one matching field

{{
    config(
        materialized='incremental',
        unique_key=['state']
    )
}}

select
    state as state,
    county as county,
    city as city,
    last_visit_date as last_visit_date
from {{ ref('seed') }}

{% if is_incremental() %}
    where last_visit_date > (select max(last_visit_date) from {{ this }})
{% endif %}

"""

not_found_unique_key_sql = """
-- a model with a unique key not found in the table itself will error out

{{
    config(
        materialized='incremental',
        unique_key='thisisnotacolumn'
    )
}}

select
    *
from {{ ref('seed') }}

{% if is_incremental() %}
    where last_visit_date > (select max(last_visit_date) from {{ this }})
{% endif %}

"""

empty_unique_key_list_sql = """
-- model with empty list unique key should build normally

{{
    config(
        materialized='incremental',
        unique_key=[]
    )
}}

select * from {{ ref('seed') }}

{% if is_incremental() %}
    where last_visit_date > (select max(last_visit_date) from {{ this }})
{% endif %}

"""

no_unique_key_sql = """
-- no specified unique key should cause no special build behavior

{{
    config(
        materialized='incremental'
    )
}}

select
    *
from {{ ref('seed') }}

{% if is_incremental() %}
    where last_visit_date > (select max(last_visit_date) from {{ this }})
{% endif %}

"""

empty_str_unique_key_sql = """
-- ensure model with empty string unique key should build normally

{{
    config(
        materialized='incremental',
        unique_key=''
    )
}}

select
    *
from {{ ref('seed') }}

{% if is_incremental() %}
    where last_visit_date > (select max(last_visit_date) from {{ this }})
{% endif %}

"""

str_unique_key_sql = """
-- a unique key with a string should trigger to overwrite behavior when
--   the source has entries in conflict (i.e. more than one row per unique key
--   combination)

{{
    config(
        materialized='incremental',
        unique_key='state'
    )
}}

select
    state as state,
    county as county,
    city as city,
    last_visit_date as last_visit_date
from {{ ref('seed') }}

{% if is_incremental() %}
    where last_visit_date > (select max(last_visit_date) from {{ this }})
{% endif %}

"""

duplicated_unary_unique_key_list_sql = """
{{
    config(
        materialized='incremental',
        unique_key=['state', 'state']
    )
}}

select
    state as state,
    county as county,
    city as city,
    last_visit_date as last_visit_date
from {{ ref('seed') }}

{% if is_incremental() %}
    where last_visit_date > (select max(last_visit_date) from {{ this }})
{% endif %}

"""

not_found_unique_key_list_sql = """
-- a unique key list with any element not in the model itself should error out

{{
    config(
        materialized='incremental',
        unique_key=['state', 'thisisnotacolumn']
    )
}}

select * from {{ ref('seed') }}

"""

expected__one_str__overwrite_sql = """
{{
    config(
        materialized='table'
    )
}}

select
    'CT' as state,
    'Hartford' as county,
    'Hartford' as city,
    cast('2022-02-14' as date) as last_visit_date
union all
select 'MA','Suffolk','Boston','2020-02-12'
union all
select 'NJ','Mercer','Trenton','2022-01-01'
union all
select 'NY','Kings','Brooklyn','2021-04-02'
union all
select 'NY','New York','Manhattan','2021-04-01'
union all
select 'PA','Philadelphia','Philadelphia','2021-05-21'

"""

expected__unique_key_list__inplace_overwrite_sql = """
{{
    config(
        materialized='table'
    )
}}

select
    'CT' as state,
    'Hartford' as county,
    'Hartford' as city,
    cast('2022-02-14' as date) as last_visit_date
union all
select 'MA','Suffolk','Boston','2020-02-12'
union all
select 'NJ','Mercer','Trenton','2022-01-01'
union all
select 'NY','Kings','Brooklyn','2021-04-02'
union all
select 'NY','New York','Manhattan','2021-04-01'
union all
select 'PA','Philadelphia','Philadelphia','2021-05-21'

"""
