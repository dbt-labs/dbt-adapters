-- Example: Simple table model
-- This creates a standard table in Hologres

{{ config(
    materialized='table',
    schema='staging'
) }}

select
    1 as order_id,
    'customer_1' as customer_id,
    cast(100.50 as numeric(10,2)) as amount,
    current_timestamp as order_date

union all

select
    2 as order_id,
    'customer_2' as customer_id,
    cast(250.75 as numeric(10,2)) as amount,
    current_timestamp as order_date

union all

select
    3 as order_id,
    'customer_1' as customer_id,
    cast(75.25 as numeric(10,2)) as amount,
    current_timestamp as order_date
