-- Example: View model
-- This creates a view that summarizes orders by customer

{{ config(
    materialized='view'
) }}

select
    customer_id,
    count(*) as total_orders,
    sum(amount) as total_amount,
    avg(amount) as avg_amount,
    min(order_date) as first_order_date,
    max(order_date) as last_order_date
from {{ ref('stg_orders') }}
group by customer_id
