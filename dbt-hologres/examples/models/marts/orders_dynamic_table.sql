-- Example: Hologres Dynamic Table (物化视图)
-- This creates a materialized view with automatic refresh

{{
    config(
        materialized='dynamic_table',
        freshness='30 minutes',
        auto_refresh_mode='auto',
        auto_refresh_enable=true,
        computing_resource='serverless',
        orientation='column'
    )
}}

select
    customer_id,
    count(*) as order_count,
    sum(amount) as total_spent,
    avg(amount) as avg_order_value,
    max(order_date) as last_order_date,
    current_timestamp as refreshed_at
from {{ ref('stg_orders') }}
group by customer_id
