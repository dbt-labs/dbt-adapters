-- Example: Incremental model with merge strategy
-- This model only processes new/updated records

{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='fail'
) }}

select
    order_id,
    customer_id,
    amount,
    order_date,
    current_timestamp as updated_at
from {{ ref('stg_orders') }}

{% if is_incremental() %}
  -- Only include new or updated records
  where order_date > (select max(order_date) from {{ this }})
{% endif %}
