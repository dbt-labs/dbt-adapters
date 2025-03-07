input_model_sql = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, TIMESTAMP '2025-01-01 01:25:00-0' as event_time
UNION ALL
select 2 as id, TIMESTAMP '2025-01-02 13:47:00-0' as event_time
UNION ALL
select 3 as id, TIMESTAMP '2025-01-03 01:32:00-0' as event_time
"""

model_that_samples_input_sql = """
{{ config(materialized='table') }}
SELECT * FROM {{ ref('input_model') }}
"""
