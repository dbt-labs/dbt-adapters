SEED = """
id,value,category
1,100,A
2,200,B
3,300,C
""".strip()


HYBRID_TABLE_BASIC = """
{{ config(
    materialized='hybrid_table',
    columns={
        'id': 'INTEGER',
        'value': 'INTEGER',
        'category': 'VARCHAR(10)'
    },
    primary_key='id'
) }}
select * from {{ ref('my_seed') }}
"""


HYBRID_TABLE_COMPOSITE_KEY = """
{{ config(
    materialized='hybrid_table',
    columns={
        'id': 'INTEGER',
        'value': 'INTEGER',
        'category': 'VARCHAR(10)'
    },
    primary_key=['id', 'category']
) }}
select * from {{ ref('my_seed') }}
"""


HYBRID_TABLE_WITH_INDEX = """
{{ config(
    materialized='hybrid_table',
    columns={
        'id': 'INTEGER',
        'value': 'INTEGER',
        'category': 'VARCHAR(10)',
        'created_at': 'TIMESTAMP_NTZ'
    },
    primary_key='id',
    indexes=[
        {'columns': ['category']},
        {'columns': ['created_at']}
    ]
) }}
select
    id,
    value,
    category,
    current_timestamp() as created_at
from {{ ref('my_seed') }}
"""


HYBRID_TABLE_WITH_NAMED_INDEX = """
{{ config(
    materialized='hybrid_table',
    columns={
        'id': 'INTEGER',
        'value': 'INTEGER',
        'category': 'VARCHAR(10)'
    },
    primary_key='id',
    indexes=[
        {'name': 'idx_category', 'columns': ['category']}
    ]
) }}
select * from {{ ref('my_seed') }}
"""


HYBRID_TABLE_WITH_UNIQUE = """
{{ config(
    materialized='hybrid_table',
    columns={
        'id': 'INTEGER',
        'value': 'INTEGER',
        'category': 'VARCHAR(10)',
        'email': 'VARCHAR(100)'
    },
    primary_key='id',
    unique_key='email'
) }}
select
    id,
    value,
    category,
    'user_' || id || '@example.com' as email
from {{ ref('my_seed') }}
"""


HYBRID_TABLE_INCREMENTAL = """
{{ config(
    materialized='hybrid_table',
    columns={
        'id': 'INTEGER',
        'value': 'INTEGER',
        'category': 'VARCHAR(10)',
        'updated_at': 'TIMESTAMP_NTZ'
    },
    primary_key='id'
) }}
select
    id,
    value,
    category,
    current_timestamp() as updated_at
from {{ ref('my_seed') }}
"""


HYBRID_TABLE_MODIFIED = """
{{ config(
    materialized='hybrid_table',
    columns={
        'id': 'INTEGER',
        'value': 'INTEGER',
        'category': 'VARCHAR(10)',
        'updated_at': 'TIMESTAMP_NTZ'
    },
    primary_key=['id', 'category']  -- Changed primary key
) }}
select
    id,
    value,
    category,
    current_timestamp() as updated_at
from {{ ref('my_seed') }}
"""


HYBRID_TABLE_CONTINUE = """
{{ config(
    materialized='hybrid_table',
    columns={
        'id': 'INTEGER',
        'value': 'INTEGER',
        'category': 'VARCHAR(10)'
    },
    primary_key='id',
    on_schema_change='continue'
) }}
select * from {{ ref('my_seed') }}
"""


HYBRID_TABLE_APPLY = """
{{ config(
    materialized='hybrid_table',
    columns={
        'id': 'INTEGER',
        'value': 'INTEGER',
        'category': 'VARCHAR(10)'
    },
    primary_key='id',
    on_schema_change='apply'
) }}
select * from {{ ref('my_seed') }}
"""


# Seed with additional data for incremental testing
SEED_INCREMENTAL_ADD = """
id,value,category
1,150,A
2,250,B
4,400,D
""".strip()
