BACKUP_IS_FALSE = """
{{ config(
    materialized='table',
    backup=False
) }}
select 1 as my_col
"""


BACKUP_IS_TRUE = """
{{ config(
    materialized='table',
    backup=True
) }}
select 1 as my_col
"""


BACKUP_IS_UNDEFINED = """
{{ config(
    materialized='table'
) }}
select 1 as my_col
"""


BACKUP_IS_TRUE_VIEW = """
{{ config(
    materialized='view',
    backup=True
) }}
select 1 as my_col
"""


SYNTAX_WITH_DISTKEY = """
{{ config(
    materialized='table',
    backup=False,
    dist='my_col'
) }}
select 1 as my_col
"""


SYNTAX_WITH_SORTKEY = """
{{ config(
    materialized='table',
    backup=False,
    sort='my_col'
) }}
select 1 as my_col
"""


BACKUP_IS_UNDEFINED_DEPENDENT_VIEW = """
{{ config(
    materialized='view',
) }}
select * from {{ ref('backup_is_undefined') }}
"""
