{% macro snowflake__get_create_hybrid_table_as_sql(relation, sql) -%}
{#-
    Produce DDL that creates a hybrid table using CTAS

    Implements CREATE HYBRID TABLE ... AS SELECT:
    https://docs.snowflake.com/en/sql-reference/sql/create-hybrid-table

    Args:
    - relation: SnowflakeRelation - the target relation
    - sql: str - the code defining the model

    Returns:
        A valid DDL statement which will result in a new hybrid table.
-#}

    {%- set config_columns = config.get('columns', {}) -%}
    {%- set primary_key = config.get('primary_key', []) -%}
    {%- set indexes = config.get('indexes', []) -%}
    {%- set unique_key = config.get('unique_key', []) -%}
    {%- set foreign_keys = config.get('foreign_keys', []) -%}

    {#- Handle primary_key as string or list -#}
    {%- if primary_key is string -%}
        {%- set primary_key = [primary_key] -%}
    {%- endif -%}

    {#- Handle unique_key as string or list -#}
    {%- if unique_key is string -%}
        {%- set unique_key = [unique_key] -%}
    {%- endif -%}

    {#- Validate required configs -#}
    {%- if not primary_key or primary_key|length == 0 -%}
        {{ exceptions.raise_compiler_error("Hybrid tables require a primary_key configuration") }}
    {%- endif -%}

    {%- if not config_columns or config_columns|length == 0 -%}
        {{ exceptions.raise_compiler_error("Hybrid tables require explicit column definitions in the 'columns' configuration") }}
    {%- endif -%}

    create or replace hybrid table {{ relation }} (
        {%- for column_name, data_type in config_columns.items() %}
        {{ column_name }} {{ data_type }}
        {%- if not loop.last %},{% endif %}
        {%- endfor %}

        {#- Add primary key constraint -#}
        , primary key ({{ primary_key | join(', ') }})

        {#- Add unique constraints if specified -#}
        {%- if unique_key and unique_key|length > 0 %}
        , unique ({{ unique_key | join(', ') }})
        {%- endif -%}

        {#- Add foreign key constraints if specified -#}
        {%- if foreign_keys %}
        {%- for fk in foreign_keys %}
        , foreign key ({{ fk.columns | join(', ') }}) references {{ fk.parent_table }}({{ fk.parent_columns | join(', ') }})
        {%- endfor %}
        {%- endif -%}

        {#- Add secondary indexes if specified -#}
        {%- if indexes %}
        {%- for index in indexes %}
        , index {% if index.name %}{{ index.name }}{% else %}idx_{{ index.columns | join('_') }}{% endif %}({{ index.columns | join(', ') }})
        {%- if index.get('include') %} include ({{ index.include | join(', ') }}){% endif %}
        {%- endfor %}
        {%- endif %}
    ) as (
        select
            {%- for column_name, _ in config_columns.items() %}
            {{ column_name }}{%- if not loop.last %}, {% endif %}
            {%- endfor %}
        from (
            {{ sql }}
        ) as source
    )

{%- endmacro %}
