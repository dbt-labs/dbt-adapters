{% macro snowflake__get_replace_interactive_table_sql(relation, sql) -%}

    {%- set interactive_table = relation.from_config(config.model) -%}

    create or replace interactive table {{ relation }}
        cluster by ({{ interactive_table.cluster_by }})
        {% if interactive_table.is_dynamic %}
        target_lag = '{{ interactive_table.target_lag }}'
        {{ optional('warehouse', interactive_table.warehouse_parameter, equals_char='= ') }}
        {% endif %}
        {{ optional('initialization_warehouse', interactive_table.snowflake_initialization_warehouse, equals_char='= ') }}
        as (
            {{ sql }}
        )

{%- endmacro %}
