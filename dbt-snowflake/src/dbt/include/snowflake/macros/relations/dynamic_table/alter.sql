{% macro snowflake__get_alter_dynamic_table_as_sql(
    existing_relation,
    configuration_changes,
    target_relation,
    sql
) -%}
    {{- log('Applying ALTER to: ' ~ existing_relation) -}}

    {% if configuration_changes.requires_full_refresh %}
        {{- get_replace_sql(existing_relation, target_relation, sql) -}}

    {% else %}

        {%- set target_lag = configuration_changes.target_lag -%}
        {%- if target_lag -%}{{- log('Applying UPDATE TARGET_LAG to: ' ~ existing_relation) -}}{%- endif -%}
        {%- set snowflake_warehouse = configuration_changes.snowflake_warehouse -%}
        {%- if snowflake_warehouse -%}{{- log('Applying UPDATE WAREHOUSE to: ' ~ existing_relation) -}}{%- endif -%}
        {%- set snowflake_initialization_warehouse = configuration_changes.snowflake_initialization_warehouse -%}
        {%- if snowflake_initialization_warehouse and snowflake_initialization_warehouse.context -%}{{- log('Applying UPDATE INITIALIZATION_WAREHOUSE to: ' ~ existing_relation) -}}{%- endif -%}
        {%- set immutable_where = configuration_changes.immutable_where -%}
        {%- if immutable_where -%}{{- log('Applying UPDATE IMMUTABLE WHERE to: ' ~ existing_relation) -}}{%- endif -%}

        {#- Only generate ALTER SET if there are properties to set -#}
        {%- set has_set_changes = target_lag or snowflake_warehouse or (snowflake_initialization_warehouse and snowflake_initialization_warehouse.context) -%}
        {% if has_set_changes %}
        alter dynamic table {{ existing_relation }} set
            {% if target_lag %}target_lag = '{{ target_lag.context }}'{% endif %}
            {% if snowflake_warehouse %}warehouse = {{ snowflake_warehouse.context }}{% endif %}
            {% if snowflake_initialization_warehouse and snowflake_initialization_warehouse.context %}initialization_warehouse = {{ snowflake_initialization_warehouse.context }}{% endif %}
        {% endif %}

        {#- Handle unsetting initialization_warehouse when changed to None/empty -#}
        {% if snowflake_initialization_warehouse and not snowflake_initialization_warehouse.context %}
        {% if has_set_changes %};{% endif %}
        alter dynamic table {{ existing_relation }} unset initialization_warehouse
            {% if immutable_where and immutable_where.context %}immutable where ({{ immutable_where.context }}){% endif %}

        {#- Handle unsetting immutable_where when changed to None/empty -#}
        {% if immutable_where and not immutable_where.context %}
        ;
        alter dynamic table {{ existing_relation }} unset immutable where
        {% endif %}

    {%- endif -%}

{%- endmacro %}
