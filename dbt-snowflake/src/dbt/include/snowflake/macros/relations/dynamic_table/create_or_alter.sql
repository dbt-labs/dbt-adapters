{% macro snowflake__get_create_or_alter_dynamic_table_sql(
    existing_relation,
    configuration_changes,
    target_relation,
    sql
) -%}

    {% if configuration_changes.requires_full_refresh %}
        {{- log('Applying full refresh (CREATE OR REPLACE) to: ' ~ existing_relation ~ ' due to changes that require table recreation') -}}
        {{- get_replace_sql(existing_relation, target_relation, sql) -}}

    {% else %}

        {%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}

        {%- if catalog_relation.catalog_type == 'INFO_SCHEMA' -%}
            {%- set dynamic_table = target_relation.from_config(config.model) -%}
            {{- log('Applying CREATE OR ALTER to: ' ~ existing_relation) -}}
            {{ snowflake__create_or_alter_dynamic_table_info_schema_sql(dynamic_table, target_relation, sql) }}
        {%- elif catalog_relation.catalog_type == 'BUILT_IN' -%}
            {%- if configuration_changes.refresh_mode -%}
                {#- Snowflake's ALTER DYNAMIC TABLE ... SET does not support refresh_mode -#}
                {{- log('Applying full refresh (CREATE OR REPLACE) to: ' ~ existing_relation ~ ' because refresh_mode cannot be altered on Iceberg tables') -}}
                {{- get_replace_sql(existing_relation, target_relation, sql) -}}
            {%- else -%}
                {{- log('Applying ALTER to: ' ~ existing_relation) -}}
                {{ snowflake__get_alter_dynamic_table_as_sql(existing_relation, configuration_changes) }}
            {%- endif -%}
        {%- else -%}
            {% do exceptions.raise_compiler_error('Unexpected model config for: ' ~ target_relation) %}
        {%- endif -%}

    {%- endif -%}

{%- endmacro %}


{% macro snowflake__create_or_alter_dynamic_table_info_schema_sql(dynamic_table, relation, sql) -%}

    {%- if dynamic_table.transient is not none -%}
        {%- set is_transient = dynamic_table.transient -%}
    {%- elif adapter.behavior.snowflake_default_transient_dynamic_tables.no_warn -%}
        {%- set is_transient = true -%}
    {%- else -%}
        {%- set is_transient = false -%}
    {%- endif -%}
    {%- set transient_keyword = 'transient ' if is_transient else '' -%}
create or alter {{ transient_keyword }}dynamic table {{ relation }}
    {% if dynamic_table.target_lag is not none %}target_lag = '{{ dynamic_table.target_lag }}'{% endif %}
    warehouse = {{ dynamic_table.warehouse_parameter }}
    {{ optional('initialization_warehouse', dynamic_table.snowflake_initialization_warehouse) }}
    {{ optional('refresh_mode', dynamic_table.refresh_mode) }}
    {{ optional('initialize', dynamic_table.initialize) }}
    {% if dynamic_table.scheduler is not none %}
    scheduler = '{{ dynamic_table.scheduler }}'
    {% elif dynamic_table.target_lag is none %}
    scheduler = 'DISABLE'
    {% endif %}
    {#- Snowflake error 001506: CREATE OR ALTER does not support setting policies or tags.
        row_access_policy and table_tag must be managed via separate ALTER TABLE statements. -#}
    {{ optional('cluster by', dynamic_table.cluster_by, quote_char='(', equals_char='') }}
    {{ optional('immutable where', dynamic_table.immutable_where, quote_char='(', equals_char='') }}
    as (
        {{ sql }}
    )

{%- endmacro %}
