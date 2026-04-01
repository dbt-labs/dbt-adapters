{% macro snowflake__can_clone_table() %}
    {{ return(True) }}
{% endmacro %}

{% macro snowflake__create_or_replace_clone(this_relation, defer_relation) %}
    {%- set is_iceberg = snowflake__is_iceberg_table(defer_relation) -%}
    create or replace
      {% if is_iceberg -%}
        iceberg
      {%- else -%}
        {{ "transient" if config.get("transient", true) }}
      {%- endif %}
      table {{ this_relation }}
      clone {{ defer_relation }}
      {{ "copy grants" if config.get("copy_grants", false) }}
{% endmacro %}

{% macro snowflake__is_iceberg_table(relation) %}
    {%- set source_relation = load_cached_relation(relation) -%}
    {%- if source_relation is not none -%}
        {{ return(source_relation.is_iceberg_format) }}
    {%- endif -%}

    {#- Cache miss: query Snowflake directly -#}
    {%- set show_sql -%}
        show tables like '{{ relation.identifier }}' in schema {{ relation.database }}.{{ relation.schema }}
    {%- endset -%}
    {%- set results = run_query(show_sql) -%}
    {%- for row in results -%}
        {%- if row['name'] == relation.identifier -%}
            {{ return(row['is_iceberg'] in ('Y', 'YES')) }}
        {%- endif -%}
    {%- endfor -%}
    {{ return(false) }}
{% endmacro %}
