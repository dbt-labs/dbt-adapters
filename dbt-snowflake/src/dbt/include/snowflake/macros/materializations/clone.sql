{% macro snowflake__can_clone_table() %}
    {{ return(True) }}
{% endmacro %}

{% macro snowflake__create_or_replace_clone(this_relation, defer_relation) %}
    {%- set source_relation = load_cached_relation(defer_relation) -%}
    {%- set is_iceberg = source_relation is not none and source_relation.is_iceberg_format -%}
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
