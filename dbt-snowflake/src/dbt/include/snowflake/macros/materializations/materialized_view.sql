{% materialization materialized_view, adapter='snowflake' -%}

  {% set original_query_tag = set_query_tag() %}

  {% set full_refresh_mode = (should_full_refresh()) %}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {{ run_hooks(pre_hooks) }}

  {% if (existing_relation is none or full_refresh_mode) %}
      {% set build_sql = create_materialized_view_as(target_relation, sql, config) %}
  {% elif existing_relation.is_table or is_regular_view(existing_relation) %}
      {#-- Can't overwrite a view or a table - we must drop --#}
      {{ log("Dropping relation " ~ target_relation ~ " because it is a " ~ existing_relation.type ~ " and this model is a materialized view.") }}
      {% do adapter.drop_relation(existing_relation) %}
      {% set build_sql = create_materialized_view_as(target_relation, sql, config) %}
  {% else %}
      {# noop #}
  {% endif %}
  
  {% if build_sql %}
      {% call statement("main") %}
          {{ build_sql }}
      {% endcall %}
  {% else %}
    {{ store_result('main', 'SKIP') }}
  {% endif %}

  {{ run_hooks(post_hooks) }}
  
  {% do persist_docs(target_relation, model) %}
  
  {% do unset_query_tag(original_query_tag) %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}

{% macro is_regular_view(relation) %}
    {#-- This is a workaround for the fact that SHOW OBJECTS does not return the materialized view type --#}
    {#-- and we need to check if the view is materialized or not --#}
    {%- if not relation or not relation.is_view -%}
        {% do return(false) %}
    {%- endif -%}
    {#-- Run a SHOW VIEWS query to check if the view is materialized --#}
    {%- set specs_sql -%}
      show views in "{{ relation.database }}"."{{ relation.schema }}" starts with '{{ relation.name }}' limit 1
    {%- endset -%}
    {% set view_specs = run_query(specs_sql) %}
    {% set is_materialized = view_specs.columns.get('is_materialized').values()[0] == 'true' %}
    {% do return(not is_materialized) %}
{% endmacro %}

{% macro refresh_materialized_view(relation, config) %}
    {{ return(adapter.dispatch('refresh_materialized_view')(relation, config)) }}
{% endmacro %}

{% macro default__refresh_materialized_view(relation, config) -%}

    refresh materialized view {{relation}}

{% endmacro %}

{# override builtin behavior of adapter.drop_relation #}
{% macro drop_relation(relation) -%}
  {% set relation_type = 'materialized view' if relation.type == 'materializedview' else relation.type %}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation_type }} if exists {{ relation }} cascade
  {%- endcall %}
{% endmacro %}

{% macro create_materialized_view_as(relation, sql, config) %}
    {{ return(adapter.dispatch('create_materialized_view_as')(relation, sql, config)) }}
{% endmacro %}

{% macro snowflake__create_materialized_view_as(relation, sql, config) -%}
    {%- set secure = config.get('secure', default=false) -%}
    {%- set cluster_by_keys = config.get('cluster_by', default=none) -%}
    {%- set enable_automatic_clustering = config.get('automatic_clustering', default=false) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {%- if cluster_by_keys is not none and cluster_by_keys is string -%}
        {%- set cluster_by_keys = [cluster_by_keys] -%}
    {%- endif -%}
    {%- if cluster_by_keys is not none -%}
        {%- set cluster_by_string = cluster_by_keys|join(", ")-%}
    {% else %}
        {%- set cluster_by_string = none -%}
    {%- endif -%}

    {{ sql_header if sql_header is not none }}

    create or replace 
        {% if secure -%} secure {%- endif %} 
        materialized view {{relation}}
    as (
        {{sql}}
    );
    
    {% if cluster_by_string is not none and not temporary -%}
      alter materialized view {{relation}} cluster by ({{cluster_by_string}});
    {%- endif -%}
    {% if enable_automatic_clustering and cluster_by_string is not none and not temporary  -%}
      alter materialized view {{relation}} resume recluster;
    {%- endif -%}

{% endmacro %}
