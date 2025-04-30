{%- materialization test, default -%}

  {% set relations = [] %}

  {% set limit = config.get('limit') %}
  {% set fail_calc = config.get('fail_calc') %}
  {% set warn_if = config.get('warn_if') %}
  {% set error_if = config.get('error_if') %}

  {% set sql_with_limit %}
    {{ get_limit_subquery_sql(sql, limit) }}
  {% endset %}

  {% if should_store_failures() %}

    {% set target_relation = store_failures(sql_with_limit) %}

    {% do relations.append(target_relation) %}

  {% else %}

      {% set main_sql = sql_with_limit %}

  {% endif %}

  {% call statement('main', fetch_result=True) -%}

    {{ get_test_sql(main_sql, fail_calc, warn_if, error_if)}}

  {%- endcall %}

  {{ return({'relations': relations}) }}

{%- endmaterialization -%}
