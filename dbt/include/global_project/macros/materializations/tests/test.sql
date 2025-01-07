{%- materialization test, default -%}

  {% set relations = [] %}

  {% set fail_calc = config.get('fail_calc') %}
  {% set warn_if = config.get('warn_if') %}
  {% set error_if = config.get('error_if') %}
  {% set limit = config.get('limit') %}

  {% set main_sql %}
    {{ sql }}
    {{ "limit " ~ limit if limit != none }}
  {% endset %}

  {% if should_store_failures() %}

    {% set target_relation = store_failures(main_sql) %}

    {# Since the test failures have already been saved to the database, reuse that result rather than querying again #}
    {% set main_sql %}
        select *
        from {{ target_relation }}
    {% endset %}

  {% endif %}

  {% call statement('main', fetch_result=True) -%}

    {# Since the limit has already been applied above, no need to apply it again! #}
    {{ get_test_sql(main_sql, fail_calc, warn_if, error_if, limit=none)}}

  {%- endcall %}

  {{ return({'relations': relations}) }}

{%- endmaterialization -%}
