{%- materialization test, default -%}

  {% set relations = [] %}
  {% set limit = config.get('limit') %}

  {% set sql_with_limit %}
    {{ get_limit_subquery_sql(sql, limit) }}
  {% endset %}

  {% if should_store_failures() %}

    {% set identifier = model['alias'] %}
    
    {# Optionally add unique suffix to test failure table names for parallel execution support #}
    {% set store_failures_unique = config.get('store_failures_unique', false) %}
    {% if store_failures_unique %}
      {% set suffix_strategy = config.get('store_failures_suffix', 'invocation_id') %}
      
      {% if suffix_strategy == 'invocation_id' %}
        {# Use first 8 chars of invocation_id for reasonable table name length #}
        {% set identifier = identifier ~ '_' ~ invocation_id[:8] %}
        
      {% elif suffix_strategy == 'timestamp' %}
        {# Full timestamp: YYYYMMDD_HHMMSS #}
        {% set identifier = identifier ~ '_' ~ run_started_at.strftime('%Y%m%d_%H%M%S') %}
        
      {% elif suffix_strategy == 'date' %}
        {# Date only: YYYYMMDD #}
        {% set identifier = identifier ~ '_' ~ run_started_at.strftime('%Y%m%d') %}
        
      {% elif suffix_strategy == 'hour' %}
        {# Date and hour: YYYYMMDD_HH - useful for hourly DAGs #}
        {% set identifier = identifier ~ '_' ~ run_started_at.strftime('%Y%m%d_%H') %}
        
      {% else %}
        {# Treat as literal string or Jinja template to evaluate #}
        {# This allows for custom suffixes or var-based suffixes #}
        {% set suffix_value = suffix_strategy %}
        {# Handle template rendering if it contains {{ }} #}
        {% if '{{' in suffix_value and '}}' in suffix_value %}
          {% set suffix_value = render(suffix_value) %}
        {% endif %}
        {% set identifier = identifier ~ '_' ~ suffix_value %}
      {% endif %}
      
      {# Ensure table name doesn't exceed platform limits (e.g., 1024 chars for BigQuery) #}
      {# Truncate if necessary, keeping the suffix to maintain uniqueness #}
      {% if identifier|length > 1000 %}
        {% set prefix_max_length = 1000 - (identifier|length - model['alias']|length) %}
        {% set identifier = model['alias'][:prefix_max_length] ~ identifier[model['alias']|length:] %}
      {% endif %}
    {% endif %}
    
    {% set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) %}

    {% set store_failures_as = config.get('store_failures_as') %}
    -- if `--store-failures` is invoked via command line and `store_failures_as` is not set,
    -- config.get('store_failures_as', 'table') returns None, not 'table'
    {% if store_failures_as == none %}{% set store_failures_as = 'table' %}{% endif %}
    {% if store_failures_as not in ['table', 'view'] %}
        {{ exceptions.raise_compiler_error(
            "'" ~ store_failures_as ~ "' is not a valid value for `store_failures_as`. "
            "Accepted values are: ['ephemeral', 'table', 'view']"
        ) }}
    {% endif %}

    {% set target_relation = api.Relation.create(
        identifier=identifier, schema=schema, database=database, type=store_failures_as) -%} %}

    {% if old_relation %}
        {% do adapter.drop_relation(old_relation) %}
    {% endif %}

    {% call statement(auto_begin=True) %}
        {{ get_create_sql(target_relation, sql_with_limit) }}
    {% endcall %}

    {% do relations.append(target_relation) %}

    {# Since the test failures have already been saved to the database, reuse that result rather than querying again #}
    {% set main_sql %}
        select *
        from {{ target_relation }}
    {% endset %}

    {{ adapter.commit() }}

  {% else %}

      {% set main_sql = sql_with_limit %}

  {% endif %}

  {% set fail_calc = config.get('fail_calc') %}
  {% set warn_if = config.get('warn_if') %}
  {% set error_if = config.get('error_if') %}

  {% call statement('main', fetch_result=True) -%}

    {# The limit has already been included above, and we do not want to duplicate it again. We also want to be safe for macro overrides treating `limit` as a required parameter. #}
    {{ get_test_sql(main_sql, fail_calc, warn_if, error_if, limit=none)}}

  {%- endcall %}

  {{ return({'relations': relations}) }}

{%- endmaterialization -%}
