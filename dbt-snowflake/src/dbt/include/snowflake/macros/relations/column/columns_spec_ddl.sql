{% macro snowflake__get_table_columns_and_constraints() -%}
  {#-
    Snowflake + Iceberg: a model contract that declares a numeric column without precision
    and scale (a bare NUMBER / NUMERIC / DECIMAL) renders DDL that Snowflake rejects on Iceberg
    tables with error 099200. Regular Snowflake tables silently coerce bare NUMBER to
    NUMBER(38,0), but Iceberg requires the precision and scale to be explicit.

    Fail fast with a clear dbt error before the CREATE is sent to Snowflake, instead of the
    opaque runtime 099200 Snowflake returns after the statement executes. This runs at the
    start of `dbt run` (where dbt enforces contracts), not during `dbt compile`. We do not
    rewrite the user's declared type; we ask them to make it explicit.

    Scope: only when the target is an Iceberg relation (BUILT_IN managed or ICEBERG_REST CLD).
    Non-Iceberg contracts are unaffected and fall through to the standard rendering.
  -#}
  {%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}
  {%- if catalog_relation is not none
        and catalog_relation.catalog_type in ['BUILT_IN', 'ICEBERG_REST']
        and model.get('columns') -%}
    {%- set bare_numeric_columns = [] -%}
    {%- for column in model['columns'].values() -%}
      {%- set data_type = (column.get('data_type') or '') | trim | lower -%}
      {%- if '(' not in data_type and data_type in ['number', 'numeric', 'decimal'] -%}
        {%- do bare_numeric_columns.append(column['name']) -%}
      {%- endif -%}
    {%- endfor -%}
    {%- if bare_numeric_columns | length > 0 -%}
      {%- do exceptions.raise_compiler_error(
        "Iceberg tables require explicit precision and scale for numeric types. In model '"
        ~ model['name'] ~ "', the contract declares column(s) "
        ~ (bare_numeric_columns | join(', '))
        ~ " as a bare NUMBER/NUMERIC/DECIMAL. Set an explicit precision and scale (for example "
        ~ "number(38, 0)) in the contract. Left unspecified, Snowflake rejects the Iceberg "
        ~ "table creation with error 099200.") -%}
    {%- endif -%}
  {%- endif -%}
  {{ return(table_columns_and_constraints()) }}
{%- endmacro %}
