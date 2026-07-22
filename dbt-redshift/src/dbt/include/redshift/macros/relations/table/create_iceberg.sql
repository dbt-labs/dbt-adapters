{% macro redshift__create_table_iceberg_sql(relation, sql, catalog_relation) -%}
{#-
    Creates an Apache Iceberg table backed by the AWS Glue Data Catalog using
    CREATE TABLE ... USING ICEBERG AS SELECT.
    https://docs.aws.amazon.com/redshift/latest/dg/iceberg-writes-sql-syntax.html

    Limitations (per AWS docs):
    - Iceberg DDL cannot run inside a multi-statement transaction, so this macro
      emits a SINGLE statement (just the CREATE). Dropping any existing relation is
      handled separately by the materialization, and the connection must run with
      autocommit so dbt does not wrap it in BEGIN/COMMIT.
    - Iceberg tables cannot be renamed, so the materialization builds directly into
      the target relation rather than using an intermediate + rename swap.
    - Iceberg tables do not support column constraints / attributes, so model
      contracts are not supported.
-#}

  {%- set contract_config = config.get('contract') -%}
  {%- if contract_config.enforced -%}
    {% do exceptions.raise_compiler_error(
      "Model contracts are not supported for Redshift Iceberg tables (Iceberg tables do not support column constraints)."
    ) %}
  {%- endif -%}

  {# Warn about native Redshift table configs that have no effect on Iceberg tables #}
  {%- set ignored_configs = [] -%}
  {%- if config.get('dist') -%}{%- do ignored_configs.append('dist') -%}{%- endif -%}
  {%- if config.get('sort') -%}{%- do ignored_configs.append('sort') -%}{%- endif -%}
  {%- if config.get('backup') == false -%}{%- do ignored_configs.append('backup') -%}{%- endif -%}
  {%- if ignored_configs -%}
    {% do exceptions.warn(
      "The following configs have no effect on Redshift Iceberg tables and are ignored: "
      ~ ignored_configs | join(", ")
      ~ ". Iceberg uses `partition_by` instead of `dist`/`sort`, and external storage instead of `backup`."
    ) %}
  {%- endif -%}

  {%- set sql_header = config.get('sql_header', none) -%}
  {{ sql_header if sql_header is not none }}

  {%- set partition_by = catalog_relation.partition_by -%}
  {%- if partition_by is string -%}
    {%- set partition_by_string = partition_by -%}
  {%- elif partition_by -%}
    {%- set partition_by_string = partition_by | join(", ") -%}
  {%- else -%}
    {%- set partition_by_string = none -%}
  {%- endif -%}

  {%- set table_properties = catalog_relation.table_properties -%}
  {%- if table_properties -%}
    {%- set tbl_props = [] -%}
    {%- for key, value in table_properties.items() -%}
      {%- do tbl_props.append("'" ~ key ~ "'='" ~ value ~ "'") -%}
    {%- endfor -%}
    {%- set table_properties_string = tbl_props | join(", ") -%}
  {%- else -%}
    {%- set table_properties_string = none -%}
  {%- endif -%}

  {#- Single statement only: Iceberg DDL can't run in a multi-statement transaction.
      Any existing relation is dropped separately by the materialization. -#}
  create table {{ relation }}
    using iceberg
    {# Redshift uses `location '...'` (no `=`), so pass an empty equals_char #}
    {{ optional('location', catalog_relation.location, "'", '') }}
    {% if partition_by_string -%} partitioned by ({{ partition_by_string }}) {%- endif %}
    {% if table_properties_string -%} table properties ({{ table_properties_string }}) {%- endif %}
  as (
    {{ sql }}
  )
{%- endmacro %}
