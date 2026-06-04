{% macro redshift__create_table_iceberg_sql(relation, sql, catalog_relation) -%}
{#-
    Creates an Apache Iceberg table backed by the AWS Glue Data Catalog using
    CREATE TABLE ... USING ICEBERG AS SELECT.
    https://docs.aws.amazon.com/redshift/latest/dg/iceberg-writes-sql-syntax.html

    Limitations (per AWS docs):
    - Iceberg does not support CREATE OR REPLACE, so we DROP first for idempotency.
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

  {# Iceberg has no CREATE OR REPLACE and tables can't be renamed; drop first for idempotency #}
  {%- set existing_relation = adapter.get_relation(
        database=relation.database, schema=relation.schema, identifier=relation.identifier) -%}
  {% if existing_relation is not none %}
    drop table if exists {{ existing_relation }};
  {% endif %}

  create table {{ relation }}
    using iceberg
    {{ optional('location', catalog_relation.external_volume, "'") }}
    {% if partition_by_string -%} partitioned by ({{ partition_by_string }}) {%- endif %}
    {% if table_properties_string -%} table properties ({{ table_properties_string }}) {%- endif %}
  as (
    {{ sql }}
  );
{%- endmacro %}
