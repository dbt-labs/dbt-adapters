{#
    Macros for creating Iceberg tables in AWS Glue Data Catalog via Redshift external schemas.

    These macros enable dbt models to be persisted as Iceberg tables, which provide:
    - ACID transactions
    - Schema evolution
    - Time travel (via Athena/EMR)
    - Efficient query performance with predicate pushdown
#}


{% macro redshift__create_iceberg_table_as(relation, sql, catalog_relation) -%}
{#
    Creates an Iceberg table in AWS Glue Data Catalog via Redshift external schema.

    This macro:
    1. Builds the external schema relation for the Iceberg table
    2. Drops existing table if present (Iceberg doesn't support CREATE OR REPLACE)
    3. Gets the column schema from the compiled query
    4. Creates the Iceberg table with explicit column definitions
    5. Inserts data from the query

    Args:
        relation: The target relation (dbt will have set schema to original schema)
        sql: The compiled SQL query
        catalog_relation: RedshiftCatalogRelation with Iceberg configuration
#}

{#-- Build the external schema relation --#}
{%- set external_schema = catalog_relation.external_schema -%}
{%- set external_relation = relation.incorporate(path={"schema": external_schema}) -%}

{#-- Get SQL header if configured --#}
{%- set sql_header = config.get('sql_header', none) -%}
{{ sql_header if sql_header is not none }}

{#-- Check if table exists and drop it (Iceberg doesn't support CREATE OR REPLACE) --#}
{%- set existing_relation = adapter.get_relation(
    database=external_relation.database,
    schema=external_relation.schema,
    identifier=external_relation.identifier
) -%}

{% if existing_relation is not none %}
DROP TABLE IF EXISTS {{ existing_relation }};
{% endif %}

{#-- Get column schema from the compiled query --#}
{%- set columns = adapter.get_columns_in_relation(
    api.Relation.create(
        database=relation.database,
        schema=relation.schema,
        identifier='__dbt_tmp_iceberg_' ~ relation.identifier
    ).incorporate(type='cte')
) -%}

{#-- If we can't get columns from CTE approach, use a temp table approach --#}
{%- if columns | length == 0 -%}
{#-- Create a temp view to extract schema --#}
CREATE OR REPLACE VIEW {{ relation.schema }}.__dbt_iceberg_schema_{{ relation.identifier }} AS (
    {{ sql }}
);

{%- set columns = adapter.get_columns_in_relation(
    api.Relation.create(
        database=relation.database,
        schema=relation.schema,
        identifier='__dbt_iceberg_schema_' ~ relation.identifier
    ).incorporate(type='view')
) -%}
{%- endif -%}

{#-- Create the Iceberg table with explicit column definitions --#}
CREATE TABLE {{ external_relation }} (
    {%- for column in columns %}
    {{ adapter.quote(column.name) }} {{ redshift__iceberg_type_mapping(column.data_type) }}
    {%- if not loop.last %},{% endif %}
    {%- endfor %}
)
{%- if catalog_relation.partition_by and catalog_relation.partition_by | length > 0 %}
PARTITIONED BY ({{ catalog_relation.partition_by | join(', ') }})
{%- endif %}
LOCATION '{{ catalog_relation.storage_uri }}'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = '{{ catalog_relation.file_format }}'
);

{#-- Insert data from the query --#}
INSERT INTO {{ external_relation }}
{{ sql }};

{#-- Clean up temp view if created --#}
{%- if columns | length > 0 %}
DROP VIEW IF EXISTS {{ relation.schema }}.__dbt_iceberg_schema_{{ relation.identifier }};
{%- endif -%}

{%- endmacro %}


{% macro redshift__iceberg_type_mapping(redshift_type) -%}
{#
    Maps Redshift data types to Iceberg-compatible types.

    Iceberg supports a specific set of types, and some Redshift types
    need to be mapped to their Iceberg equivalents.

    Args:
        redshift_type: The Redshift data type string

    Returns:
        The Iceberg-compatible type string
#}
{%- set type_lower = redshift_type | lower -%}

{#-- Handle varchar/character varying --#}
{%- if 'character varying' in type_lower or 'varchar' in type_lower -%}
    {{ return('string') }}

{#-- Handle char/character --#}
{%- elif 'character' in type_lower or type_lower.startswith('char') -%}
    {{ return('string') }}

{#-- Handle text --#}
{%- elif type_lower == 'text' -%}
    {{ return('string') }}

{#-- Handle numeric/decimal --#}
{%- elif 'numeric' in type_lower or 'decimal' in type_lower -%}
    {{ return(redshift_type) }}

{#-- Handle integer types --#}
{%- elif type_lower in ['smallint', 'int2'] -%}
    {{ return('int') }}
{%- elif type_lower in ['integer', 'int', 'int4'] -%}
    {{ return('int') }}
{%- elif type_lower in ['bigint', 'int8'] -%}
    {{ return('bigint') }}

{#-- Handle floating point --#}
{%- elif type_lower in ['real', 'float4'] -%}
    {{ return('float') }}
{%- elif type_lower in ['double precision', 'float8', 'float'] -%}
    {{ return('double') }}

{#-- Handle boolean --#}
{%- elif type_lower in ['boolean', 'bool'] -%}
    {{ return('boolean') }}

{#-- Handle date/time types --#}
{%- elif type_lower == 'date' -%}
    {{ return('date') }}
{%- elif 'timestamp' in type_lower -%}
    {{ return('timestamp') }}
{%- elif type_lower == 'time' or 'time without' in type_lower -%}
    {{ return('string') }}

{#-- Handle binary --#}
{%- elif type_lower in ['bytea', 'varbyte'] -%}
    {{ return('binary') }}

{#-- Default: pass through --#}
{%- else -%}
    {{ return(redshift_type) }}
{%- endif -%}

{%- endmacro %}


{% macro redshift__get_iceberg_columns_from_sql(sql) -%}
{#
    Extracts column names and types from a SQL query by creating a temporary view.

    This is a workaround since Redshift doesn't support DESCRIBE on a query directly.
    We create a temp view, get its columns, then drop it.

    Args:
        sql: The SQL query to analyze

    Returns:
        List of column objects with name and data_type attributes
#}
{%- set tmp_view_name = 'dbt_iceberg_tmp_' ~ modules.datetime.datetime.now().strftime('%Y%m%d%H%M%S%f') -%}
{%- set tmp_relation = api.Relation.create(
    database=target.database,
    schema=target.schema,
    identifier=tmp_view_name
).incorporate(type='view') -%}

{#-- Create temporary view --#}
{% call statement('create_tmp_view', fetch_result=False) %}
    CREATE OR REPLACE VIEW {{ tmp_relation }} AS ({{ sql }})
{% endcall %}

{#-- Get columns --#}
{%- set columns = adapter.get_columns_in_relation(tmp_relation) -%}

{#-- Drop temporary view --#}
{% call statement('drop_tmp_view', fetch_result=False) %}
    DROP VIEW IF EXISTS {{ tmp_relation }}
{% endcall %}

{{ return(columns) }}

{%- endmacro %}
