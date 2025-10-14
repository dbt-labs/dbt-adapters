{% macro snowflake__create_table_as(temporary, relation, compiled_code, language='sql') -%}

    {%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}

    {%- if language == 'sql' -%}
        {%- if temporary -%}
            {{ snowflake__create_table_temporary_sql(relation, compiled_code) }}
        {%- elif catalog_relation.catalog_type == 'INFO_SCHEMA' -%}
            {{ snowflake__create_table_info_schema_sql(relation, compiled_code) }}
        {%- elif catalog_relation.catalog_type == 'BUILT_IN' -%}
            {{ snowflake__create_table_built_in_sql(relation, compiled_code) }}
        {%- elif catalog_relation.catalog_type == 'ICEBERG_REST' -%}
            {%- if catalog_relation.catalog_linked_database_type is defined and
            catalog_relation.catalog_linked_database_type == 'glue' -%}
                {{ snowflake__create_table_iceberg_rest_with_glue(relation, compiled_code, catalog_relation) }}
            {%- else -%}
                {{ snowflake__create_table_iceberg_rest_sql(relation, compiled_code) }}
            {%- endif -%}
        {%- else -%}
            {% do exceptions.raise_compiler_error('Unexpected model config for: ' ~ relation) %}
        {%- endif -%}

    {%- elif language == 'python' -%}
        {%- if catalog_relation.catalog_type == 'BUILT_IN' %}
            {% do exceptions.raise_compiler_error('Iceberg is incompatible with Python models. Please use a SQL model for the iceberg format.') %}
        {%- else -%}
            {{ py_write_table(compiled_code, relation) }}
        {%- endif %}

    {%- else -%}
        {% do exceptions.raise_compiler_error("snowflake__create_table_as macro didn't get supported language, it got %s" % language) %}

    {%- endif -%}

{% endmacro %}


{% macro snowflake__create_table_temporary_sql(relation, compiled_code) -%}
{#-
    Implements CREATE TEMPORARY TABLE and CREATE TEMPORARY TABLE ... AS SELECT:
    https://docs.snowflake.com/en/sql-reference/sql/create-table
    https://docs.snowflake.com/en/sql-reference/sql/create-table#create-table-as-select-also-referred-to-as-ctas
-#}

{%- set contract_config = config.get('contract') -%}
{%- if contract_config.enforced -%}
    {{- get_assert_columns_equivalent(compiled_code) -}}
    {%- set compiled_code = get_select_subquery(compiled_code) -%}
{%- endif -%}

{%- set sql_header = config.get('sql_header', none) -%}
{{ sql_header if sql_header is not none }}

create or replace temporary table {{ relation }}
    {%- if contract_config.enforced %}
    {{ get_table_columns_and_constraints() }}
    {%- endif %}
as (
    {{ compiled_code }}
    )
;

{%- endmacro %}


{% macro snowflake__create_table_info_schema_sql(relation, compiled_code) -%}
{#-
    Implements CREATE TABLE and CREATE TABLE ... AS SELECT:
    https://docs.snowflake.com/en/sql-reference/sql/create-table
    https://docs.snowflake.com/en/sql-reference/sql/create-table#create-table-as-select-also-referred-to-as-ctas
-#}

{%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}

{%- if catalog_relation.is_transient -%}
    {%- set transient='transient ' -%}
{%- else -%}
    {%- set transient='' -%}
{%- endif -%}

{%- set copy_grants = config.get('copy_grants', default=false) -%}

{%- set row_access_policy = config.get('row_access_policy', default=none) -%}
{%- set table_tag = config.get('table_tag', default=none) -%}

{%- set contract_config = config.get('contract') -%}
{%- if contract_config.enforced -%}
    {{- get_assert_columns_equivalent(compiled_code) -}}
    {%- set compiled_code = get_select_subquery(compiled_code) -%}
{%- endif -%}

{%- set sql_header = config.get('sql_header', none) -%}
{{ sql_header if sql_header is not none }}

create or replace {{ transient }}table {{ relation }}
    {%- set contract_config = config.get('contract') -%}
    {%- if contract_config.enforced %}
    {{ get_table_columns_and_constraints() }}
    {%- endif %}
    {% if copy_grants -%} copy grants {%- endif %}
    {% if row_access_policy -%} with row access policy {{ row_access_policy }} {%- endif %}
    {% if table_tag -%} with tag ({{ table_tag }}) {%- endif %}
    as (
        {%- if catalog_relation.cluster_by is not none -%}
        select * from (
            {{ compiled_code }}
        )
        order by (
            {{ catalog_relation.cluster_by }}
        )
        {%- else -%}
        {{ compiled_code }}
        {%- endif %}
    )
;

{% if catalog_relation.cluster_by is not none -%}
alter table {{ relation }} cluster by ({{ catalog_relation.cluster_by }});
{%- endif -%}

{% if catalog_relation.automatic_clustering and catalog_relation.cluster_by is not none %}
alter table {{ relation }} resume recluster;
{%- endif -%}

{%- endmacro %}


{% macro snowflake__create_table_built_in_sql(relation, compiled_code) -%}
{#-
    Implements CREATE ICEBERG TABLE and CREATE ICEBERG TABLE ... AS SELECT (Snowflake as the Iceberg catalog):
    https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-snowflake

    Limitations:
    - Iceberg does not support temporary tables (use a standard Snowflake table)
-#}

{%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}

{%- set copy_grants = config.get('copy_grants', default=false) -%}

{%- set row_access_policy = config.get('row_access_policy', default=none) -%}
{%- set table_tag = config.get('table_tag', default=none) -%}

{%- set contract_config = config.get('contract') -%}
{%- if contract_config.enforced -%}
    {{- get_assert_columns_equivalent(compiled_code) -}}
    {%- set compiled_code = get_select_subquery(compiled_code) -%}
{%- endif -%}

{%- set sql_header = config.get('sql_header', none) -%}
{{ sql_header if sql_header is not none }}

create or replace iceberg table {{ relation }}
    {%- if contract_config.enforced %}
    {{ get_table_columns_and_constraints() }}
    {%- endif %}
    {{ optional('external_volume', catalog_relation.external_volume, "'") }}
    catalog = 'SNOWFLAKE'  -- required, and always SNOWFLAKE for built-in Iceberg tables
    base_location = '{{ catalog_relation.base_location }}'
    {{ optional('storage_serialization_policy', catalog_relation.storage_serialization_policy, "'")}}
    {{ optional('max_data_extension_time_in_days', catalog_relation.max_data_extension_time_in_days)}}
    {{ optional('data_retention_time_in_days', catalog_relation.data_retention_time_in_days)}}
    {{ optional('change_tracking', catalog_relation.change_tracking)}}
    {% if row_access_policy -%} with row access policy {{ row_access_policy }} {%- endif %}
    {% if table_tag -%} with tag ({{ table_tag }}) {%- endif %}
    {% if copy_grants -%} copy grants {%- endif %}
as (
    {%- if catalog_relation.cluster_by is not none -%}
    select * from (
        {{ compiled_code }}
    )
    order by (
        {{ catalog_relation.cluster_by }}
    )
    {%- else -%}
    {{ compiled_code }}
    {%- endif %}
    )
;

{% if catalog_relation.cluster_by is not none -%}
alter iceberg table {{ relation }} cluster by ({{ catalog_relation.cluster_by }});
{%- endif -%}

{% if catalog_relation.automatic_clustering and catalog_relation.cluster_by is not none %}
alter iceberg table {{ relation }} resume recluster;
{%- endif -%}

{%- endmacro %}


{% macro snowflake__create_table_iceberg_rest_sql(relation, compiled_code) -%}
{#-
    Implements CREATE ICEBERG TABLE for Iceberg REST catalogs with Catalog Linked Databases.
    https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-rest

    Limitations:
    - Iceberg does not support temporary tables (use a standard Snowflake table)
    - Iceberg REST does not support CREATE OR REPLACE
    - Iceberg catalogs do not support table renaming operations
    - For existing tables, we must DROP the table first before creating the new one

    Note: Iceberg REST writes only work with Catalog Linked Databases (CLD).
-#}

{%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}

{%- set copy_grants = config.get('copy_grants', default=false) -%}

{%- set row_access_policy = config.get('row_access_policy', default=none) -%}
{%- set table_tag = config.get('table_tag', default=none) -%}

{%- set contract_config = config.get('contract') -%}
{%- if contract_config.enforced -%}
    {{- get_assert_columns_equivalent(compiled_code) -}}
    {%- set compiled_code = get_select_subquery(compiled_code) -%}
{%- endif -%}

{%- set sql_header = config.get('sql_header', none) -%}
{{ sql_header if sql_header is not none }}

{# Check if relation exists #}
{% set existing_relation = adapter.get_relation(database=relation.database, schema=relation.schema, identifier=relation.identifier) %}

{% if existing_relation %}
    {# Iceberg catalogs don't support table renaming, so we must drop first #}
    {# This is less safe but the only option for Iceberg REST catalogs #}
    drop table if exists {{ existing_relation }};

{% endif %}

{#- All Iceberg REST writes use CLD and support CTAS (except Glue, handled in table.sql) -#}
create iceberg table {{ relation }}
    {%- if contract_config.enforced %}
    {{ get_table_columns_and_constraints() }}
    {%- endif %}
    {{ optional('external_volume', catalog_relation.external_volume, "'") }}
    {{ optional('target_file_size', catalog_relation.target_file_size, "'") }}
    {{ optional('auto_refresh', catalog_relation.auto_refresh) }}
    {{ optional('max_data_extension_time_in_days', catalog_relation.max_data_extension_time_in_days)}}
    {% if row_access_policy -%} with row access policy {{ row_access_policy }} {%- endif %}
    {% if table_tag -%} with tag ({{ table_tag }}) {%- endif %}
    {% if copy_grants -%} copy grants {%- endif %}
as (
    {{ compiled_code }}
);

{%- endmacro %}


{% macro snowflake__create_table_iceberg_rest_with_glue(relation, compiled_code, catalog_relation) -%}
{#-
    Creates an Iceberg table for Catalog Linked Databases (e.g., AWS Glue) with explicit column definitions.
    This is used when CTAS is not supported.

    This macro is specifically for CLD where we need to create the table with an explicit schema
    because CTAS is not available.
-#}

{# Step 1: Get the schema from the compiled query #}
{% set sql_columns = get_column_schema_from_query(compiled_code) %}

{# Step 2: Create the iceberg table in the CLD with explicit column definitions #}

{%- set copy_grants = config.get('copy_grants', default=false) -%}
{%- set row_access_policy = config.get('row_access_policy', default=none) -%}
{%- set table_tag = config.get('table_tag', default=none) -%}

{%- set sql_header = config.get('sql_header', none) -%}
{{ sql_header if sql_header is not none }}

{# Step 2a: Check if relation exists and drop if necessary (CLD doesn't support CREATE OR REPLACE) #}
{% set existing_relation = adapter.get_relation(database=relation.database, schema=relation.schema, identifier=relation.identifier) %}
{% if existing_relation %}
    drop table if exists {{ existing_relation }};
{% endif %}

{# Step 2b: Create the table with explicit column definitions #}
create iceberg table {{ relation }} (
    {%- for column in sql_columns -%}
        {% if column.data_type == "FIXED" %}
            {%- set data_type = "INT" -%}
        {% elif "character varying" in column.data_type %}
            {%- set data_type = "STRING" -%}
        {% else %}
            {%- set data_type = column.data_type -%}
        {% endif %}
        {{ adapter.quote(column.name.lower()) }} {{ data_type }}
        {%- if not loop.last %}, {% endif -%}
    {% endfor -%}
)
{{ optional('external_volume', catalog_relation.external_volume, "'") }}
{{ optional('target_file_size', catalog_relation.target_file_size, "'") }}
{{ optional('auto_refresh', catalog_relation.auto_refresh) }}
{{ optional('max_data_extension_time_in_days', catalog_relation.max_data_extension_time_in_days)}}
{% if row_access_policy -%} with row access policy {{ row_access_policy }} {%- endif %}
{% if table_tag -%} with tag ({{ table_tag }}) {%- endif %}
{% if copy_grants -%} copy grants {%- endif %}
;

{# Step 3: Insert data from the view (in regular DB) into the table (in CLD) #}
insert into {{ relation }}
    {{ compiled_code }};

{%- endmacro %}


{% macro py_write_table(compiled_code, target_relation) %}

{%- set catalog_relation = adapter.build_catalog_relation(config.model) -%}

{% if catalog_relation.is_transient %}
    {%- set table_type='transient' -%}
{% endif %}

{{ compiled_code }}


def materialize(session, df, target_relation):
    # make sure pandas exists
    import importlib.util
    package_name = 'pandas'
    if importlib.util.find_spec(package_name):
        import pandas
        if isinstance(df, pandas.core.frame.DataFrame):
            session.use_database(target_relation.database)
            session.use_schema(target_relation.schema)
            # session.write_pandas does not have overwrite function
            df = session.createDataFrame(df)
    {% set target_relation_name = resolve_model_name(target_relation) %}
    df.write.mode("overwrite").save_as_table('{{ target_relation_name }}', table_type='{{table_type}}')


def main(session):
    dbt = dbtObj(session.table)
    df = model(dbt, session)
    materialize(session, df, dbt.this)
    return "OK"

{% endmacro %}
