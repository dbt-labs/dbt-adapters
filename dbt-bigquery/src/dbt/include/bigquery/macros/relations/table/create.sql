{% macro bigquery__create_table_as(temporary, relation, compiled_code, language='sql') -%}
    {%- if language == 'sql' -%}
        {{ bigquery__create_table_info_schema_sql(temporary, relation, compiled_code }}
    {%- elif language == 'python' -%}
        {#-
            N.B. Python models _can_ write to temp views HOWEVER they use a different session
            and have already expired by the time they need to be used (I.E. in merges for incremental models)

            TODO: Deep dive into spark sessions to see if we can reuse a single session for an entire dbt invocation.
        -#}

        {#- when a user wants to change the schema of an existing relation, they must intentionally drop the table in the dataset -#}
        {%- set old_relation = adapter.get_relation(database=relation.database, schema=relation.schema, identifier=relation.identifier) -%}
        {%- if (old_relation.is_table and (should_full_refresh())) -%}
            {%- do adapter.drop_relation(relation) -%}
        {%- endif -%}

        {%- set submission_method = config.get("submission_method", "serverless") -%}
        {%- if submission_method in ("serverless", "cluster") -%}
            {{ py_write_table(compiled_code=compiled_code, target_relation=relation.quote(database=False, schema=False, identifier=False)) }}
        {%- elif submission_method == "bigframes" -%}
            {{ bigframes_write_table(compiled_code=compiled_code, target_relation=relation.quote(database=False, schema=False, identifier=False)) }}
        {%- else -%}
            {%- do exceptions.raise_compiler_error("bigquery__create_table_as macro didn't get supported dataframe syntax, it got %s" % submission_method) -%}
        {%- endif -%}
    {%- else -%}
        {% do exceptions.raise_compiler_error("bigquery__create_table_as macro didn't get supported language, it got %s" % language) %}
    {%- endif -%}

{%- endmacro -%}


{% macro bigquery__create_table_info_schema_sql(temporary, relation, compiled_code) -%}
    {%- set raw_partition_by = config.get('partition_by', none) -%}
    {%- set raw_cluster_by = config.get('cluster_by', none) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {%- set partition_config = adapter.parse_partition_by(raw_partition_by) -%}
    {%- if partition_config.time_ingestion_partitioning -%}
        {%- set columns = get_columns_with_types_in_query_sql(sql) -%}
        {%- set table_dest_columns_csv = columns_without_partition_fields_csv(partition_config, columns) -%}
        {%- set columns = '(' ~ table_dest_columns_csv ~ ')' -%}
    {%- endif -%}

    {%- set contract_config = config.get('contract') -%}
    {%- if contract_config.enforced -%}
        {{ get_assert_columns_equivalent(compiled_code) }}
        {%- set compiled_code = get_select_subquery(compiled_code) %}
    {% endif %}

{{ sql_header if sql_header is not none }}

create or replace table {{ relation }}
    {%- if contract_config.enforced -%}
    {{ get_table_columns_and_constraints() }}
    {% else %}
    {#-- cannot do contracts at the same time as time ingestion partitioning -#}
    {{ columns }}
    {% endif %}
    {{ partition_by(partition_config) }}
    {{ cluster_by(raw_cluster_by) }}
    {{ bigquery_table_options(config, model, temporary) }}
    {#-- PARTITION BY cannot be used with the AS query_statement clause.
         https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#partition_expression
    -#}
    {%- if not partition_config.time_ingestion_partitioning %}
    as (
        {{ compiled_code }}
    )
    {% endif %}
;

{%- endmacro -%}


{% macro py_write_table(compiled_code, target_relation) %}

{%- set raw_partition_by = config.get('partition_by', none) -%}
{%- set raw_cluster_by = config.get('cluster_by', none) -%}
{%- set enable_list_inference = config.get('enable_list_inference', true) -%}
{%- set intermediate_format = config.get('intermediate_format', none) -%}
{%- set partition_config = adapter.parse_partition_by(raw_partition_by) %}
{#-
    For writeMethod we need to use "indirect" if materializing a partitioned table otherwise we can use "direct".
    Note that indirect will fail if the GCS bucket has a retention policy set on it.
-#}
{%- if partition_config -%}
    {%- set write_method = 'indirect' -%}
{%- else %}
    {%- set write_method = 'direct' -%}
{%- endif -%}

from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('smallTest').getOrCreate()


spark.conf.set("viewsEnabled","true")
spark.conf.set("temporaryGcsBucket","{{target.gcs_bucket}}")
spark.conf.set("enableListInference", "{{ enable_list_inference }}")
{% if intermediate_format -%}
spark.conf.set("intermediateFormat", "{{ intermediate_format }}")
{%- endif %}


{{ compiled_code }}


# COMMAND ----------
# this is materialization code dbt generated, please do not modify
import pyspark
# make sure pandas exists before using it
try:
    import pandas
    pandas_available = True
except ImportError:
    pandas_available = False
# make sure pyspark.pandas exists before using it
try:
    import pyspark.pandas
    pyspark_pandas_api_available = True
except ImportError:
    pyspark_pandas_api_available = False
# make sure databricks.koalas exists before using it
try:
    import databricks.koalas
    koalas_available = True
except ImportError:
    koalas_available = False


dbt = dbtObj(spark.read.format("bigquery").load)
df = model(dbt, spark)


# preferentially convert pandas DataFrames to pandas-on-Spark or Koalas DataFrames first
# since they know how to convert pandas DataFrames better than `spark.createDataFrame(df)`
# and converting from pandas-on-Spark to Spark DataFrame has no overhead
if pyspark_pandas_api_available and pandas_available and isinstance(df, pandas.core.frame.DataFrame):
    df = pyspark.pandas.frame.DataFrame(df)
elif koalas_available and pandas_available and isinstance(df, pandas.core.frame.DataFrame):
    df = databricks.koalas.frame.DataFrame(df)


# convert to pyspark.sql.dataframe.DataFrame
if isinstance(df, pyspark.sql.dataframe.DataFrame):
    pass  # since it is already a Spark DataFrame
elif pyspark_pandas_api_available and isinstance(df, pyspark.pandas.frame.DataFrame):
    df = df.to_spark()
elif koalas_available and isinstance(df, databricks.koalas.frame.DataFrame):
    df = df.to_spark()
elif pandas_available and isinstance(df, pandas.core.frame.DataFrame):
    df = spark.createDataFrame(df)
else:
    msg = f"{type(df)} is not a supported type for dbt Python materialization"
    raise Exception(msg)


df.write \
    .mode("overwrite") \
    .format("bigquery") \
    .option("writeMethod", "{{ write_method }}") \
    .option("writeDisposition", 'WRITE_TRUNCATE') \
    {%- if partition_config is not none %}
    {%- if partition_config.data_type | lower in ('date','timestamp','datetime') %}
    .option("partitionField", "{{- partition_config.field -}}") \
    {%- if partition_config.granularity is not none %}
    .option("partitionType", "{{- partition_config.granularity| upper -}}") \
    {%- endif %}
    {%- endif %}
    {%- endif %}
    {%- if raw_cluster_by is not none %}
    .option("clusteredFields", "{{- raw_cluster_by | join(',') -}}") \
    {%- endif %}
    .save("{{target_relation}}")

{% endmacro %}


{% macro bigframes_write_table(compiled_code, target_relation) %}
import bigframes.pandas as bpd


bpd.options.compute.extra_query_labels["bigframes-dbt-api"] = "python-model-table"
bpd.options.bigquery.project = "{{ target.project }}"
{% if target.location -%}
bpd.options.bigquery.location = "{{ target.location }}"
{%- endif %}


session = bpd.get_global_session()


{{ compiled_code }}


dbt = dbtObj(bpd.read_gbq)
df = model(dbt, session)
df.to_gbq("{{ target_relation }}", if_exists="replace")
df._session.close()

{% endmacro %}
