{%- macro redshift__py_save_table_as(temporary, target_relation, compiled_code, optional_args={}) -%}
    {%- set submission_method = config.get('submission_method', default='emr_serverless') -%}
    {%- set s3_uri = config.get('s3_uri', default=target.s3_uri) -%}
    {%- set host = config.get('host', default=target.host) -%}
    {%- set port = config.get('port', default=target.port) -%}
    {%- set dbname = config.get('dbname', default=target.dbname) -%}

{%- set table_schema  = target_relation.schema  %}
{%- set table_name  = target_relation.identifier  %}

{% if temporary %}
    {%- set table  = table_name  %}
{% else %}
    {%- set table  = table_schema~'.'~table_name  %}
{% endif %}

{%- set url -%}
jdbc:redshift://{{ host }}:{{ port }}/{{ dbname }}?user={{ target.user }}
{%- endset -%}

def get_url(url):
    import os
    db_password = os.getenv("MDATA_DB_PASSWORD")
    return f"{url}&password={db_password}"

{{-"\n"-}}
import pyspark

{% if submission_method == "emr_serverless" -%}

{{-"\n"-}}
spark = pyspark.sql.SparkSession.builder.appName("dbt_{{ table_schema  }}_{{ table_name }}").getOrCreate()
{%- endif -%}

{{-"\n"-}}
{{ compiled_code }}

{{ redshift__py_write_spark_df(table, url, s3_uri) }}

{{ redshift__py_read_spark_df(table, url, s3_uri) }}

dbt = SparkdbtObj()
df = model(dbt, spark)
materialize(spark, df, dbt.this)
{%- endmacro -%}

{%- macro redshift__py_execute_query(query) -%}
{{-"\n"-}}
def execute_query(spark_session):
    spark_session.sql("""
    {{ query }}
    """)
    return "OK"

execute_query(spark)
{%- endmacro -%}

{%- macro redshift__py_write_spark_df(table, url, s3_uri) -%}

def materialize(spark_session, df, target_relation):
    import pandas
    if isinstance(df, pyspark.sql.dataframe.DataFrame):
        pass
    elif isinstance(df, pandas.core.frame.DataFrame):
        df = spark_session.createDataFrame(df)
    else:
        msg = f"{type(df)} is not a supported type for dbt Python materialization"
        raise Exception(msg)
    url = get_url("{{ url }}")

    df.write \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", url) \
        .option("dbtable", "{{ table }}") \
        .option("tempdir", "{{ s3_uri }}") \
        .option("forward_spark_s3_credentials", "true") \
        .option("tempformat", "PARQUET") \
        .option("unload_s3_format", "PARQUET") \
        .mode("overwrite") \
        .save()

    return "Success: {{ table }}"

{%- endmacro -%}

{%- macro redshift__py_read_spark_df(table, url, s3_uri) -%}
{{-"\n"-}}
import re

def get_spark_df(identifier):
    """
    Override the arguments to ref and source dynamically
    """

    db_name = identifier.split(".")[0].replace('"', '')
    read_table = ".".join(identifier.split(".")[1:]).replace('"', '')

    url = "{{ url }}"
    pattern = r"(//[^:/]+:\d+/)([^?]+)"
    url = re.sub(pattern, rf"\1{db_name}", url)
    url = get_url(url)

    df_read = spark.read \
        .format("io.github.spark_redshift_community.spark.redshift") \
        .option("url", url) \
        .option("dbtable", read_table) \
        .option("tempdir", "{{ s3_uri }}") \
        .option("tempformat", "PARQUET") \
        .option("unload_s3_format", "PARQUET") \
        .option("forward_spark_s3_credentials", "true") \
        .load()
    return df_read

class SparkdbtObj(dbtObj):
    def __init__(self):
        super().__init__(load_df_function=get_spark_df)
        self.source = lambda *args: source(*args, dbt_load_df_function=get_spark_df)
        self.ref = lambda *args: ref(*args, dbt_load_df_function=get_spark_df)

{%- endmacro -%}
