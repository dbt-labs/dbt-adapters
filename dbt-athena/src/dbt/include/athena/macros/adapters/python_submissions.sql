{%- macro athena__py_save_table_as(compiled_code, target_relation, optional_args={}) -%}
    {% set location = optional_args.get("location") %}
    {% set format = optional_args.get("format", "parquet") %}
    {% set mode = optional_args.get("mode", "overwrite") %}
    {% set write_compression = optional_args.get("write_compression", "snappy") %}
    {% set partitioned_by = optional_args.get("partitioned_by") %}
    {% set bucketed_by = optional_args.get("bucketed_by") %}
    {% set sorted_by = optional_args.get("sorted_by") %}
    {% set merge_schema = optional_args.get("merge_schema", true) %}
    {% set bucket_count = optional_args.get("bucket_count") %}
    {% set field_delimiter = optional_args.get("field_delimiter") %}
    {% set extra_table_properties = optional_args.get("extra_table_properties") %}
    {% set use_iceberg_write_to = optional_args.get("use_iceberg_write_to", false) %}
    {% set spark_ctas = optional_args.get("spark_ctas", "") %}

import pyspark


{{ compiled_code }}
def materialize(spark_session, df, target_relation):
    import pandas
    if isinstance(df, pyspark.sql.dataframe.DataFrame):
        pass
    elif isinstance(df, pandas.core.frame.DataFrame):
        df = spark_session.createDataFrame(df)
    else:
        msg = f"{type(df)} is not a supported type for dbt Python materialization"
        raise Exception(msg)

{% if use_iceberg_write_to %}
    import re
    from pyspark.sql import functions as F

    def _parse_iceberg_partition(expr_str):
        expr_str = expr_str.strip()
        m = re.match(r"(\w+)\((.+)\)", expr_str)
        if not m:
            return F.col(expr_str)
        func = m.group(1).lower()
        args = [a.strip() for a in m.group(2).split(",")]
        if func in ("day", "days"):
            return F.days(F.col(args[0]))
        if func in ("month", "months"):
            return F.months(F.col(args[0]))
        if func in ("year", "years"):
            return F.years(F.col(args[0]))
        if func in ("hour", "hours"):
            return F.hours(F.col(args[0]))
        if func in ("bucket", "truncate"):
            if len(args) != 2:
                raise ValueError(
                    f"Iceberg partition transform '{func}' requires 2 arguments (column, n), got: {expr_str}"
                )
            n = int(args[1])
            return F.bucket(n, F.col(args[0])) if func == "bucket" else F.truncate(n, F.col(args[0]))
        raise ValueError(f"Unknown Iceberg partition transform: {func}")

    _writer = df.writeTo("{{ target_relation.schema | replace('\"', '`') }}.{{ target_relation.identifier | replace('\"', '`') }}")
    _writer = _writer.using("iceberg")
    _writer = _writer.tableProperty("location", {{ (location ~ "/") | tojson }})
    {% if extra_table_properties is not none %}
    {% for prop_name, prop_value in extra_table_properties.items() %}
    _writer = _writer.tableProperty({{ prop_name | tojson }}, {{ prop_value | string | tojson }})
    {% endfor %}
    {% endif %}
    {% if partitioned_by is not none %}
    _writer = _writer.partitionedBy(
        {%- for part_expr in partitioned_by %}
        _parse_iceberg_partition({{ part_expr | tojson }}){{ "," if not loop.last }}
        {%- endfor %}
    )
    {% endif %}
    _writer.createOrReplace()
{% elif spark_ctas|length > 0 %}
    df.createOrReplaceTempView("{{ target_relation.schema}}_{{ target_relation.identifier }}_tmpvw")
    spark_session.sql("""
    {{ spark_ctas }}
    select * from {{ target_relation.schema}}_{{ target_relation.identifier }}_tmpvw
    """)
{% else %}
    writer = df.write \
    .format("{{ format }}") \
    .mode("{{ mode }}") \
    .option("path", "{{ location }}") \
    .option("compression", "{{ write_compression }}") \
    .option("mergeSchema", "{{ merge_schema }}") \
    .option("delimiter", "{{ field_delimiter }}")
    if {{ partitioned_by }} is not None:
        writer = writer.partitionBy({{ partitioned_by }})
    if {{ bucketed_by }} is not None:
        writer = writer.bucketBy({{ bucket_count }},{{ bucketed_by }})
    if {{ sorted_by }} is not None:
        writer = writer.sortBy({{ sorted_by }})

    writer.saveAsTable(
        name="{{ target_relation.schema}}.{{ target_relation.identifier }}",
    )
{% endif %}

    return "Success: {{ target_relation.schema}}.{{ target_relation.identifier }}"

{{ athena__py_get_spark_dbt_object() }}

dbt = SparkdbtObj()
df = model(dbt, spark)
materialize(spark, df, dbt.this)
{%- endmacro -%}

{%- macro athena__py_execute_query(query) -%}
{{ athena__py_get_spark_dbt_object() }}

def execute_query(spark_session):
    spark_session.sql("""{{ query }}""")
    return "OK"

dbt = SparkdbtObj()
execute_query(spark)
{%- endmacro -%}

{%- macro athena__py_get_spark_dbt_object() -%}
def get_spark_df(identifier):
    """
    Override the arguments to ref and source dynamically

    spark.table('awsdatacatalog.analytics_dev.model')
    Raises pyspark.sql.utils.AnalysisException:
    spark_catalog requires a single-part namespace,
    but got [awsdatacatalog, analytics_dev]

    So the override removes the catalog component and only
    provides the schema and identifer to spark.table()
    """
    return spark.table(".".join(identifier.split(".")[1:]).replace('"', ''))

class SparkdbtObj(dbtObj):
    def __init__(self):
        super().__init__(load_df_function=get_spark_df)
        self.source = lambda *args: source(*args, dbt_load_df_function=get_spark_df)
        self.ref = lambda *args: ref(*args, dbt_load_df_function=get_spark_df)

{%- endmacro -%}
