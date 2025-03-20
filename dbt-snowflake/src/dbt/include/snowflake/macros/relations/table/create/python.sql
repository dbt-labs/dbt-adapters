{% macro py_write_table(compiled_code, target_relation, temporary=False, table_type=none) %}
{#- The following logic is only for backwards-compatiblity with deprecated `temporary` parameter -#}
{% if table_type is not none %}
    {#- Just use the table_type as-is -#}
{% elif temporary -%}
    {#- Case 1 when the deprecated `temporary` parameter is used without the replacement `table_type` parameter -#}
    {%- set table_type = "temporary" -%}
{% else %}
    {#- Case 2 when the deprecated `temporary` parameter is used without the replacement `table_type` parameter -#}
    {#- Snowflake treats "" as meaning "permanent" -#}
    {%- set table_type = "" -%}
{%- endif %}
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
