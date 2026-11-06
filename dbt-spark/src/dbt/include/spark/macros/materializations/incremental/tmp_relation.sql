{% macro dbt_spark_get_incremental_tmp_relation(target_relation, language) %}
  {%- set suffix = '__dbt_tmp' -%}
  {#-- Microbatch batches can run concurrently (MicrobatchConcurrency), so each batch
       needs its own temp relation; otherwise batches sharing a Spark session would
       clobber each other's temp view and overwrite partitions with another batch's rows. --#}
  {%- if model.batch -%}
    {%- set suffix = suffix ~ '_' ~ model.batch.id -%}
  {%- endif -%}
  {%- set tmp_relation = target_relation.incorporate(path={"identifier": target_relation.identifier ~ suffix}) -%}

  {#-- for SQL model we will create temp view that doesn't have database and schema --#}
  {%- if language == 'sql' -%}
    {%- set tmp_relation = tmp_relation.include(database=false, schema=false) -%}
  {%- endif -%}

  {% do return(tmp_relation) %}
{% endmacro %}
