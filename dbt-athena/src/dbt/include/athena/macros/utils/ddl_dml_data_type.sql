{# Athena has different types between DML and DDL #}
{# ref: https://docs.aws.amazon.com/athena/latest/ug/data-types.html #}
{% macro ddl_data_type(col_type, table_type = 'hive') -%}
  {#- Complex types nest field names (struct<event_timestamp:string>), so each rule
      replaces a whole type word and skips field names, which are followed by ':'. -#}
  -- transform varchar
  {% set re = modules.re %}
  {% set data_type = re.sub('\\b(?:varchar|character varying)\\b(?:\\(\\d+\\))?(?!:)', 'string', col_type) %}

  -- transform timestamp
  {%- if table_type == 'iceberg' -%}
    {%- set data_type = re.sub('\\btimestamp\\b(?:\\(\\d+\\))?(?: with time zone)?(?!:)', 'timestamp', data_type) -%}
    {%- set data_type = re.sub('\\bvarbinary\\b(?!:)', 'binary', data_type) -%}
  {%- endif -%}

  -- transform array and map
  {%- if 'array' in data_type or 'map' in data_type -%}
    {% set data_type = data_type.replace('(', '<').replace(')', '>') -%}
  {%- endif -%}

  -- transform int
  {%- set data_type = re.sub('\\binteger\\b(?!:)', 'int', data_type) -%}

  {{ return(data_type) }}
{% endmacro %}

{% macro dml_data_type(col_type) -%}
  {%- set re = modules.re -%}
  -- transform int to integer
  {%- set data_type = re.sub('\bint\b', 'integer', col_type) -%}
  -- transform string to varchar because string does not work in DML
  {%- set data_type = re.sub('string', 'varchar', data_type) -%}
  -- transform float to real because float does not work in DML
  {%- set data_type = re.sub('float', 'real', data_type) -%}
  {{ return(data_type) }}
{% endmacro %}
