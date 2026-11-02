{# Athena has different types between DML and DDL #}
{# ref: https://docs.aws.amazon.com/athena/latest/ug/data-types.html #}
{% macro ddl_data_type(col_type, table_type = 'hive') -%}
  -- transform varchar
  {% set re = modules.re %}
  {% set data_type = re.sub('(?:varchar|character varying)(?:\(\d+\))?', 'string', col_type) %}

  -- transform array and map
  {%- if 'array' in data_type or 'map' in data_type -%}
    {#- Only an array or map's own parentheses become <>, so nested decimal(38,0) keeps its own. -#}
    {%- set ns = namespace(out='', stack='') -%}
    {%- for char in data_type -%}
      {%- if char == '(' -%}
        {%- if re.search('(?:^|[^a-z0-9_])(?:array|map)$', ns.out) -%}
          {%- set ns.stack = ns.stack ~ '>' -%}
          {%- set ns.out = ns.out ~ '<' -%}
        {%- else -%}
          {%- set ns.stack = ns.stack ~ ')' -%}
          {%- set ns.out = ns.out ~ '(' -%}
        {%- endif -%}
      {%- elif char == ')' and ns.stack -%}
        {%- set ns.out = ns.out ~ ns.stack[-1] -%}
        {%- set ns.stack = ns.stack[:-1] -%}
      {%- else -%}
        {%- set ns.out = ns.out ~ char -%}
      {%- endif -%}
    {%- endfor -%}
    {%- set data_type = ns.out -%}
  {%- endif -%}

  -- transform int
  {%- if 'integer' in data_type -%}
    {% set data_type = data_type.replace('integer', 'int') -%}
  {%- endif -%}

  -- transform timestamp
  {%- if table_type == 'iceberg' -%}
    {%- if 'timestamp' in data_type -%}
        {% set data_type = 'timestamp' -%}
    {%- endif -%}

    {%- if 'binary' in data_type -%}
        {% set data_type = 'binary' -%}
    {%- endif -%}
  {%- endif -%}

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
