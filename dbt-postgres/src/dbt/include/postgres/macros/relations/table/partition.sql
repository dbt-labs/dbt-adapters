{#
  Native PostgreSQL declarative partitioning (issue #679).

  Postgres does not allow `CREATE TABLE ... AS SELECT` together with `PARTITION BY`,
  so a partitioned table is built in stages:
    1. stage the model results in a temp table (gives us column types + the data)
    2. create the partitioned parent with `CREATE TABLE (LIKE stage) PARTITION BY ...`
    3. create the child partitions (+ optional DEFAULT)
    4. `INSERT INTO parent SELECT * FROM stage`
#}

{% macro postgres__create_partitioned_table_as(temporary, relation, sql, partition_config) -%}
  {%- set unlogged = config.get('unlogged', default=false) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  {#-- 1. stage the model results (temp table lives on this connection) --#}
  {%- set stage_relation = make_temp_relation(relation, '__dbt_pstage') -%}
  {% call statement('stage_partition_data') -%}
    create temporary table {{ stage_relation }} as (
      {{ sql }}
    )
  {%- endcall %}

  {#-- 2. resolve which partitions to create (explicit, or auto from the staged data) --#}
  {%- set partitions = postgres__resolve_partitions(stage_relation, partition_config) -%}

  {#-- 3. build the DDL that becomes the `main` statement --#}
  create {% if temporary -%} temporary {%- elif unlogged -%} unlogged {%- endif %} table {{ relation }} (
    like {{ stage_relation }} including defaults
  ) partition by {{ partition_config.render }};

  {% for partition in partitions %}
    {{ postgres__create_partition(relation, partition, partition_config.method) }}
  {% endfor %}

  {#-- hash partitioning does not support a DEFAULT partition --#}
  {% if partition_config.default_partition and partition_config.method != 'hash' %}
    {{ postgres__create_default_partition(relation) }}
  {% endif %}

  insert into {{ relation }} select * from {{ stage_relation }};
{%- endmacro %}


{#-- Resolve the list of partition specs for a build. --#}
{% macro postgres__resolve_partitions(stage_relation, partition_config) %}
  {%- if partition_config.partitions -%}
    {{ return(partition_config.partitions) }}
  {%- elif partition_config.method == 'range' and partition_config.granularity -%}
    {%- set field = partition_config.fields[0] -%}
    {%- set result = run_query('select min(' ~ field ~ '), max(' ~ field ~ ') from ' ~ stage_relation) -%}
    {%- set minimum = result.columns[0].values()[0] -%}
    {%- set maximum = result.columns[1].values()[0] -%}
    {{ return(adapter.get_partition_bounds(minimum, maximum, partition_config.granularity)) }}
  {%- else -%}
    {{ return([]) }}
  {%- endif -%}
{% endmacro %}


{#-- Name a child partition after its parent, e.g. `mymodel` + `p202401` -> `mymodel__p202401`. --#}
{% macro postgres__partition_name(parent_relation, suffix) %}
  {%- set identifier = parent_relation.identifier ~ '__' ~ suffix -%}
  {%- if identifier | length > parent_relation.relation_max_name_length() -%}
    {%- do exceptions.raise_compiler_error(
      "Partition name '" ~ identifier ~ "' exceeds the " ~ parent_relation.relation_max_name_length()
      ~ " character limit. Shorten the model name or the partition names.") -%}
  {%- endif -%}
  {{ return(parent_relation.incorporate(path={"identifier": identifier})) }}
{% endmacro %}


{% macro postgres__get_partition_values_clause(partition, method) %}
  {%- if method == 'range' -%}
    for values from ({{ partition['from'] }}) to ({{ partition['to'] }})
  {%- elif method == 'list' -%}
    for values in ({{ partition['values'] | join(', ') }})
  {%- elif method == 'hash' -%}
    for values with (modulus {{ partition['modulus'] }}, remainder {{ partition['remainder'] }})
  {%- endif -%}
{% endmacro %}


{% macro postgres__create_partition(parent_relation, partition, method) %}
  {%- set child = postgres__partition_name(parent_relation, partition['name']) -%}
  create table if not exists {{ child }}
    partition of {{ parent_relation }}
    {{ postgres__get_partition_values_clause(partition, method) }};
{% endmacro %}


{% macro postgres__create_default_partition(parent_relation) %}
  {%- set child = postgres__partition_name(parent_relation, 'default') -%}
  create table if not exists {{ child }} partition of {{ parent_relation }} default;
{% endmacro %}


{#-- Names of the child partitions attached to `relation`. --#}
{% macro postgres__get_partition_children(relation) %}
  {% set sql -%}
    select c.relname as name
    from pg_inherits i
    join pg_class c on c.oid = i.inhrelid
    join pg_class p on p.oid = i.inhparent
    join pg_namespace n on n.oid = p.relnamespace
    where p.relname = '{{ relation.identifier }}'
      and n.nspname = '{{ relation.schema }}'
    order by c.relname
  {%- endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}
