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


{#--
  Incremental lifecycle: create any partitions the incoming batch needs before the
  strategy DML runs. Auto range/granularity partitions are computed from the staged
  batch; explicit list/hash partitions are static (set at first build) and rely on the
  DEFAULT partition for new values. Idempotent via `create table if not exists`.
--#}
{% macro postgres__create_incremental_missing_partitions(target_relation, temp_relation, partition_config) %}
  {%- set ddl = [] -%}
  {%- if partition_config.method == 'range' and partition_config.granularity -%}
    {%- set partitions = postgres__resolve_partitions(temp_relation, partition_config) -%}
    {%- for partition in partitions -%}
      {%- do ddl.append(postgres__create_partition(target_relation, partition, partition_config.method)) -%}
    {%- endfor -%}
  {%- endif -%}
  {{ return(ddl | join('\n')) }}
{% endmacro %}


{#-- Prepended to every incremental strategy: guard against repartitioning + create missing partitions. --#}
{% macro postgres__partition_ddl_for_incremental(arg_dict) %}
  {%- set partition_config = adapter.parse_partition_by(config.get('partition_by')) -%}
  {%- if partition_config is none -%}
    {{ return('') }}
  {%- endif -%}
  {%- set target_relation = arg_dict['target_relation'] -%}
  {{ postgres__assert_partition_scheme_unchanged(target_relation, partition_config) }}
  {{ return(postgres__create_incremental_missing_partitions(target_relation, arg_dict['temp_relation'], partition_config)) }}
{% endmacro %}


{#-- A partition scheme can't be changed in place; require a full refresh (issue #679). --#}
{% macro postgres__assert_partition_scheme_unchanged(target_relation, partition_config) %}
  {%- set sql -%}
    select pg_get_partkeydef('{{ target_relation.schema }}.{{ target_relation.identifier }}'::regclass) as partkey
  {%- endset -%}
  {%- set existing_key = run_query(sql).columns[0].values()[0] -%}
  {%- if existing_key is not none -%}
    {%- set normalized_existing = existing_key | lower | replace(' ', '') -%}
    {%- set normalized_config = partition_config.render | lower | replace(' ', '') -%}
    {%- if normalized_existing != normalized_config -%}
      {%- do exceptions.raise_compiler_error(
        "partition_by scheme changed from '" ~ existing_key ~ "' to '" ~ partition_config.render
        ~ "'. Postgres cannot repartition in place; run with --full-refresh to rebuild.") -%}
    {%- endif -%}
  {%- endif -%}
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
