{% macro declare_dbt_max_partition(relation, partition_by, compiled_code, language='sql') %}

  {#-- TODO: revisit partitioning with python models --#}
  {%- if language != 'sql' -%}
    {{ return('') }}
  {%- endif -%}

  {#-
    Ignore mentions in comments/quoted literals when deciding whether to declare
    _dbt_max_partition.
  -#}
  {%- set compiled_code_without_comments = modules.re.sub("(?s)/\\*.*?\\*/|--[^\\n\\r]*", " ", compiled_code) -%}
  {%- set compiled_code_without_comments_or_strings = modules.re.sub("'(?:''|[^'])*'", " ", compiled_code_without_comments) -%}
  {%- set uses_dbt_max_partition = modules.re.search("(^|[^A-Za-z0-9_])_dbt_max_partition([^A-Za-z0-9_]|$)", compiled_code_without_comments_or_strings) is not none -%}
  {%- set relation_exists = load_relation(relation) is not none -%}

  {%- if uses_dbt_max_partition and relation_exists -%}

    declare _dbt_max_partition {{ partition_by.data_type_for_partition() }} default (
      select max({{ partition_by.field }}) from {{ this }}
      where {{ partition_by.field }} is not null
    );

  {%- endif -%}

{% endmacro %}

{% macro predicate_for_avoid_require_partition_filter(target='DBT_INTERNAL_DEST') %}

    {%- set raw_partition_by = config.get('partition_by', none) -%}
    {%- set partition_config = adapter.parse_partition_by(raw_partition_by) -%}
    {%- set predicate = none -%}

    {% if partition_config and config.get('require_partition_filter') -%}
        {%- set partition_field = partition_config.time_partitioning_field() if partition_config.time_ingestion_partitioning else partition_config.field -%}
        {% set predicate %}
            (
                `{{ target }}`.`{{ partition_field }}` is null
                or `{{ target }}`.`{{ partition_field }}` is not null
            )
        {% endset %}
    {%- endif -%}

    {{ return(predicate) }}

{% endmacro %}
