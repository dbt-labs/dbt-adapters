{% macro bigquery__can_clone_table() %}
    {{ return(True) }}
{% endmacro %}

{#
  Check if a target relation can be replaced by cloning from a source relation.
  BigQuery requires partition and clustering specs to match when using CREATE OR REPLACE.
  Returns True if the target can be replaced directly (or doesn't exist),
  False if it needs to be dropped first.
#}
{% macro bigquery__is_clone_replaceable(target_relation, source_relation) %}
    {%- if not target_relation -%}
        {{ return(True) }}
    {%- else -%}
        {%- set target_table = adapter.get_bq_table(target_relation) -%}
        {%- set source_table = adapter.get_bq_table(source_relation) -%}

        {%- if not target_table or not source_table -%}
            {{ return(True) }}
        {%- else -%}
            {# Check if partition specs match #}
            {%- set target_is_partitioned = true if (target_table.range_partitioning or target_table.time_partitioning) else false -%}
            {%- set source_is_partitioned = true if (source_table.range_partitioning or source_table.time_partitioning) else false -%}

            {%- set is_replaceable = true -%}

            {# If one is partitioned and the other isn't, they don't match #}
            {%- if target_is_partitioned != source_is_partitioned -%}
                {%- set is_replaceable = false -%}
            {%- endif -%}

            {# Check time partitioning match #}
            {%- if is_replaceable and target_table.time_partitioning and source_table.time_partitioning -%}
                {%- set target_field = target_table.time_partitioning.field.lower() if target_table.time_partitioning.field else none -%}
                {%- set source_field = source_table.time_partitioning.field.lower() if source_table.time_partitioning.field else none -%}
                {%- set target_type = target_table.partitioning_type -%}
                {%- set source_type = source_table.partitioning_type -%}

                {%- if target_field != source_field or target_type != source_type -%}
                    {%- set is_replaceable = false -%}
                {%- endif -%}
            {%- endif -%}

            {# Check range partitioning match #}
            {%- if is_replaceable and target_table.range_partitioning and source_table.range_partitioning -%}
                {%- set target_range = target_table.range_partitioning -%}
                {%- set source_range = source_table.range_partitioning -%}

                {%- if target_range.field != source_range.field or
                       target_range.range_.start != source_range.range_.start or
                       target_range.range_.end != source_range.range_.end or
                       target_range.range_.interval != source_range.range_.interval -%}
                    {%- set is_replaceable = false -%}
                {%- endif -%}
            {%- endif -%}

            {# Check clustering fields match #}
            {%- if is_replaceable and target_table.clustering_fields != source_table.clustering_fields -%}
                {%- set is_replaceable = false -%}
            {%- endif -%}

            {{ return(is_replaceable) }}
        {%- endif -%}
    {%- endif -%}
{% endmacro %}

{% macro bigquery__create_or_replace_clone(this_relation, defer_relation) %}
    create or replace
      table {{ this_relation }}
      clone {{ defer_relation }};
{% endmacro %}
