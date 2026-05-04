{% macro bigquery__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates=none) -%}
    {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set merge_update_columns = config.get('merge_update_columns') -%}
    {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
    {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}
    {%- set sql_header = config.get('sql_header', none) -%}
    {%- set truthy_nulls = adapter.behavior.enable_truthy_nulls_equals_macro.no_warn -%}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
            {% for key in unique_key %}
                {% set this_key_match %}
                    DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
                {% endset %}
                {% do predicates.append(this_key_match) %}
            {% endfor %}
        {% else %}
            {% set source_unique_key = ("DBT_INTERNAL_SOURCE." ~ unique_key) | trim %}
            {% set target_unique_key = ("DBT_INTERNAL_DEST." ~ unique_key) | trim %}
            {% if truthy_nulls %}
                {% set unique_key_match %}
                    (({{ source_unique_key }} is null and {{ target_unique_key }} is null) or ({{ source_unique_key }} = {{ target_unique_key }}))
                {% endset %}
            {% else %}
                {% set unique_key_match %}
                    ({{ source_unique_key }} = {{ target_unique_key }})
                {% endset %}
            {% endif %}
            {% do predicates.append(unique_key_match | trim) %}
        {% endif %}
    {% else %}
        {% do predicates.append('FALSE') %}
    {% endif %}

    {{ sql_header if sql_header is not none }}

    merge into {{ target }} as DBT_INTERNAL_DEST
        using {{ source }} as DBT_INTERNAL_SOURCE
        on {{"(" ~ predicates | join(") and (") ~ ")"}}

    {% if unique_key %}
    when matched then update set
        {% for column_name in update_columns -%}
            {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
    {% endif %}

    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})

{% endmacro %}


{% macro bq_generate_incremental_merge_build_sql(
    tmp_relation, target_relation, sql, unique_key, partition_by, dest_columns, tmp_relation_exists, incremental_predicates
) %}
    {%- set source_sql -%}
        {%- if tmp_relation_exists -%}
        (
        select
        {% if partition_by.time_ingestion_partitioning -%}
        {{ partition_by.insertable_time_partitioning_field() }},
        {%- endif -%}
        * from {{ tmp_relation }}
        )
        {%- else -%} {#-- wrap sql in parens to make it a subquery --#}
        (
            {%- if partition_by.time_ingestion_partitioning -%}
            {{ wrap_with_time_ingestion_partitioning_sql(partition_by, sql, True) }}
            {%- else -%}
            {{sql}}
            {%- endif %}
        )
        {%- endif -%}
    {%- endset -%}

    {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
    {%- set avoid_require_partition_filter = predicate_for_avoid_require_partition_filter() -%}
    {%- if avoid_require_partition_filter is not none -%}
        {% do predicates.append(avoid_require_partition_filter) %}
    {%- endif -%}

    {% set build_sql = get_merge_sql(target_relation, source_sql, unique_key, dest_columns, predicates) %}

    {{ return(build_sql) }}

{% endmacro %}
