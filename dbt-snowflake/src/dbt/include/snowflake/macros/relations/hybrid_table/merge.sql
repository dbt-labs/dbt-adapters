{% macro snowflake__get_merge_sql_hybrid_table(target_relation, source_sql, unique_key, dest_columns, predicates=none) -%}
{#-
    Generate a MERGE statement for incremental updates to a hybrid table

    Args:
    - target_relation: the target hybrid table
    - source_sql: the SQL that generates the source data
    - unique_key: the columns to use for matching (typically the primary key)
    - dest_columns: the columns to update
    - predicates: optional additional predicates for the merge
-#}

    {%- set primary_key = config.get('primary_key', []) -%}
    {%- set merge_update_columns = config.get('merge_update_columns') -%}

    {#- Handle primary_key as string or list -#}
    {%- if primary_key is string -%}
        {%- set primary_key = [primary_key] -%}
    {%- endif -%}

    {#- Use primary_key for merge condition -#}
    {%- set merge_key = primary_key -%}

    {#- Build the merge condition -#}
    {%- set merge_condition -%}
        {%- for key in merge_key %}
        target.{{ key }} = source.{{ key }}
        {%- if not loop.last %} and {% endif %}
        {%- endfor %}
    {%- endset -%}

    {#- Determine which columns to update -#}
    {%- if merge_update_columns -%}
        {%- set update_columns = merge_update_columns -%}
    {%- else -%}
        {#- Update all columns except primary key (case-insensitive comparison) -#}
        {%- set merge_key_lower = [] -%}
        {%- for k in merge_key -%}
            {%- do merge_key_lower.append(k | lower) -%}
        {%- endfor -%}
        {%- set update_columns = [] -%}
        {%- for c in dest_columns -%}
            {%- if (c | lower) not in merge_key_lower -%}
                {%- do update_columns.append(c) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}

    merge into {{ target_relation }} as target
    using (
        select
            {%- for column in dest_columns %}
            {{ column | lower }} as {{ column }}{%- if not loop.last %}, {% endif %}
            {%- endfor %}
        from (
            {{ source_sql }}
        ) as src
    ) as source
    on {{ merge_condition }}

    {%- if predicates %}
        {% for predicate in predicates %}
        and {{ predicate }}
        {% endfor %}
    {%- endif %}

    when matched then update set
        {%- for column in update_columns %}
        target.{{ column }} = source.{{ column }}
        {%- if not loop.last %},{% endif %}
        {%- endfor %}

    when not matched then insert (
        {%- for column in dest_columns %}
        {{ column }}
        {%- if not loop.last %},{% endif %}
        {%- endfor %}
    )
    values (
        {%- for column in dest_columns %}
        source.{{ column }}
        {%- if not loop.last %},{% endif %}
        {%- endfor %}
    )

{%- endmacro %}
