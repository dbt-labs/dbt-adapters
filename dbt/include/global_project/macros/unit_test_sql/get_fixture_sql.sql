{% macro get_fixture_sql(rows, column_name_to_data_types) %}
-- Fixture for {{ model.name }}
{% set default_row = {} %}

{%- if not column_name_to_data_types -%}
{%-   set columns_in_relation = adapter.get_columns_in_relation(this) -%}
{%-   set column_name_to_data_types = {} -%}
{%-   for column in columns_in_relation -%}
{#-- This needs to be a case-insensitive comparison --#}
{%-     do column_name_to_data_types.update({column.name|lower: column.data_type}) -%}
{%-   endfor -%}
{%- endif -%}

{%- if not column_name_to_data_types -%}
    {{ exceptions.raise_compiler_error("Not able to get columns for unit test '" ~ model.name ~ "' from relation " ~ this) }}
{%- endif -%}

{%- for column_name, column_type in column_name_to_data_types.items() -%}
    {%- do default_row.update({column_name: (safe_cast("null", column_type) | trim )}) -%}
{%- endfor -%}


{%- for row in rows -%}
{%-   set formatted_row = format_row(row, column_name_to_data_types) -%}
{%-   set default_row_copy = default_row.copy() -%}
{%-   do default_row_copy.update(formatted_row) -%}
select
{%-   for column_name, column_value in default_row_copy.items() %} {{ column_value }} as {{ column_name }}{% if not loop.last -%}, {%- endif %}
{%-   endfor %}
{%-   if not loop.last %}
union all
{%    endif %}
{%- endfor -%}

{%- if (rows | length) == 0 -%}
    select
    {%- for column_name, column_value in default_row.items() %} {{ column_value }} as {{ column_name }}{% if not loop.last -%},{%- endif %}
    {%- endfor %}
    limit 0
{%- endif -%}
{% endmacro %}


{% macro get_expected_sql(rows, column_name_to_data_types) %}

{%- if (rows | length) == 0 -%}
    select * FROM dbt_internal_unit_test_actual
    limit 0
{%- else -%}
{%- for row in rows -%}
{%- do format_row(row, column_name_to_data_types) -%}
select
{%- for column_name, column_value in row.items() %} {{ column_value }} as {{ column_name }}{% if not loop.last -%}, {%- endif %}
{%- endfor %}
{%- if not loop.last %}
union all
{% endif %}
{%- endfor -%}
{%- endif -%}

{% endmacro %}

{%- macro format_row(row, column_name_to_data_types) -%}
    {% set formatted_row = {} %}
    {%- for column_name, column_value in row.items() -%}
        {#-- generate case-insensitive formatted row --#}
        {% set column_name = column_name|lower %}
        {% set row_update = {column_name: column_value} %}
        
        {#-- wrap yaml strings in quotes, apply cast --#}
        {%- if column_value is string -%}
            {%- set row_update = {column_name: safe_cast(dbt.string_literal(dbt.escape_single_quotes(column_value)), column_name_to_data_types[column_name]) } -%}
        {%- elif column_value is none -%}
            {%- set row_update = {column_name: safe_cast('null', column_name_to_data_types[column_name]) } -%}
        {%- else -%}
            {%- set row_update = {column_name: safe_cast(column_value, column_name_to_data_types[column_name]) } -%}
    {%- endif -%}
    {%- do formatted_row.update(row_update) -%}
    {%- endfor -%}
    {{ return(formatted_row) }}
{%- endmacro -%}
