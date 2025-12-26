{% macro bigquery__create_search_index(relation, search_index_config) %}
    {%- set analyzer = search_index_config.analyzer -%}
    {%- set analyzer_options = search_index_config.analyzer_options -%}
    {%- set data_types = search_index_config.data_types -%}
    {%- set default_index_column_granularity = search_index_config.default_index_column_granularity -%}
    {%- set column_options = search_index_config.column_options -%}
    {%- set columns = search_index_config.columns -%}
    {%- set name = search_index_config.name or (relation.identifier ~ '_search_index') -%}

    create search index if not exists `{{ name }}`
    on {{ relation }}
    (
        {%- if "ALL COLUMNS" in columns -%}
            all columns
            {%- if column_options -%}
                with column options(
                    {%- for col, opts in column_options.items() -%}
                        {{ col }} options(
                            {%- for opt_name, opt_val in opts.items() -%}
                                {{ opt_name }} = '{{ opt_val }}'
                                {%- if not loop.last -%}, {% endif -%}
                            {%- endfor -%}
                        )
                        {%- if not loop.last -%}, {% endif -%}
                    {%- endfor -%}
                )
            {%- endif -%}
        {%- else -%}
            {%- for col in columns -%}
                {{ col }}
                {%- if col in column_options -%}
                    options(
                        {%- for opt_name, opt_val in column_options[col].items() -%}
                            {{ opt_name }} = '{{ opt_val }}'
                            {%- if not loop.last -%}, {% endif -%}
                        {%- endfor -%}
                    )
                {%- endif -%}
                {%- if not loop.last -%}, {% endif -%}
            {%- endfor -%}
        {%- endif -%}
    )
    options (
        analyzer = '{{ analyzer }}'
        {%- if analyzer_options -%}, analyzer_options = '{{ analyzer_options }}'{%- endif -%}
        {%- if data_types -%}, data_types = [
            {%- for dt in data_types -%}
                '{{ dt }}'
                {%- if not loop.last -%}, {% endif -%}
            {%- endfor -%}
        ]{%- endif -%}
        {%- if default_index_column_granularity -%}, default_index_column_granularity = '{{ default_index_column_granularity }}'{%- endif -%}
    )
{% endmacro %}

{% macro bigquery__drop_search_index(relation) %}
    {%- set search_index_config = adapter.describe_search_index(relation) -%}
    {%- if search_index_config -%}
        {%- set name = search_index_config.name or (relation.identifier ~ '_search_index') -%}
        drop search index if exists `{{ name }}` on {{ relation }}
    {%- endif -%}
{% endmacro %}
