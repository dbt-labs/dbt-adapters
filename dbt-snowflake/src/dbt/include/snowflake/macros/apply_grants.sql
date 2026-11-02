{% macro snowflake__copy_grants() %}
    {% set copy_grants = config.get('copy_grants', False) %}
    {{ return(copy_grants) }}
{% endmacro %}

{%- macro snowflake__support_multiple_grantees_per_dcl_statement() -%}
    {{ return(False) }}
{%- endmacro -%}

{#-
    Grants clause for CREATE OR REPLACE. Snowflake BCR-2433 makes a bare CREATE OR REPLACE
    copy grants from the replaced object, with IGNORE GRANTS as the opt-out.
    - copy_grants: true      -> copy grants
    - false or unset         -> ignore grants (preserves dbt's no-copy default)
-#}
{% macro snowflake__copy_grants_clause() -%}
    {%- if config.get('copy_grants', default=false) -%}
        copy grants
    {%- else -%}
        ignore grants
    {%- endif -%}
{%- endmacro %}
