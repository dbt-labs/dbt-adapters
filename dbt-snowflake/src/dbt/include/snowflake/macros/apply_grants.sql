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
    - copy_grants: true  -> copy grants
    - copy_grants: false -> ignore grants
    - unset              -> ignore grants (legacy dbt behavior), unless the
                            snowflake_copy_grants_default behavior flag is on, in which
                            case nothing is emitted and Snowflake's default applies.
-#}
{% macro snowflake__copy_grants_clause() -%}
    {%- set copy_grants = config.get('copy_grants', default=none) -%}
    {%- if copy_grants -%}
        copy grants
    {%- elif copy_grants is none and adapter.behavior.snowflake_copy_grants_default.no_warn -%}
    {%- else -%}
        ignore grants
    {%- endif -%}
{%- endmacro %}
