{#- Unset copy_grants emits a bare CREATE OR REPLACE, which may keep grants, so revoke too -#}
{% macro snowflake__copy_grants() %}
    {% set copy_grants = config.get('copy_grants') %}
    {{ return(copy_grants is none or copy_grants) }}
{% endmacro %}

{%- macro snowflake__support_multiple_grantees_per_dcl_statement() -%}
    {{ return(False) }}
{%- endmacro -%}

{#-
    Grants clause for CREATE OR REPLACE. Snowflake BCR-2433 makes a bare CREATE OR REPLACE
    copy grants from the replaced object, with IGNORE GRANTS as the opt-out. The BCR was pulled
    from the bundle but stays enabled for accounts that opted in, so the default varies by account.
    - copy_grants: true      -> copy grants
    - copy_grants: false     -> ignore grants
    - unset                  -> no clause, so the account default applies
-#}
{% macro snowflake__copy_grants_clause() -%}
    {%- set copy_grants = config.get('copy_grants') -%}
    {%- if copy_grants is none -%}
    {%- elif copy_grants -%}
        copy grants
    {%- else -%}
        ignore grants
    {%- endif -%}
{%- endmacro %}
