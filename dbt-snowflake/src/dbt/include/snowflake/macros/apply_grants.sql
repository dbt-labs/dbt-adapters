{% macro snowflake__copy_grants() %}
    {% set copy_grants = config.get('copy_grants', False) %}
    {{ return(copy_grants) }}
{% endmacro %}

{%- macro snowflake__support_multiple_grantees_per_dcl_statement() -%}
    {{ return(False) }}
{%- endmacro -%}


{#- A Cortex Search service needs its object type spelled out in grant DCL; the
    defaults' bare `on <name>` resolves as a table. Everything else keeps the defaults. -#}
{% macro snowflake__get_show_grant_sql(relation) %}
    {%- if relation.is_cortex_search_service -%}
        show grants on cortex search service {{ relation.render() }}
    {%- else -%}
        {{ default__get_show_grant_sql(relation) }}
    {%- endif -%}
{% endmacro %}


{%- macro snowflake__get_grant_sql(relation, privilege, grantees) -%}
    {%- if relation.is_cortex_search_service -%}
        grant {{ privilege }} on cortex search service {{ relation.render() }} to {{ grantees | join(', ') }}
    {%- else -%}
        {{ default__get_grant_sql(relation, privilege, grantees) }}
    {%- endif -%}
{%- endmacro -%}


{%- macro snowflake__get_revoke_sql(relation, privilege, grantees) -%}
    {%- if relation.is_cortex_search_service -%}
        {%- set quoted_grantees = [] -%}
        {%- for grantee in grantees -%}
            {%- do quoted_grantees.append(adapter.quote(grantee)) -%}
        {%- endfor -%}
        revoke {{ privilege }} on cortex search service {{ relation.render() }} from {{ quoted_grantees | join(', ') }}
    {%- else -%}
        {{ default__get_revoke_sql(relation, privilege, grantees) }}
    {%- endif -%}
{%- endmacro -%}
