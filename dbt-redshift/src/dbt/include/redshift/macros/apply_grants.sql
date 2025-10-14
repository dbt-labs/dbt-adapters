{%- macro quote_grantees(grantees) -%}
    {%- set quoted_grantees = [] -%}
    {%- for grantee in grantees -%}
        {%- set grantee_parts = grantee.split() -%}
        {%- if grantee_parts | length > 1 and grantee_parts[0].upper() in ('GROUP', 'ROLE') -%}
            {%- set reserved_keyword = grantee_parts[0].upper() -%}
            {%- set identifier = grantee_parts[1:] | join(' ') -%}
            {%- do quoted_grantees.append(reserved_keyword ~ ' ' ~ adapter.quote(identifier)) -%}
        {%- else -%}
            {%- do quoted_grantees.append(adapter.quote(grantee)) -%}
        {%- endif -%}
    {% endfor -%}
    {%- do return(quoted_grantees) -%}
{%- endmacro -%}

{%- macro redshift__get_revoke_sql(relation, privilege, grantees) -%}
    revoke {{ privilege }} on {{ relation.type }} {{ relation }} from {{ quote_grantees(grantees) | join(', ') }}
{%- endmacro -%}

{%- macro redshift__get_grant_sql(relation, privilege, grantees) -%}
    grant {{ privilege }} on {{ relation.type }} {{ relation }} to {{ quote_grantees(grantees) | join(', ') }}
{%- endmacro -%}
