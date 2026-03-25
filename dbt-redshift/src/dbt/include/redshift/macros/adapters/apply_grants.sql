{#-- Normalize a grants config dict by adding 'user:' prefix to unprefixed entries --#}
{% macro redshift__normalize_grants_dict(grants_dict) %}
    {%- set normalized = {} -%}
    {%- for privilege, grantees in grants_dict.items() -%}
        {%- set normalized_grantees = [] -%}
        {%- for grantee in grantees -%}
            {%- if grantee.startswith('group:') or grantee.startswith('role:') or grantee.startswith('user:') -%}
                {%- do normalized_grantees.append(grantee) -%}
            {%- else -%}
                {#-- Unprefixed = user, add prefix for consistent comparison --#}
                {%- do normalized_grantees.append('user:' ~ grantee) -%}
            {%- endif -%}
        {%- endfor -%}
        {%- do normalized.update({privilege: normalized_grantees}) -%}
    {%- endfor -%}
    {{ return(normalized) }}
{% endmacro %}


{#-- Override apply_grants to normalize config before delegating to default --#}
{% macro redshift__apply_grants(relation, grant_config, should_revoke=True) %}
    {% if grant_config %}
        {% set normalized_grant_config = redshift__normalize_grants_dict(grant_config) %}
        {{ default__apply_grants(relation, normalized_grant_config, should_revoke) }}
    {% endif %}
{% endmacro %}


{% macro redshift__get_show_grant_sql(relation) %}
  {% if redshift__use_show_apis() %}
{#-
    Use SHOW GRANTS for cross-database support (required for RA3/datasharing).
    Note: SHOW GRANTS conflates groups and roles — groups appear with a '/'
    prefix on identity_name and identity_type='role'. The standardize_grants_dict
    method in RedshiftAdapter handles this translation.
-#}
    SHOW GRANTS ON TABLE {{ relation.database }}.{{ relation.schema }}.{{ relation.identifier }}
  {% else %}
{#-
    Use svv_relation_privileges for same-database grants. This view correctly
    distinguishes users, groups, and roles via the identity_type column.
    Requires superuser or SYSLOG ACCESS UNRESTRICTED for full visibility.
    Does NOT support cross-database references.
-#}
    select
        identity_name,
        identity_type,
        privilege_type
    from svv_relation_privileges
    where namespace_name = '{{ relation.schema }}'
      and relation_name = '{{ relation.identifier }}'
      and not (identity_type = 'user' and identity_name = current_user)
  {% endif %}
{% endmacro %}


{#-- Format prefixed grantees into Redshift SQL syntax --#}
{% macro redshift__format_grantees(grantees) %}
    {%- set formatted = [] -%}
    {%- for grantee in grantees -%}
        {%- if grantee.startswith('group:') -%}
            {%- do formatted.append('GROUP ' ~ grantee[6:]) -%}
        {%- elif grantee.startswith('role:') -%}
            {%- do formatted.append('ROLE ' ~ grantee[5:]) -%}
        {%- elif grantee.startswith('user:') -%}
            {%- do formatted.append(grantee[5:]) -%}
        {%- else -%}
            {%- do formatted.append(grantee) -%}
        {%- endif -%}
    {%- endfor -%}
    {{ return(formatted) }}
{% endmacro %}


{% macro redshift__get_grant_sql(relation, privilege, grantees) %}
    {%- set formatted_grantees = redshift__format_grantees(grantees) -%}
    grant {{ privilege }} on {{ relation.render() }} to {{ formatted_grantees | join(', ') }}
{% endmacro %}


{% macro redshift__get_revoke_sql(relation, privilege, grantees) %}
    {%- set formatted_grantees = redshift__format_grantees(grantees) -%}
    revoke {{ privilege }} on {{ relation.render() }} from {{ formatted_grantees | join(', ') }}
{% endmacro %}
