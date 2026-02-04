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


{#-- Override apply_grants to normalize config before comparison --#}
{% macro redshift__apply_grants(relation, grant_config, should_revoke=True) %}
    {#-- If grant_config is {} or None, this is a no-op --#}
    {% if grant_config %}
        {#-- Normalize the grant config to add 'user:' prefix to unprefixed entries --#}
        {% set normalized_grant_config = redshift__normalize_grants_dict(grant_config) %}

        {% if should_revoke %}
            {#-- We think previous grants may have carried over --#}
            {#-- Show current grants and calculate diffs --#}
            {% set current_grants_table = run_query(get_show_grant_sql(relation)) %}
            {% set current_grants_dict = adapter.standardize_grants_dict(current_grants_table) %}
            {% set needs_granting = diff_of_two_dicts(normalized_grant_config, current_grants_dict) %}
            {% set needs_revoking = diff_of_two_dicts(current_grants_dict, normalized_grant_config) %}
            {% if not (needs_granting or needs_revoking) %}
                {{ log('On ' ~ relation.render() ~': All grants are in place, no revocation or granting needed.')}}
            {% endif %}
        {% else %}
            {#-- We don't think there's any chance of previous grants having carried over. --#}
            {#-- Jump straight to granting what the user has configured. --#}
            {% set needs_revoking = {} %}
            {% set needs_granting = normalized_grant_config %}
        {% endif %}

        {% if needs_granting or needs_revoking %}
            {% set revoke_statement_list = get_dcl_statement_list(relation, needs_revoking, get_revoke_sql) %}
            {% set grant_statement_list = get_dcl_statement_list(relation, needs_granting, get_grant_sql) %}
            {% set dcl_statement_list = revoke_statement_list + grant_statement_list %}
            {% if dcl_statement_list %}
                {{ call_dcl_statements(dcl_statement_list) }}
            {% endif %}
        {% endif %}
    {% endif %}
{% endmacro %}


{% macro redshift__get_show_grant_sql(relation) %}
{#-
    Query existing grants on a relation, including users, groups, and roles.
    Returns grantee names with 'user:' or 'group:' prefix to match the config format.

    Uses generate_series + split_part to parse pg_class.relacl since Redshift
    doesn't support unnest() on aclitem[] arrays.

    ACL format: "grantee=privileges/grantor" where:
    - Users: "username=rw/grantor"
    - Groups: "group groupname=rw/grantor" (note space after 'group')

    Privilege codes: r=select, a=insert, w=update, d=delete, R=references
-#}

with ns as (
    select generate_series(1, 100) as n
),

acl_entries as (
    select
        trim(split_part(array_to_string(c.relacl, ','), ',', ns.n)) as acl_entry
    from pg_class c
    cross join ns
    where c.oid = '{{ relation }}'::regclass
      and len(trim(split_part(array_to_string(c.relacl, ','), ',', ns.n))) > 0
),

parsed as (
    select
        case
            when split_part(acl_entry, '=', 1) like 'group %'
            then 'group:' || trim(substring(split_part(acl_entry, '=', 1), 7))
            else 'user:' || split_part(acl_entry, '=', 1)
        end as grantee,
        split_part(split_part(acl_entry, '=', 2), '/', 1) as privileges
    from acl_entries
    where len(split_part(acl_entry, '=', 1)) > 0
),

privilege_map as (
    select grantee, privileges, 'select' as privilege_type from parsed where privileges like '%r%'
    union all
    select grantee, privileges, 'insert' as privilege_type from parsed where privileges like '%a%'
    union all
    select grantee, privileges, 'update' as privilege_type from parsed where privileges like '%w%'
    union all
    select grantee, privileges, 'delete' as privilege_type from parsed where privileges like '%d%'
    union all
    select grantee, privileges, 'references' as privilege_type from parsed where privileges like '%R%'
)

select
    grantee,
    privilege_type
from privilege_map
where grantee <> 'user:' || current_user
  and grantee not like '%=%'  -- filter out malformed entries

{% endmacro %}


{% macro redshift__get_grant_sql(relation, privilege, grantees) %}
{#-
    Generate GRANT statement for Redshift, supporting users, groups, and roles.

    Grantee format:
    - 'username' or 'user:username' -> GRANT ... TO username
    - 'group:groupname' -> GRANT ... TO GROUP groupname
    - 'role:rolename' -> GRANT ... TO ROLE rolename
-#}
    {%- set formatted_grantees = [] -%}
    {%- for grantee in grantees -%}
        {%- if grantee.startswith('group:') -%}
            {%- do formatted_grantees.append('GROUP ' ~ grantee[6:]) -%}
        {%- elif grantee.startswith('role:') -%}
            {%- do formatted_grantees.append('ROLE ' ~ grantee[5:]) -%}
        {%- elif grantee.startswith('user:') -%}
            {%- do formatted_grantees.append(grantee[5:]) -%}
        {%- else -%}
            {#-- No prefix = user (backward compatible) --#}
            {%- do formatted_grantees.append(grantee) -%}
        {%- endif -%}
    {%- endfor -%}
    grant {{ privilege }} on {{ relation.render() }} to {{ formatted_grantees | join(', ') }}
{% endmacro %}


{% macro redshift__get_revoke_sql(relation, privilege, grantees) %}
{#-
    Generate REVOKE statement for Redshift, supporting users, groups, and roles.

    Grantee format:
    - 'username' or 'user:username' -> REVOKE ... FROM username
    - 'group:groupname' -> REVOKE ... FROM GROUP groupname
    - 'role:rolename' -> REVOKE ... FROM ROLE rolename
-#}
    {%- set formatted_grantees = [] -%}
    {%- for grantee in grantees -%}
        {%- if grantee.startswith('group:') -%}
            {%- do formatted_grantees.append('GROUP ' ~ grantee[6:]) -%}
        {%- elif grantee.startswith('role:') -%}
            {%- do formatted_grantees.append('ROLE ' ~ grantee[5:]) -%}
        {%- elif grantee.startswith('user:') -%}
            {%- do formatted_grantees.append(grantee[5:]) -%}
        {%- else -%}
            {#-- No prefix = user (backward compatible) --#}
            {%- do formatted_grantees.append(grantee) -%}
        {%- endif -%}
    {%- endfor -%}
    revoke {{ privilege }} on {{ relation.render() }} from {{ formatted_grantees | join(', ') }}
{% endmacro %}
