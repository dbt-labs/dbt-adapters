{% macro should_replace_on_create(relation) -%}
  {{ return(adapter.dispatch('should_replace_on_create', 'dbt')(relation)) }}
{%- endmacro %}


{% macro snowflake__should_replace_on_create(relation) -%}
{#-
    Decide whether CREATE TABLE ... AS for `relation` may use OR REPLACE.

    Incremental models and snapshots take their "create a new table" path when dbt's
    relation cache has no entry for the target. With the
    `snowflake_fail_on_unexpected_existing_relation` behavior flag enabled (the default), that
    path emits a plain CREATE. A table that exists despite the cache (an incomplete metadata
    listing, or a concurrent create) then makes the statement fail instead of being replaced
    along with its data.

    Unchanged: other materializations (table models are rebuilt on every run by design),
    tables other than the node's own target (e.g. helper tables created by custom strategies or
    packages), incremental models run with --full-refresh, and relations present in the cache.
    Snapshots never honor --full-refresh, so it is not consulted for them.

    The flag is read (without `no_warn`) only on the uncached first-run path, where it decides the
    DDL. dbt fires the behavior change notice at most once per invocation, so users who opt out
    see it the first time an incremental model or snapshot is created without a cache entry,
    whether or not the table actually existed.
-#}
{%- set materialized = config.get('materialized') -%}
{#- Case-insensitive, so a quoting mismatch errs toward the guarded (fail-loud) path. -#}
{%- set is_node_target = this is defined and this is not none
    and (relation.database | lower, relation.schema | lower, relation.identifier | lower)
        == (this.database | lower, this.schema | lower, this.identifier | lower) -%}
{%- if materialized not in ['incremental', 'snapshot'] -%}
    {%- set replace = true -%}
{%- elif not is_node_target -%}
    {%- set replace = true -%}
{%- elif materialized == 'incremental' and should_full_refresh() -%}
    {%- set replace = true -%}
{%- elif load_cached_relation(relation) is not none -%}
    {%- set replace = true -%}
{%- else -%}
    {%- set replace = not adapter.behavior.snowflake_fail_on_unexpected_existing_relation -%}
{%- endif -%}
{{ return(replace) }}
{%- endmacro %}
