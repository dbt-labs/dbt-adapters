{#- Fail early with the model's name instead of falling through to the global defaults. -#}

{% macro lakecompute__unsupported_materialization(what, instead) -%}
  {{ exceptions.raise_compiler_error(
    "lakecompute does not support the '" ~ what ~ "' materialization (model "
    ~ model.unique_id ~ "). " ~ instead
  ) }}
{%- endmacro %}


{% materialization incremental, adapter='lakecompute' %}
  {{ lakecompute__unsupported_materialization(
    'incremental',
    "Incremental models are not implemented yet; use materialized='table'."
  ) }}
{% endmaterialization %}


{% materialization view, adapter='lakecompute' %}
  {#- Polaris does not grant CREATE_VIEW -#}
  {{ lakecompute__unsupported_materialization(
    'view',
    "Views are not available on this service; use materialized='table'. "
    ~ "dbt defaults models to views, so set `+materialized: table` in dbt_project.yml."
  ) }}
{% endmaterialization %}


{% materialization materialized_view, adapter='lakecompute' %}
  {{ lakecompute__unsupported_materialization(
    'materialized_view',
    "Use materialized='table' and schedule the model."
  ) }}
{% endmaterialization %}


{% materialization snapshot, adapter='lakecompute' %}
  {{ lakecompute__unsupported_materialization(
    'snapshot',
    "Snapshots require merge and column-type changes, neither of which this service supports."
  ) }}
{% endmaterialization %}


{% materialization clone, adapter='lakecompute' %}
  {{ lakecompute__unsupported_materialization(
    'clone',
    "There is no zero-copy clone for Iceberg relations here. Re-run the model instead."
  ) }}
{% endmaterialization %}


{% materialization unit, adapter='lakecompute' %}
  {{ lakecompute__unsupported_materialization(
    'unit',
    "dbt unit tests are not supported by this adapter; use data tests instead."
  ) }}
{% endmaterialization %}


{% materialization function, adapter='lakecompute', supported_languages=['sql'] %}
  {{ lakecompute__unsupported_materialization(
    'function',
    "User-defined functions are not available on this service."
  ) }}
{% endmaterialization %}
