{% materialization snapshot, adapter='redshift' %}
  {% set original_query_group = set_query_group() %}

  {% set relations = materialization_snapshot_default() %}

  {% do unset_query_group(original_query_group) %}
  {{ return(relations) }}
{% endmaterialization %}
