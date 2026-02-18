{% materialization seed, adapter='redshift' %}
  {% set original_query_group = set_query_group() %}

  {% set relations = materialization_seed_default() %}

  {% do unset_query_group(original_query_group) %}
  {{ return(relations) }}
{% endmaterialization %}
