{% macro snowflake__rename_relation(from_relation, to_relation) -%}
  {%- set ddl_prefix = from_relation.get_ddl_prefix_for_alter() -%}
  {% call statement('rename_relation') -%}
    alter{% if ddl_prefix %} {{ ddl_prefix }}{% endif %} table {{ from_relation }} rename to {{ to_relation }}
  {%- endcall %}
{% endmacro %}
