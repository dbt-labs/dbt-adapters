{% macro snowflake__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') -%}
    alter {{ from_relation.get_ddl_prefix_for_alter() }} table {{ from_relation }} rename to {{ to_relation }}
  {%- endcall %}
{% endmacro %}
