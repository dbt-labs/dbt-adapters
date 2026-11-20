{#- persist_docs is ignored, as in v2: the service rejects `comment on` for Iceberg relations -#}
{% macro lakecompute__persist_docs(relation, model, for_relation=true, for_columns=true) -%}
{%- endmacro %}


{% macro lakecompute__alter_relation_comment(relation, relation_comment) -%}
{%- endmacro %}


{% macro lakecompute__alter_column_comment(relation, column_dict) -%}
{%- endmacro %}
