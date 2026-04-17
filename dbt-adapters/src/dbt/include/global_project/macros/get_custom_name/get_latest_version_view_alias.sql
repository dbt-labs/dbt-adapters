
{#
    Renders the alias for the latest-version view that dbt automatically creates
    for versioned models. By default the view takes the unsuffixed model name
    (e.g. "dim_customers"), which is the same name BI tools referenced before
    versioning was introduced.

    Override this macro in your project to change the default behaviour. For
    example, to use a "_latest" suffix instead:

        {% macro generate_latest_version_view_alias(node=none) %}
            {{ node.name ~ "_latest" }}
        {% endmacro %}

    Arguments:
    node: The versioned ModelNode whose latest-version view alias is being
          generated.
#}

{% macro generate_latest_version_view_alias(node=none) -%}
    {% do return(adapter.dispatch('generate_latest_version_view_alias', 'dbt')(node)) %}
{%- endmacro %}

{% macro default__generate_latest_version_view_alias(node=none) -%}
    {{ node.name }}
{%- endmacro %}
