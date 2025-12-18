{% macro snowflake__cast(field, type) %}
    {% if (type|upper == "GEOGRAPHY") -%}
        to_geography({{field}})
    {% elif (type|upper == "GEOMETRY") -%}
        to_geometry({{field}})
    {% else -%}
        {#--
          SnowflakeColumn.data_type may include a collation clause for string types,
          e.g. "VARCHAR(100) COLLATE 'en-ci-rtrim'". CAST does not accept collation
          inside the data type, so strip it here.
        --#}
        {%- set cleaned_type = modules.re.sub(
          "(?i)\\s+collat(?:e|ion)\\s+(?:'[^']*'|\"[^\"]*\")(?:\\s+rtrim)?",
          "",
          type
        ) | trim -%}
        cast({{field}} as {{cleaned_type}})
    {% endif -%}
{% endmacro %}
