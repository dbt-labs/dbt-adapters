{% macro do_something(foo, bar) %}

    select
        '{{ foo }}'::text as foo,
        '{{ bar }}'::text as bar

{% endmacro %}
