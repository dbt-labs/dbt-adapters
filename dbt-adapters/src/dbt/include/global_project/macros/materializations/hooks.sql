{% macro run_hooks(hooks, inside_transaction=True, span_name='run_hooks') %}
  {% if hooks|length > 0 %}
      {% set tracer = modules.opentelemetry.trace.get_tracer("dbt.runner") %}
      {% with _ = tracer.start_as_current_span(span_name) %}
      {% for hook in hooks | selectattr('transaction', 'equalto', inside_transaction)  %}
        {% if not inside_transaction and loop.first %}
          {% call statement(auto_begin=inside_transaction) %}
            commit;
{% endcall %}
        {% endif %}
        {% set rendered = render(hook.get('sql')) | trim %}
        {% if (rendered | length) > 0 %}
          {% call statement(auto_begin=inside_transaction) %}
            {{ rendered }}
          {% endcall %}
        {% endif %}
      {% endfor %}
      {% endwith %}
  {% endif %}
{% endmacro %}


{% macro make_hook_config(sql, inside_transaction) %}
    {{ tojson({"sql": sql, "transaction": inside_transaction}) }}
{% endmacro %}


{% macro before_begin(sql) %}
    {{ make_hook_config(sql, inside_transaction=False) }}
{% endmacro %}


{% macro in_transaction(sql) %}
    {{ make_hook_config(sql, inside_transaction=True) }}
{% endmacro %}


{% macro after_commit(sql) %}
    {{ make_hook_config(sql, inside_transaction=False) }}
{% endmacro %}
