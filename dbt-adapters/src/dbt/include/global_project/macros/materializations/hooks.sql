{% macro run_hooks(hooks, inside_transaction=True) %}
  {% set tracer = modules.opentelemetry.trace.get_tracer("dbt-runner") %}
  {% set hook_span = tracer.start_span('hooks', context=modules.opentelemetry.context.get_current())%}
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
  {% set end_span = hook_span.end() %}
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
