import os
import shutil

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

_NON_JS_FUNCTION_SQL = """\
{% materialization function, default, supported_languages=['sql', 'python'] %}
    {% set existing_relation = load_cached_relation(this) %}
    {% set target_relation = this.incorporate(type=this.Function) %}

    {{ run_hooks(pre_hooks) }}

    {% set function_config = this.get_function_config(model) %}
    {% set macro_name = this.get_function_macro_name(function_config) %}

    {# Doing this aliasing of adapter.dispatch is a hacky way to disable the static analysis of actually calling adapter.dispatch #}
    {# This is necessary because the static analysis breaks being able to dynamically pass a macro_name #}
    {% set _dispatch = adapter.dispatch %}

    {% set build_sql = _dispatch(macro_name, 'dbt')(target_relation) %}
    {{ function_execute_build_sql(build_sql, existing_relation, target_relation) }}
    {{ run_hooks(post_hooks) }}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
"""

_FUNCTION_SQL_REL = os.path.join("macros", "materializations", "functions", "function.sql")


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version, build_data):
        src = os.path.join(self.root, "src", "dbt", "include", "global_project")
        dst = os.path.join(self.root, "src", "dbt", "include", "global_project_non_js")

        if os.path.exists(dst):
            shutil.rmtree(dst)
        shutil.copytree(src, dst)

        with open(os.path.join(dst, _FUNCTION_SQL_REL), "w") as f:
            f.write(_NON_JS_FUNCTION_SQL)
