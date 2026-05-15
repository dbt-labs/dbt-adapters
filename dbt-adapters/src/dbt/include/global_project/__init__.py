import os

PACKAGE_PATH = os.path.dirname(__file__)
PROJECT_NAME = "dbt"

# dbt-core < 1.12 does not include 'javascript' in ModelLanguage, so parsing
# any macro that declares supported_languages=['sql', 'python', 'javascript']
# raises KeyError at manifest load time. Patch get_supported_languages to drop
# unrecognised language names instead of crashing. The patch is a no-op on
# dbt-core >= 1.12 where 'javascript' is a valid ModelLanguage member.
try:
    import dbt.clients.jinja as _dbt_jinja
    from dbt.node_types import ModelLanguage as _ModelLanguage

    def _tolerant_get_supported_languages(node):
        if "supported_languages" not in node.args:
            return [_ModelLanguage.sql]
        lang_idx = node.args.index("supported_languages")
        result = []
        for item in node.defaults[-(len(node.args) - lang_idx)].items:
            try:
                result.append(_ModelLanguage[item.value])
            except KeyError:
                pass
        return result or [_ModelLanguage.sql]

    _dbt_jinja.get_supported_languages = _tolerant_get_supported_languages
except Exception:
    pass
