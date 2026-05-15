import os

PACKAGE_PATH = os.path.dirname(__file__)
PROJECT_NAME = "dbt"

# dbt-core < 1.12 does not include 'javascript' in ModelLanguage, causing a
# KeyError when the manifest parser encounters function.sql's declaration of
# supported_languages=['sql', 'python', 'javascript']. Extend the enum with
# the missing member so the lookup succeeds at runtime. Patching the class
# itself — rather than wrapping get_supported_languages — means the fix is
# effective regardless of whether that function was already imported under a
# local binding by dbt's parser modules before this module was imported.
# On dbt-core >= 1.12, 'javascript' already exists in the enum and this block
# is skipped entirely.
try:
    from dbt.node_types import ModelLanguage as _ModelLanguage

    if "javascript" not in _ModelLanguage._member_map_:
        # StrEnum (dbt-core 1.9–1.11) inherits from str, an immutable type.
        # object.__new__ raises TypeError for str subclasses; str.__new__ must
        # be used instead so the underlying str value is set correctly.
        if issubclass(_ModelLanguage, str):
            _js = str.__new__(_ModelLanguage, "javascript")
        else:
            _js = object.__new__(_ModelLanguage)
        _js._name_ = "javascript"  # type: ignore[attr-defined]
        _js._value_ = "javascript"  # type: ignore[attr-defined]
        setattr(_ModelLanguage, "javascript", _js)
        _ModelLanguage._member_names_.append("javascript")
        _ModelLanguage._member_map_["javascript"] = _js
        _ModelLanguage._value2member_map_["javascript"] = _js
except Exception:
    pass
