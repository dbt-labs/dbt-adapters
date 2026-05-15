import os
from importlib.metadata import version as pkg_version
from packaging.version import Version

_HERE = os.path.dirname(__file__)

try:
    _core_version = Version(pkg_version("dbt-core"))
    _js_supported = _core_version >= Version("1.12.0")
except Exception:
    _js_supported = False

# On dbt-core >= 1.12, ModelLanguage includes 'javascript', so we can safely
# load the js-capable macro directory. On older versions we fall back to the
# base directory which omits javascript from supported_languages.
PACKAGE_PATH = (
    os.path.normpath(os.path.join(_HERE, "..", "global_project_js")) if _js_supported else _HERE
)
PROJECT_NAME = "dbt"
