import os
from importlib.metadata import version as pkg_version
from packaging.version import Version

_HERE = os.path.dirname(__file__)

try:
    _core_version = Version(pkg_version("dbt-core"))
    _js_supported = _core_version >= Version("1.12.0")
except Exception:
    _js_supported = False

# global_project/ declares supported_languages=['sql', 'python', 'javascript'],
# which requires dbt-core >= 1.12 to parse without error. On older versions,
# fall back to the generated global_project_non_js/ directory, which omits
# javascript from supported_languages. global_project_non_js/ is a build
# artifact; run `hatch build` or the generate-compat script to produce it
# locally when testing against dbt-core < 1.12.
PACKAGE_PATH = (
    _HERE
    if _js_supported
    else os.path.normpath(os.path.join(_HERE, "..", "global_project_non_js"))
)
PROJECT_NAME = "dbt"
