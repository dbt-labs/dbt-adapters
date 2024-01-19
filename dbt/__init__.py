# N.B.
# This will add to the package’s __path__ all subdirectories of directories on sys.path named after the package which effectively combines both modules into a single namespace (dbt.adapters)

from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)
