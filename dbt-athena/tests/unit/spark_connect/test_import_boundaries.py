import ast
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

_PACKAGE_DIR = (
    Path(__file__).resolve().parents[3] / "src" / "dbt" / "adapters" / "athena" / "spark_connect"
)

_BLOCK_PYSPARK = textwrap.dedent(
    """
    import os, sys

    class _Blocker:
        def find_spec(self, name, path=None, target=None):
            if name == "pyspark" or name.startswith("pyspark."):
                print("PYSPARK_IMPORT env=" + str(os.environ.get("SPARK_CONNECT_MODE_ENABLED")))
                raise ImportError("pyspark blocked")

    sys.meta_path.insert(0, _Blocker())
    """
)


def _run(code, env_overrides=None, unset=()):
    env = dict(os.environ)
    for name in unset:
        env.pop(name, None)
    env.update(env_overrides or {})
    result = subprocess.run(
        [sys.executable, "-c", _BLOCK_PYSPARK + textwrap.dedent(code)],
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip().splitlines()


def test_channel_import_does_not_import_pyspark():
    out = _run(
        """
        import dbt.adapters.athena.spark_connect.channel
        print("pyspark" in sys.modules)
        print("dbt.adapters.athena.spark_connect._channel_impl" in sys.modules)
        """
    )
    assert out == ["False", "False"]


@pytest.mark.parametrize(
    "env_overrides, unset, expected",
    [
        ({}, ("SPARK_CONNECT_MODE_ENABLED",), "1"),
        ({"SPARK_CONNECT_MODE_ENABLED": "0"}, (), "0"),
    ],
)
def test_connect_mode_env_is_set_before_pyspark_import(env_overrides, unset, expected):
    out = _run(
        """
        try:
            import dbt.adapters.athena.spark_connect._channel_impl
        except ImportError:
            pass
        """,
        env_overrides=env_overrides,
        unset=unset,
    )
    assert out == [f"PYSPARK_IMPORT env={expected}"]


def _imported_modules(path, *, module_level_only):
    tree = ast.parse(path.read_text())
    nodes = tree.body if module_level_only else ast.walk(tree)
    names = []
    for node in nodes:
        if isinstance(node, ast.ImportFrom):
            names.append(node.module or "")
            names.extend(f"{node.module}.{alias.name}" for alias in node.names)
        elif isinstance(node, ast.Import):
            names.extend(alias.name for alias in node.names)
    return names


def test_channel_impl_does_not_import_channel():
    names = _imported_modules(_PACKAGE_DIR / "_channel_impl.py", module_level_only=False)
    assert not [n for n in names if n.endswith("spark_connect.channel")]


def test_channel_imports_channel_impl_only_lazily():
    names = _imported_modules(_PACKAGE_DIR / "channel.py", module_level_only=True)
    assert not [n for n in names if "_channel_impl" in n]


def test_package_init_has_no_imports():
    assert _imported_modules(_PACKAGE_DIR / "__init__.py", module_level_only=False) == []


def test_connections_imports_session_at_module_level_only():
    connections = _PACKAGE_DIR.parent / "connections.py"
    session = "dbt.adapters.athena.spark_connect.session"
    assert session in _imported_modules(connections, module_level_only=True)
    in_functions = [
        n for n in _imported_modules(connections, module_level_only=False) if "spark_connect" in n
    ]
    assert set(in_functions) == {session, f"{session}.SparkConnectSessionPool"}


def test_job_and_connections_import_in_a_fresh_interpreter():
    out = _run(
        """
        import dbt.adapters.athena.spark_connect.session
        import dbt.adapters.athena.connections
        import dbt.adapters.athena.spark_connect.job
        print("ok")
        """
    )
    assert out == ["ok"]
