import os
import re
from pathlib import Path

import pytest

REQUIRED_ENV_VARS = ("DBT_COMPUTE_BASE_URL", "DBT_COMPUTE_AUTH_TOKEN", "DBT_COMPUTE_DATABASE")

FUNCTIONAL_DIR = Path(__file__).parent


def pytest_collection_modifyitems(config, items):
    functional = [item for item in items if item.path.is_relative_to(FUNCTIONAL_DIR)]
    if not functional:
        return

    missing = [name for name in REQUIRED_ENV_VARS if not os.getenv(name)]
    if missing:
        skip = pytest.mark.skip(
            reason=f"functional tests need a live endpoint; set {', '.join(missing)}"
        )
        for item in functional:
            item.add_marker(skip)


@pytest.fixture(scope="class")
def dbt_profile_target():
    """Profile for the functional suite: a live lake compute endpoint."""
    # Defaults to `fivetran`, because a `dct_` personal access token is exchanged for a
    # session rather than sent as a bearer token -- `method: token` with a PAT fails with
    # `401 invalid token`. Set DBT_COMPUTE_AUTH_METHOD=token for a service bearer token.
    method = os.getenv("DBT_COMPUTE_AUTH_METHOD", "fivetran")
    target = {
        "type": "lakecompute",
        "threads": 4,
        "base_url": os.environ["DBT_COMPUTE_BASE_URL"],
        "method": method,
        "database": os.environ["DBT_COMPUTE_DATABASE"],
    }
    token = os.environ["DBT_COMPUTE_AUTH_TOKEN"]
    target["fivetran_credential" if method == "fivetran" else "token"] = token
    return target


@pytest.fixture(scope="class")
def unique_schema(request, prefix) -> str:
    """Where the suite builds.

    By default this is dbt-tests-adapter's generated `test{timestamp}{pid}_{file}` name, which
    keeps parallel workers apart. But schemas are catalog namespaces in a shared account, so
    one per test file litters the catalog, and `drop_schema` has to empty a schema relation by
    relation before it can drop it, which costs a round trip each.

    Set DBT_COMPUTE_SCHEMA to pin every test to one scratch schema instead. It is still
    created and dropped by the `project` fixture -- it just has a name a human chose, so it
    can be cleaned up by hand if a run is interrupted. Pinning it means test classes cannot
    run in parallel; pass `-n0`.
    """
    pinned = os.getenv("DBT_COMPUTE_SCHEMA")
    if pinned:
        return pinned

    test_file = request.module.__name__.split(".")[-1]
    return f"{prefix}_{test_file}"


@pytest.fixture
def captured_sql(project, monkeypatch):
    """Every statement body the connection manager is handed, in order.

    Records at ``_query``, which ``execute``, ``add_query`` and ``add_select_query`` all go
    through. Omit the leading query comment when comparing materialization SQL, while
    sending the original SQL to the driver.
    """
    from dbt.adapters.lakecompute.connections import LakeComputeConnectionManager

    sent = []
    original = LakeComputeConnectionManager._query

    def recording_query(self, sql, *args, **kwargs):
        sent.append(re.sub(r"^\s*/\*.*?\*/\s*", "", sql, count=1, flags=re.DOTALL))
        return original(self, sql, *args, **kwargs)

    monkeypatch.setattr(LakeComputeConnectionManager, "_query", recording_query)
    return sent
