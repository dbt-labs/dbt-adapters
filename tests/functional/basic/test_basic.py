from dbt.tests.util import get_manifest
import pytest

from tests.functional.utils import run_dbt


@pytest.fixture(scope="class")
def models():
    return {"my_model.sql": "select 1 as fun"}


def test_basic(project):
    # Tests that a project with a single model works
    results = run_dbt(["run"])
    assert len(results) == 1
    manifest = get_manifest(project.project_root)
    assert "model.test.my_model" in manifest.nodes
