import pytest

from tests.functional.projects import dbt_integration


pytest_plugins = ["dbt.tests.fixtures.project"]


@pytest.fixture(scope="class")
def dbt_integration_project():
    return dbt_integration()
