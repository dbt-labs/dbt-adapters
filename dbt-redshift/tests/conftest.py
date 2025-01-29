pytest_plugins = ["dbt.tests.fixtures.project"]


def pytest_sessionfinish(session, exitstatus):
    """
    Configures pytest to treat a scenario with no tests as passing

    pytest returns a code 5 when it collects no tests in an effort to warn when tests are expected but not collected
    We don't want this when running tox because some combinations of markers and test segments return nothing
    """
    if exitstatus == 5:
        session.exitstatus = 0
