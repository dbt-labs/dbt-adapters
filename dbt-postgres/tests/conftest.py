# in order to call dbt's internal profile rendering, we need to set the
# flags global. This is a bit of a hack, but it's the best way to do it.
from dbt.flags import set_from_args
from argparse import Namespace

set_from_args(Namespace(), None)

pytest_plugins = "dbt.tests.fixtures.project"


def pytest_sessionfinish(session, exitstatus):
    """
    Configures pytest to treat a scenario with no tests as passing

    pytest returns a code 5 when it collects no tests in an effort to warn when tests are expected but not collected
    We don't want this when running tox because some combinations of markers and test segments return nothing
    """
    if exitstatus == 5:
        session.exitstatus = 0
