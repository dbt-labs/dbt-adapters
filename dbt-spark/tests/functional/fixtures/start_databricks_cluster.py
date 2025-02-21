import base64
import time

import pytest

try:
    from pyhive import hive
    from thrift.transport import THttpClient
except ImportError:
    pass

from dbt.adapters.spark.connections import SparkConnectionManager

from tests.functional.fixtures.profiles import databricks_http_cluster_target


# Running this should prevent tests from needing to be retried because the Databricks cluster isn't available
@pytest.fixture(scope="session", autouse=True)
def start_databricks_cluster(request):

    profile = request.config.getoption("--profile")

    if profile == "databricks_http_cluster":
        _wait_for_databricks_cluster()

    yield


def _wait_for_databricks_cluster():
    """
    It takes roughly 3-5 minutes for the cluster to start, to be safe we'll wait for 10 minutes
    """
    cursor = _cursor()

    for _ in range(60):
        try:
            cursor.execute("SELECT 1", async_=False)
            return
        except Exception:
            time.sleep(10)

    raise Exception("Databricks cluster did not start in time")


def _cursor():
    creds = databricks_http_cluster_target()

    conn_url = SparkConnectionManager.SPARK_CONNECTION_URL.format(
        host=creds["host"],
        port=creds["port"],
        organization=creds["organization"],
        cluster=creds["cluster"],
    )

    transport = THttpClient.THttpClient(conn_url)
    raw_token = f"token:{creds['token']}".encode()
    token = base64.standard_b64encode(raw_token).decode()
    transport.setCustomHeaders({"Authorization": f"Basic {token}"})

    conn = hive.connect(thrift_transport=transport)
    return conn.cursor()
