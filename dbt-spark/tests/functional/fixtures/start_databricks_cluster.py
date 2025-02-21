from base64 import standard_b64encode
from os import getenv
from time import sleep

import pytest

try:
    from pyhive import hive
    from thrift.transport.THttpClient import THttpClient
except ImportError:
    pass

from dbt.adapters.spark.connections import SparkConnectionManager


HOST = "https://" + getenv("DBT_DATABRICKS_HOST_NAME")
CLUSTER = getenv("DBT_DATABRICKS_CLUSTER_NAME")
TOKEN = getenv("DBT_DATABRICKS_TOKEN")
PORT = 443
ORGANIZATION = "0"


# Running this should prevent tests from needing to be retried because the Databricks cluster isn't available
@pytest.fixture(scope="session", autouse=True)
def start_databricks_cluster(request):

    profile = request.config.getoption("--profile")

    if profile.startswith("databricks"):
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
            sleep(10)

    raise Exception("Databricks cluster did not start in time")


def _cursor():
    conn_url = SparkConnectionManager.SPARK_CONNECTION_URL.format(
        host=HOST,
        cluster=CLUSTER,
        port=PORT,
        organization=ORGANIZATION,
    )

    transport = THttpClient(conn_url)
    raw_token = f"token:{TOKEN}".encode()
    token = standard_b64encode(raw_token).decode()
    transport.setCustomHeaders({"Authorization": f"Basic {token}"})

    conn = hive.connect(thrift_transport=transport)
    return conn.cursor()
