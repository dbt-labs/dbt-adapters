from base64 import standard_b64encode
from os import getenv
from time import sleep

from pyhive import hive
from thrift.transport.THttpClient import THttpClient
from thrift.Thrift import TApplicationException


HOST = getenv("DBT_DATABRICKS_HOST_NAME")
CLUSTER = getenv("DBT_DATABRICKS_CLUSTER_NAME")
TOKEN = getenv("DBT_DATABRICKS_TOKEN")


def _wait_for_databricks_cluster() -> None:
    """
    It takes roughly 3-5 minutes for the cluster to start, to be safe we'll wait for 10 minutes
    """
    transport_client = _transport_client()

    for _ in range(20):
        try:
            hive.connect(thrift_transport=transport_client)
            return
        except TApplicationException:
            sleep(30)

    raise Exception("Databricks cluster did not start in time")


def _transport_client() -> THttpClient:
    transport_client = THttpClient(f"https://{HOST}:443/sql/protocolv1/o/0/{CLUSTER}")
    raw_token = f"token:{TOKEN}".encode()
    token = standard_b64encode(raw_token).decode()
    transport_client.setCustomHeaders({"Authorization": f"Basic {token}"})
    return transport_client


if __name__ == "__main__":
    _wait_for_databricks_cluster()
