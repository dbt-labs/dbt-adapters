from os import getenv
from time import sleep

from databricks import sql


HOST = getenv("DBT_DATABRICKS_HOST_NAME")
ENDPOINT = getenv("DBT_DATABRICKS_ENDPOINT")
TOKEN = getenv("DBT_DATABRICKS_TOKEN")


def _wait_for_databricks_cluster() -> None:
    """
    It takes roughly 3-5 minutes for the cluster to start, to be safe we'll wait for 10 minutes
    """
    for _ in range(1):
        try:
            sql.connect(
                server_hostname=HOST,
                http_path=f"/sql/1.0/warehouses/{ENDPOINT}",
                access_token=TOKEN,
            )
            return
        except KeyError:
            sleep(30)

    raise Exception("Databricks cluster did not start in time")


if __name__ == "__main__":
    _wait_for_databricks_cluster()
