"""Keep an Athena Spark session active while a model is running on it."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Optional

from dbt.adapters.athena.constants import LOGGER
from dbt.adapters.athena.spark_connect.pyspark_patches import client_retries_disabled

if TYPE_CHECKING:
    from pyspark.sql.connect.session import SparkSession as ConnectSparkSession

_STOP_JOIN_TIMEOUT_SECONDS = 5


class SessionKeepalive:
    """Send a trivial Spark operation every ``interval`` seconds until stopped.

    Athena counts idle time from the last Spark Connect operation, not from
    the client process, so a model that spends longer than the session idle
    timeout in driver-side code (planning, commits, plain Python) loses its
    session mid-run.
    """

    def __init__(self, spark: ConnectSparkSession, session_id: str, interval: float) -> None:
        self._spark = spark
        self._session_id = session_id
        self._interval = interval
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run,
            name=f"spark-connect-keepalive-{self._session_id}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is None:
            return
        self._thread.join(timeout=_STOP_JOIN_TIMEOUT_SECONDS)
        if self._thread.is_alive():
            LOGGER.debug(
                f"Spark Connect keepalive for session {self._session_id} is still finishing "
                f"its last operation after {_STOP_JOIN_TIMEOUT_SECONDS}s"
            )

    def _run(self) -> None:
        failing = False
        # A failed keepalive is retried on the next tick; client-side retries would
        # outlive stop() and keep sending to a session that may be reused or closed.
        with client_retries_disabled():
            while not self._stop.wait(self._interval):
                try:
                    self._spark.sql("SELECT 1").collect()
                except Exception as e:  # noqa: BLE001 - must not end the model
                    log = LOGGER.debug if failing else LOGGER.warning
                    log(f"Spark Connect keepalive failed for session {self._session_id}: {e}")
                    failing = True
                else:
                    if failing:
                        LOGGER.debug(
                            f"Spark Connect keepalive recovered for session {self._session_id}"
                        )
                    failing = False
