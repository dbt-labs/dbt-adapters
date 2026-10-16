"""Process environment that pyspark reads when it is first imported."""

import os


def ensure_connect_mode_env() -> None:
    os.environ.setdefault("SPARK_CONNECT_MODE_ENABLED", "1")
