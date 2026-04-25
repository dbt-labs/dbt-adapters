"""Spark Connect submission path for Athena Apache Spark 3.5+ python models.

Public API: :class:`SparkConnectSubmitter` and :class:`SparkConnectSessionPool`.
The remaining modules (``channel``, ``errors``, ``pyspark_patches``) are
internal helpers used by the submitter and not part of the public surface.
"""

from dbt.adapters.athena.spark_connect.job import SparkConnectSubmitter
from dbt.adapters.athena.spark_connect.session import SparkConnectSessionPool

__all__ = ["SparkConnectSubmitter", "SparkConnectSessionPool"]
