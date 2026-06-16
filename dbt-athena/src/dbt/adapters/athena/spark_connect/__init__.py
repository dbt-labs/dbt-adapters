"""Spark Connect submission path for Athena Apache Spark 3.5+ python models."""

from dbt.adapters.athena.spark_connect.job import SparkConnectSubmitter
from dbt.adapters.athena.spark_connect.session import SparkConnectSessionPool

__all__ = ["SparkConnectSubmitter", "SparkConnectSessionPool"]
