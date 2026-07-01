from pyspark.sql import SparkSession

from src.spark_session import get_spark_session

_spark: SparkSession | None = None


def get_spark() -> SparkSession:
    global _spark
    if _spark is None:
        _spark = get_spark_session(app_name="binder-query-api")
    return _spark


def shutdown_spark() -> None:
    global _spark
    if _spark is not None:
        _spark.stop()
        _spark = None
