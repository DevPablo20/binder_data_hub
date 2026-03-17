from pyspark.sql import DataFrame, SparkSession

from src.config import settings


def read_raw_parquet(
    spark: SparkSession,
    path: str = "",
    bucket: str | None = None,
) -> DataFrame:
    b = bucket or settings.bucket_raw
    uri = settings.s3a_uri(b, path)
    return spark.read.parquet(uri)
