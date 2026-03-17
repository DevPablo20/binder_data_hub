"""
Leitura de dados das camadas Raw (Parquet) e Bronze/Silver/Gold (Delta) no MinIO.
Usa settings para endpoint e buckets (medallion: raw, bronze, silver, gold).
"""
from pyspark.sql import DataFrame, SparkSession

from src.config import settings


def read_raw_parquet(
    spark: SparkSession,
    path: str = "",
    bucket: str | None = None,
) -> DataFrame:
    """Lê Parquet da camada Raw (origem do Airbyte)."""
    b = bucket or settings.bucket_raw
    uri = settings.s3a_uri(b, path)
    return spark.read.parquet(uri)


def read_delta(
    spark: SparkSession,
    layer: str,
    table_path: str,
) -> DataFrame:
    """Lê tabela Delta de uma camada (bronze, silver ou gold)."""
    bucket = settings.bucket_for_layer(layer)
    uri = settings.s3a_uri(bucket, table_path)
    return spark.read.format("delta").load(uri)
