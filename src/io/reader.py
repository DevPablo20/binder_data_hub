"""
Leitura de dados da camada Raw no MinIO (Parquet).
"""
from pyspark.sql import DataFrame, SparkSession

from src.config import settings


def read_raw_parquet(
    spark: SparkSession,
    path: str = "",
    bucket: str | None = None,
) -> DataFrame:
    """
    Lê Parquet da camada Raw no MinIO.

    Args:
        spark: SparkSession com config MinIO (S3A).
        path: Subcaminho dentro do bucket (ex.: '' para todo o bucket, ou 'schema/table').
        bucket: Bucket a usar; default é settings.bucket_raw.

    Returns:
        DataFrame com os dados lidos.
    """
    b = bucket or settings.bucket_raw
    uri = settings.s3a_uri(b, path)
    return spark.read.parquet(uri)
