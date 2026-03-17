"""
Escrita de dados nas camadas Bronze, Silver e Gold no MinIO (Delta/Parquet).
Usa settings.bucket_for_layer para mapear camada → bucket (medallion).
"""
from pyspark.sql import DataFrame

from src.config import settings


def write_delta(
    df: DataFrame,
    layer: str,
    table_path: str,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
) -> None:
    """Persiste DataFrame em Delta na camada indicada (bronze, silver ou gold)."""
    bucket = settings.bucket_for_layer(layer)
    base_uri = settings.s3a_uri(bucket, table_path)

    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(base_uri)


def write_parquet(
    df: DataFrame,
    layer: str,
    table_path: str,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
) -> None:
    """Persiste DataFrame em Parquet na camada indicada (bronze, silver ou gold)."""
    bucket = settings.bucket_for_layer(layer)
    base_uri = settings.s3a_uri(bucket, table_path)

    writer = df.write.format("parquet").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(base_uri)
