"""
Escrita de dados nas camadas Bronze, Silver e Gold no MinIO (Delta/Parquet).
"""
from pyspark.sql import DataFrame, SparkSession

from src.config import settings


def _bucket_for_layer(layer: str) -> str:
    layer = layer.lower()
    if layer == "bronze":
        return settings.bucket_bronze
    if layer == "silver":
        return settings.bucket_silver
    if layer == "gold":
        return settings.bucket_gold
    raise ValueError(f"Camada inválida: {layer}. Use bronze, silver ou gold.")


def write_delta(
    df: DataFrame,
    layer: str,
    table_path: str,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
) -> None:
    """
    Escreve DataFrame como tabela Delta no MinIO na camada indicada.

    Args:
        df: DataFrame a persistir.
        layer: 'bronze', 'silver' ou 'gold'.
        table_path: Caminho lógico da tabela (ex.: 'users', 'sales/fact').
        mode: 'overwrite' ou 'append'.
        partition_by: Colunas para particionamento (opcional).
    """
    bucket = _bucket_for_layer(layer)
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
    """
    Escreve DataFrame como Parquet no MinIO (alternativa sem Delta).
    """
    bucket = _bucket_for_layer(layer)
    base_uri = settings.s3a_uri(bucket, table_path)

    writer = df.write.format("parquet").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(base_uri)
