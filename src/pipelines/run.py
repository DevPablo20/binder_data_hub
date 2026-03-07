"""
Pipeline base: lê da camada Raw (MinIO), aplica transformações placeholder
e grava nas camadas Bronze, Silver e Gold (Delta no MinIO).

Uso:
  python -m src.pipelines.run
  ou, com path/table opcionais: RAW_PATH e TABLE_NAME via env (futuro).
"""
from pyspark.sql import DataFrame

from src.config import settings
from src.io import read_raw_parquet, write_delta
from src.spark_session import get_spark_session


def transform_raw_to_bronze(df: DataFrame) -> DataFrame:
    """Placeholder: raw -> bronze (por hora apenas repassa os dados)."""
    return df


def transform_bronze_to_silver(df: DataFrame) -> DataFrame:
    """Placeholder: bronze -> silver."""
    return df


def transform_silver_to_gold(df: DataFrame) -> DataFrame:
    """Placeholder: silver -> gold."""
    return df


def run(
    raw_path: str = "",
    table_name: str = "default_table",
) -> None:
    """
    Executa o pipeline: Raw (parquet) -> Bronze (delta) -> Silver (delta) -> Gold (delta).
    """
    spark = get_spark_session()

    # Leitura origem: MinIO raw (parquet)
    raw_df = read_raw_parquet(spark, path=raw_path)
    if raw_df.isEmpty():
        spark.stop()
        return

    # Bronze
    bronze_df = transform_raw_to_bronze(raw_df)
    write_delta(bronze_df, "bronze", f"{table_name}", mode="overwrite")

    # Silver (por hora lê do bronze que acabou de escrever; depois pode ler do path delta)
    silver_df = transform_bronze_to_silver(bronze_df)
    write_delta(silver_df, "silver", f"{table_name}", mode="overwrite")

    # Gold
    gold_df = transform_silver_to_gold(silver_df)
    write_delta(gold_df, "gold", f"{table_name}", mode="overwrite")

    spark.stop()


if __name__ == "__main__":
    import os

    raw_path = os.getenv("RAW_PATH", "")
    table_name = os.getenv("TABLE_NAME", "default_table")
    run(raw_path=raw_path, table_name=table_name)
