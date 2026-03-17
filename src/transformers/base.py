"""
Classe abstrata para transformação Raw -> Bronze.
Cada plataforma (Google, TikTok, Pinterest) implementa get_table_configs() e opcionalmente
transform() por tabela; a base cuida de ler, deduplicar e escrever.
"""
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

from src.io import read_raw_parquet, write_delta
from src.transformers.table_config import TableConfig


class BronzeTransformerBase(ABC):
    """
    Contrato para transformar dados da camada Raw (Parquet no MinIO) para Bronze (Delta no MinIO).
    Uso: implementar get_table_configs(); depois run(spark) para processar todas as tabelas.
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    @abstractmethod
    def get_table_configs(self) -> list[TableConfig]:
        """Retorna a lista de tabelas desta plataforma (raw_path, dedupe_columns, bronze_table_name)."""
        ...

    def dedupe(self, df: DataFrame, columns: list[str], order_column: str = "_airbyte_extracted_at") -> DataFrame:
        """
        Mantém uma linha por partição (columns), ficando com a mais recente por order_column.
        """
        window_spec = Window.partitionBy(columns).orderBy(col(order_column).desc())
        return (
            df.withColumn("_row_num", row_number().over(window_spec))
            .filter(col("_row_num") == 1)
            .drop("_row_num")
        )

    def transform(self, df: DataFrame, config: TableConfig) -> DataFrame:
        """
        Hook opcional: transformação extra por tabela antes de escrever.
        Subclasses podem sobrescrever para lógica específica.
        """
        return df

    def run(self, mode: str = "overwrite") -> None:
        """Para cada tabela em get_table_configs(): lê raw, dedupe (se houver colunas), transform, escreve bronze."""
        for config in self.get_table_configs():
            df = read_raw_parquet(self.spark, path=config.raw_path)
            if df.isEmpty():
                continue
            if config.dedupe_columns:
                df = self.dedupe(df, config.dedupe_columns)
            df = self.transform(df, config)
            write_delta(df, "bronze", config.bronze_table_name, mode=mode)
