"""Contrato para transformação Bronze → Silver."""
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from src.io import read_delta, write_delta
from src.transformers.table_config import SilverTableConfig


class BronzeToSilverTransformer(ABC):
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    @property
    @abstractmethod
    def platform_name(self) -> str: ...

    @abstractmethod
    def get_table_configs(self) -> list[SilverTableConfig]: ...

    def transform(self, df: DataFrame, config: SilverTableConfig) -> DataFrame:
        return df

    def run(self, mode: str = "overwrite") -> None:
        for config in self.get_table_configs():
            try:
                df = read_delta(self.spark, "bronze", config.bronze_table_name)
            except Exception:
                continue
            if df.isEmpty():
                continue
            df = self.transform(df, config)
            write_delta(
                df,
                "silver",
                config.silver_table_name,
                mode=mode,
            )
