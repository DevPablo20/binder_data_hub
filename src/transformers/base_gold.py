from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession

from src.io import read_delta, write_delta
from src.transformers.table_config import GoldTableConfig


class SilverToGoldTransformer(ABC):
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    @property
    @abstractmethod
    def platform_name(self) -> str: ...

    @abstractmethod
    def get_table_configs(self) -> list[GoldTableConfig]: ...

    def build(self, sources: dict[str, DataFrame], config: GoldTableConfig) -> DataFrame | None:
        return None

    def run(self, mode: str = "overwrite") -> None:
        for config in self.get_table_configs():
            sources: dict[str, DataFrame] = {}
            for table in config.silver_sources:
                try:
                    df = read_delta(self.spark, "silver", table)
                except Exception:
                    df = None
                if df is None or df.isEmpty():
                    continue
                sources[table] = df
            if not sources:
                continue
            gold_df = self.build(sources, config)
            if gold_df is None or gold_df.isEmpty():
                continue
            write_delta(
                gold_df,
                "gold",
                config.gold_table_name,
                mode=mode,
            )
