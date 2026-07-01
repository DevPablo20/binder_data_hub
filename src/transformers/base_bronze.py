from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

from src.io import read_raw_parquet, write_delta
from src.transformers.table_config import BronzeTableConfig


class RawToBronzeTransformer(ABC):
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    @property
    @abstractmethod
    def platform_name(self) -> str:
        ...

    @abstractmethod
    def get_table_configs(self) -> list[BronzeTableConfig]: ...

    def dedupe(
        self,
        df: DataFrame,
        columns: list[str],
        order_column: str = "_airbyte_extracted_at",
    ) -> DataFrame:
        partition_cols = [col(name) for name in columns]
        window_spec = Window.partitionBy(*partition_cols).orderBy(
            col(order_column).desc()
        )
        return (
            df.withColumn("_row_num", row_number().over(window_spec))
            .filter(col("_row_num") == 1)
            .drop("_row_num")
        )

    def transform(self, df: DataFrame, config: BronzeTableConfig) -> DataFrame:
        return df

    def run(self, mode: str = "overwrite") -> None:
        for config in self.get_table_configs():
            df = read_raw_parquet(self.spark, path=config.raw_path)
            if df.isEmpty():
                continue
            if config.dedupe_columns:
                df = self.dedupe(df, config.dedupe_columns)
            df = self.transform(df, config)
            write_delta(
                df,
                "bronze",
                config.bronze_table_name,
                mode=mode,
                partition_by=config.partition_columns,
            )
