from dataclasses import dataclass


@dataclass(frozen=True)
class BronzeTableConfig:
    raw_path: str
    bronze_table_name: str
    dedupe_columns: list[str] | None = None
    partition_columns: list[str] | None = None


@dataclass(frozen=True)
class SilverTableConfig:
    bronze_table_name: str
    silver_table_name: str


@dataclass(frozen=True)
class GoldTableConfig:
    silver_sources: list[str]
    gold_table_name: str


# Backward-compatible alias used by bronze base
TableConfig = BronzeTableConfig
