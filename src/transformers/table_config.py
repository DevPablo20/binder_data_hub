from dataclasses import dataclass


@dataclass(frozen=True)
class TableConfig:
    raw_path: str
    dedupe_columns: list[str] | None = None
    bronze_table_name: str
    partition_columns: list[str] | None = None
