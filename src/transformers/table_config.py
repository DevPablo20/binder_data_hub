"""Configuração de uma tabela no pipeline Raw -> Bronze."""
from dataclasses import dataclass


@dataclass(frozen=True)
class TableConfig:
    """Define origem (raw), colunas de dedupe e destino (bronze) para uma tabela."""

    raw_path: str
    """Subpath no bucket raw (ex.: 'pinterest/ad_accounts', 'google/customers')."""
    dedupe_columns: list[str] | None = None
    """Colunas para particionar na deduplicação (mais recente por _airbyte_extracted_at)."""
    bronze_table_name: str
    """Nome da tabela na camada bronze (ex.: 'pinterest_ad_accounts')."""
