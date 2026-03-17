"""Transformers Raw -> Bronze por plataforma (classe abstrata + implementações)."""
from src.transformers.base import RawToBronzeTransformer
from src.transformers.google import GoogleRawToBronzeTransformer
from src.transformers.table_config import TableConfig

__all__ = [
    "RawToBronzeTransformer",
    "TableConfig",
    "GoogleRawToBronzeTransformer",
]
