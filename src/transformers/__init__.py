"""Transformers Raw -> Bronze por plataforma (classe abstrata + implementações)."""
from src.transformers.base import BronzeTransformerBase
from src.transformers.google import GoogleRawToBronzeTransformer
from src.transformers.table_config import TableConfig

__all__ = [
    "BronzeTransformerBase",
    "TableConfig",
    "GoogleRawToBronzeTransformer",
]
