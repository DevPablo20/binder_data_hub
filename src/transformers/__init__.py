"""Transformers Raw → Bronze → Silver → Gold por plataforma."""
from src.transformers.base_bronze import RawToBronzeTransformer
from src.transformers.base_gold import SilverToGoldTransformer
from src.transformers.base_silver import BronzeToSilverTransformer
from src.transformers.table_config import (
    BronzeTableConfig,
    GoldTableConfig,
    SilverTableConfig,
    TableConfig,
)
from src.transformers.tiktok.bronze import TikTokRawToBronzeTransformer
from src.transformers.tiktok.gold import TikTokSilverToGoldTransformer
from src.transformers.tiktok.silver import TikTokBronzeToSilverTransformer

__all__ = [
    "RawToBronzeTransformer",
    "BronzeToSilverTransformer",
    "SilverToGoldTransformer",
    "BronzeTableConfig",
    "SilverTableConfig",
    "GoldTableConfig",
    "TableConfig",
    "TikTokRawToBronzeTransformer",
    "TikTokBronzeToSilverTransformer",
    "TikTokSilverToGoldTransformer",
]
