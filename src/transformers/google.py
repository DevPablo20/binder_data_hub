"""Transformer Raw -> Bronze para dados Google."""
from src.transformers.base import BronzeTransformerBase
from src.transformers.table_config import TableConfig


class GoogleRawToBronzeTransformer(BronzeTransformerBase):
    def get_table_configs(self) -> list[TableConfig]:
        return [
            TableConfig(
                raw_path="google/customers",
                dedupe_columns=["customer_id"],
                bronze_table_name="google_customers",
            ),
            TableConfig(
                raw_path="google/campaigns",
                dedupe_columns=["customer_id", "campaign_id"],
                bronze_table_name="google_campaigns",
            ),
            TableConfig(
                raw_path="google/ads",
                dedupe_columns=["customer_id", "ad_id"],
                bronze_table_name="google_ads",
            ),
        ]
