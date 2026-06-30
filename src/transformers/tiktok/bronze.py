"""Transformer Raw → Bronze para TikTok Ads."""
from src.transformers.base_bronze import RawToBronzeTransformer
from src.transformers.table_config import BronzeTableConfig

_RAW_PREFIX = "airbyte/tiktok"


class TikTokRawToBronzeTransformer(RawToBronzeTransformer):
    @property
    def platform_name(self) -> str:
        return "tiktok"

    def get_table_configs(self) -> list[BronzeTableConfig]:
        return [
            BronzeTableConfig(
                raw_path=f"{_RAW_PREFIX}/advertisers",
                bronze_table_name="tiktok/advertisers",
                dedupe_columns=["advertiser_id"],
            ),
            BronzeTableConfig(
                raw_path=f"{_RAW_PREFIX}/campaigns",
                bronze_table_name="tiktok/campaigns",
                dedupe_columns=["advertiser_id", "campaign_id"],
            ),
            BronzeTableConfig(
                raw_path=f"{_RAW_PREFIX}/ad_groups",
                bronze_table_name="tiktok/ad_groups",
                dedupe_columns=["advertiser_id", "campaign_id", "adgroup_id"],
            ),
            BronzeTableConfig(
                raw_path=f"{_RAW_PREFIX}/ads",
                bronze_table_name="tiktok/ads",
                dedupe_columns=["advertiser_id", "campaign_id", "adgroup_id", "ad_id"],
            ),
            BronzeTableConfig(
                raw_path=f"{_RAW_PREFIX}/ads_reports_daily",
                bronze_table_name="tiktok/ads_reports_daily",
                dedupe_columns=[
                    "ad_id",
                    "stat_time_day",
                    "metrics.campaign_id",
                    "metrics.adgroup_id",
                ],
            ),
        ]
