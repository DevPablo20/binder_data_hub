from pyspark.sql import DataFrame
from pyspark.sql.functions import col, first, sum

from src.transformers.base_gold import SilverToGoldTransformer
from src.transformers.table_config import GoldTableConfig

_SILVER_ADVERTISERS = "tiktok/advertisers"
_SILVER_CAMPAIGNS = "tiktok/campaigns"
_SILVER_AD_GROUPS = "tiktok/ad_groups"
_SILVER_ADS = "tiktok/ads"
_SILVER_ADS_REPORTS_DAILY = "tiktok/ads_reports_daily"


class TikTokSilverToGoldTransformer(SilverToGoldTransformer):
    @property
    def platform_name(self) -> str:
        return "tiktok"

    def get_table_configs(self) -> list[GoldTableConfig]:
        return [
            GoldTableConfig(
                silver_sources=[
                    _SILVER_ADVERTISERS,
                    _SILVER_CAMPAIGNS,
                    _SILVER_AD_GROUPS,
                    _SILVER_ADS,
                    _SILVER_ADS_REPORTS_DAILY,
                ],
                gold_table_name="tiktok/campaign_daily_metrics",
            ),
        ]

    def build(self, sources: dict[str, DataFrame], config: GoldTableConfig) -> DataFrame | None:
        required = [
            _SILVER_ADVERTISERS,
            _SILVER_CAMPAIGNS,
            _SILVER_AD_GROUPS,
            _SILVER_ADS,
            _SILVER_ADS_REPORTS_DAILY,
        ]
        if not all(key in sources for key in required):
            return None

        advertisers = sources[_SILVER_ADVERTISERS].select(
            col("ad_account_id"),
            col("ad_account_name"),
        )
        campaigns = sources[_SILVER_CAMPAIGNS].select(
            col("ad_account_id"),
            col("campaign_id"),
            col("campaign_name"),
            col("campaign_status"),
            col("campaign_operation_status"),
            col("objective_type"),
            col("budget"),
        )
        ad_groups = sources[_SILVER_AD_GROUPS].select(
            col("ad_account_id"),
            col("campaign_id"),
            col("ad_group_id"),
            col("optimization_goal"),
            col("billing_event"),
        )
        ads = sources[_SILVER_ADS].select(
            col("ad_account_id"),
            col("campaign_id"),
            col("ad_group_id"),
            col("ad_id"),
            col("ad_name"),
            col("ad_text"),
            col("display_name"),
        )
        reports = sources[_SILVER_ADS_REPORTS_DAILY].select(
            col("campaign_id"),
            col("ad_group_id"),
            col("ad_id"),
            col("stat_time_day").alias("date"),
            col("conversion").alias("conversions"),
            col("clicks"),
            col("result").alias("engagement"),
            col("impressions"),
            col("spend"),
            col("purchase"),
            col("shares"),
            col("comments"),
            col("complete_payment"),
            col("clicks_on_music_disc"),
            col("profile_visits"),
            col("total_app_event_add_to_cart"),
            col("registration"),
            col("sales_lead"),
            col("onsite_shopping"),
            col("video_watched_2s").alias("video_views_2s"),
            col("video_watched_6s").alias("video_views_6s"),
            col("video_views_p25").alias("video_views_25p"),
            col("video_views_p50").alias("video_views_50p"),
            col("video_views_p75").alias("video_views_75p"),
            col("video_views_p100").alias("video_views_100p"),
            col("video_play_actions").alias("video_views"),
            col("reach"),
        )

        joined = (
            reports.alias("tard")
            .join(
                ads.alias("tad"),
                on=(
                    (col("tard.ad_id") == col("tad.ad_id"))
                    & (col("tard.ad_group_id") == col("tad.ad_group_id"))
                    & (col("tard.campaign_id") == col("tad.campaign_id"))
                ),
                how="inner",
            )
            .join(
                ad_groups.alias("tag"),
                on=(
                    (col("tad.ad_group_id") == col("tag.ad_group_id"))
                    & (col("tad.campaign_id") == col("tag.campaign_id"))
                ),
                how="inner",
            )
            .join(
                campaigns.alias("tc"),
                on=(
                    (col("tag.campaign_id") == col("tc.campaign_id"))
                    & (col("tag.ad_account_id") == col("tc.ad_account_id"))
                ),
                how="inner",
            )
            .join(
                advertisers.alias("taa"),
                on=(col("tc.ad_account_id") == col("taa.ad_account_id")),
                how="inner",
            )
        )

        selected = joined.select(
            col("taa.ad_account_id"),
            col("taa.ad_account_name"),
            col("tc.campaign_id"),
            col("tc.campaign_name"),
            col("tc.campaign_status"),
            col("tc.campaign_operation_status"),
            col("tc.objective_type"),
            col("tc.budget"),
            col("tag.ad_group_id"),
            col("tag.optimization_goal"),
            col("tag.billing_event"),
            col("tad.ad_id"),
            col("tad.ad_name"),
            col("tad.ad_text"),
            col("tad.display_name"),
            col("tard.date"),
            col("tard.conversions"),
            col("tard.clicks"),
            col("tard.engagement"),
            col("tard.impressions"),
            col("tard.spend"),
            col("tard.purchase"),
            col("tard.shares"),
            col("tard.comments"),
            col("tard.complete_payment"),
            col("tard.clicks_on_music_disc"),
            col("tard.profile_visits"),
            col("tard.total_app_event_add_to_cart"),
            col("tard.registration"),
            col("tard.sales_lead"),
            col("tard.onsite_shopping"),
            col("tard.video_views_2s"),
            col("tard.video_views_6s"),
            col("tard.video_views_25p"),
            col("tard.video_views_50p"),
            col("tard.video_views_75p"),
            col("tard.video_views_100p"),
            col("tard.video_views"),
            col("tard.reach"),
        )

        grouped = selected.groupBy(
            col("campaign_id"),
            col("date"),
            col("billing_event"),
        ).agg(
            first(col("ad_account_id")).alias("ad_account_id"),
            first(col("ad_account_name")).alias("ad_account_name"),
            first(col("campaign_name")).alias("campaign_name"),
            first(col("campaign_status")).alias("campaign_status"),
            first(col("campaign_operation_status")).alias("campaign_operation_status"),
            first(col("objective_type")).alias("objective_type"),
            first(col("ad_group_id")).alias("ad_group_id"),
            first(col("optimization_goal")).alias("optimization_goal"),
            first(col("ad_id")).alias("ad_id"),
            first(col("ad_name")).alias("ad_name"),
            first(col("ad_text")).alias("ad_text"),
            first(col("display_name")).alias("display_name"),
            sum(col("budget")).alias("budget"),
            sum(col("conversions")).alias("conversions"),
            sum(col("clicks")).alias("clicks"),
            sum(col("engagement")).alias("engagement"),
            sum(col("impressions")).alias("impressions"),
            sum(col("spend")).alias("spend"),
            sum(col("purchase")).alias("purchase"),
            sum(col("shares")).alias("shares"),
            sum(col("comments")).alias("comments"),
            sum(col("complete_payment")).alias("complete_payment"),
            sum(col("clicks_on_music_disc")).alias("clicks_on_music_disc"),
            sum(col("profile_visits")).alias("profile_visits"),
            sum(col("total_app_event_add_to_cart")).alias("total_app_event_add_to_cart"),
            sum(col("registration")).alias("registration"),
            sum(col("sales_lead")).alias("sales_lead"),
            sum(col("onsite_shopping")).alias("onsite_shopping"),
            sum(col("video_views_2s")).alias("video_views_2s"),
            sum(col("video_views_6s")).alias("video_views_6s"),
            sum(col("video_views_25p")).alias("video_views_25p"),
            sum(col("video_views_50p")).alias("video_views_50p"),
            sum(col("video_views_75p")).alias("video_views_75p"),
            sum(col("video_views_100p")).alias("video_views_100p"),
            sum(col("video_views")).alias("video_views"),
            sum(col("reach")).alias("reach"),
        )

        return (
            grouped.withColumn(
                "frequency",
                (col("reach") / col("impressions")).cast("decimal(10,3)"),
            )
            .withColumn(
                "engagement_rate",
                (col("engagement") / col("impressions")).cast("decimal(10,3)"),
            )
            .withColumn("cpc", (col("spend") / col("clicks")).cast("decimal(10,3)"))
            .withColumn(
                "cpm",
                (col("spend") / col("impressions") * 1000).cast("decimal(10,3)"),
            )
            .withColumn("ctr", (col("clicks") / col("impressions")).cast("decimal(10,3)"))
            .withColumn(
                "cpv",
                (col("spend") / col("video_views_2s")).cast("decimal(10,3)"),
            )
            .withColumn(
                "cpvc",
                (col("spend") / col("video_views_100p")).cast("decimal(10,3)"),
            )
            .withColumn(
                "vtr",
                (col("video_views_2s") / col("impressions")).cast("decimal(10,3)"),
            )
            .withColumn(
                "vtrc",
                (col("video_views_100p") / col("video_views_2s")).cast("decimal(10,3)"),
            )
        )
