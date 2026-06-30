"""Transformer Bronze → Silver para TikTok Ads."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.transformers.base_silver import BronzeToSilverTransformer
from src.transformers.table_config import SilverTableConfig


class TikTokBronzeToSilverTransformer(BronzeToSilverTransformer):
    @property
    def platform_name(self) -> str:
        return "tiktok"

    def get_table_configs(self) -> list[SilverTableConfig]:
        tables = (
            "advertisers",
            "campaigns",
            "ad_groups",
            "ads",
            "ads_reports_daily",
        )
        return [
            SilverTableConfig(
                bronze_table_name=f"tiktok/{name}",
                silver_table_name=f"tiktok/{name}",
            )
            for name in tables
        ]

    def transform(self, df: DataFrame, config: SilverTableConfig) -> DataFrame:
        transforms = {
            "tiktok/advertisers": self._advertisers,
            "tiktok/campaigns": self._campaigns,
            "tiktok/ad_groups": self._ad_groups,
            "tiktok/ads": self._ads,
            "tiktok/ads_reports_daily": self._ads_reports_daily,
        }
        handler = transforms.get(config.silver_table_name)
        if handler is None:
            return df
        return handler(df)

    def _advertisers(self, df: DataFrame) -> DataFrame:
        return df.select(
            col("advertiser_id").cast("string").alias("ad_account_id"),
            col("name").alias("ad_account_name"),
            col("country"),
            col("currency"),
            col("role").alias("permissions"),
            col("create_time").cast("timestamp").alias("created_at"),
            col("_airbyte_raw_id"),
            col("_airbyte_extracted_at"),
            col("_airbyte_meta"),
        )

    def _campaigns(self, df: DataFrame) -> DataFrame:
        return df.select(
            col("campaign_id").cast("string").alias("campaign_id"),
            col("campaign_name"),
            col("campaign_type"),
            col("advertiser_id").cast("string").alias("ad_account_id"),
            col("budget").cast("decimal(10,3)").alias("budget"),
            col("budget_mode"),
            col("operation_status").alias("campaign_status"),
            col("secondary_status").alias("campaign_secondary_status"),
            col("operation_status").alias("campaign_operation_status"),
            col("objective_type"),
            col("budget_optimize_on"),
            col("is_new_structure"),
            col("create_time").cast("timestamp").alias("created_at"),
            col("modify_time").cast("timestamp").alias("updated_at"),
            col("roas_bid").cast("decimal(10,3)"),
            col("is_smart_performance_campaign"),
            col("is_search_campaign"),
            col("app_promotion_type"),
            col("_airbyte_raw_id"),
            col("_airbyte_extracted_at"),
            col("_airbyte_meta"),
        )

    def _ad_groups(self, df: DataFrame) -> DataFrame:
        return df.select(
            col("adgroup_id").cast("string").alias("ad_group_id"),
            col("campaign_id").cast("string").alias("campaign_id"),
            col("advertiser_id").cast("string").alias("ad_account_id"),
            col("adgroup_name").alias("ad_group_name"),
            col("audience_ids"),
            col("excluded_audience_ids"),
            col("location_ids"),
            col("interest_category_ids"),
            col("interest_keyword_ids"),
            col("device_model_ids"),
            col("category_exclusion_ids"),
            col("contextual_tag_ids"),
            col("isp_ids"),
            col("zipcode_ids"),
            col("age_groups"),
            col("placement_type"),
            col("placements"),
            col("inventory_filter_enabled"),
            col("comment_disabled"),
            col("promotion_type"),
            col("optimization_event"),
            col("creative_material_mode"),
            col("modify_time").cast("timestamp").alias("updated_at"),
            col("create_time").cast("timestamp").alias("created_at"),
            col("is_hfss"),
            col("gender"),
            col("languages"),
            col("operating_systems"),
            col("network_types"),
            col("device_price_ranges"),
            col("budget_mode"),
            col("budget").cast("decimal(10,3)").alias("budget"),
            col("schedule_type"),
            col("schedule_start_time"),
            col("schedule_end_time"),
            col("dayparting"),
            col("optimization_goal"),
            col("pacing"),
            col("billing_event"),
            col("skip_learning_phase"),
            col("bid_type"),
            col("bid_price").cast("decimal(10,3)").alias("bid_price"),
            col("conversion_bid_price").cast("decimal(10,3)").alias("conversion_bid_price"),
            col("deep_cpa_bid").cast("decimal(10,3)").alias("deep_cpa_bid"),
            col("secondary_status"),
            col("operation_status"),
            col("frequency"),
            col("frequency_schedule"),
            col("video_download_disabled"),
            col("is_new_structure"),
            col("is_smart_performance_campaign"),
            col("included_custom_actions"),
            col("excluded_custom_actions"),
            col("roas_bid").cast("decimal(10,3)").alias("roas_bid"),
            col("actions"),
            col("share_disabled"),
            col("auto_targeting_enabled"),
            col("ios14_quota_type"),
            col("bid_display_mode"),
            col("scheduled_budget").cast("decimal(10,3)").alias("scheduled_budget"),
            col("category_id").cast("string").alias("category_id"),
            col("search_result_enabled"),
            col("household_income"),
            col("spending_power"),
            col("_airbyte_raw_id").cast("string").alias("_airbyte_raw_id"),
            col("_airbyte_extracted_at"),
            col("_airbyte_meta"),
        )

    def _ads(self, df: DataFrame) -> DataFrame:
        return df.select(
            col("advertiser_id").cast("string").alias("ad_account_id"),
            col("campaign_id").cast("string").alias("campaign_id"),
            col("campaign_name"),
            col("adgroup_id").cast("string").alias("ad_group_id"),
            col("adgroup_name").alias("ad_group_name"),
            col("ad_id").cast("string").alias("ad_id"),
            col("ad_name"),
            col("image_ids"),
            col("call_to_action"),
            col("call_to_action_id").cast("string").alias("call_to_action_id"),
            col("secondary_status"),
            col("operation_status"),
            col("is_aco"),
            col("ad_format"),
            col("ad_text"),
            col("video_id").cast("string").alias("video_id"),
            col("tiktok_item_id").cast("string").alias("tiktok_item_id"),
            col("app_name"),
            col("display_name"),
            col("profile_image_url"),
            col("tracking_pixel_id").cast("string").alias("tracking_pixel_id"),
            col("deeplink"),
            col("deeplink_type"),
            col("fallback_type"),
            col("playable_url"),
            col("vast_moat_enabled"),
            col("creative_authorized"),
            col("is_new_structure"),
            col("create_time"),
            col("modify_time"),
            col("promotional_music_disabled"),
            col("avatar_icon_web_uri"),
            col("brand_safety_postbid_partner"),
            col("identity_id").cast("string").alias("identity_id"),
            col("identity_type"),
            col("optimization_event"),
            col("viewability_postbid_partner"),
            col("_airbyte_raw_id"),
            col("_airbyte_extracted_at"),
            col("_airbyte_meta"),
        )

    def _ads_reports_daily(self, df: DataFrame) -> DataFrame:
        return df.select(
            col("metrics.campaign_id").cast("string").alias("campaign_id"),
            col("metrics.adgroup_id").cast("string").alias("ad_group_id"),
            col("ad_id").cast("string").alias("ad_id"),
            col("stat_time_day"),
            col("metrics.impressions").cast("integer").alias("impressions"),
            col("metrics.real_time_cost_per_conversion")
            .cast("decimal(10,2)")
            .alias("real_time_cost_per_conversion"),
            col("metrics.mobile_app_id").cast("string").alias("mobile_app_id"),
            col("metrics.ctr").cast("decimal(10,2)").alias("ctr"),
            col("metrics.conversion_rate").cast("decimal(10,2)").alias("conversion_rate"),
            col("metrics.tt_app_id"),
            col("metrics.real_time_conversion_rate")
            .cast("decimal(10,2)")
            .alias("real_time_conversion_rate"),
            col("metrics.conversion").cast("integer").alias("conversion"),
            col("metrics.ad_text"),
            col("metrics.cost_per_conversion")
            .cast("decimal(10,2)")
            .alias("cost_per_conversion"),
            col("metrics.cpc").cast("decimal(10,2)").alias("cpc"),
            col("metrics.real_time_cost_per_result")
            .cast("decimal(10,2)")
            .alias("real_time_cost_per_result"),
            col("metrics.result_rate").cast("decimal(10,2)").alias("result_rate"),
            col("metrics.clicks").cast("integer").alias("clicks"),
            col("metrics.result").cast("integer").alias("result"),
            col("metrics.placement_type"),
            col("metrics.promotion_type"),
            col("metrics.tt_app_name"),
            col("metrics.spend").cast("decimal(10,2)").alias("spend"),
            col("metrics.cost_per_result").cast("decimal(10,2)").alias("cost_per_result"),
            col("metrics.real_time_conversion")
            .cast("integer")
            .alias("real_time_conversion"),
            col("metrics.real_time_result").cast("integer").alias("real_time_result"),
            col("metrics.cpm").cast("decimal(10,2)").alias("cpm"),
            col("metrics.real_time_result_rate")
            .cast("decimal(10,2)")
            .alias("real_time_result_rate"),
            col("metrics.cost_per_total_sales_lead")
            .cast("decimal(10,2)")
            .alias("cost_per_total_sales_lead"),
            col("metrics.purchase").cast("integer").alias("purchase"),
            col("metrics.cost_per_sales_lead")
            .cast("decimal(10,2)")
            .alias("cost_per_sales_lead"),
            col("metrics.registration_rate")
            .cast("decimal(10,2)")
            .alias("registration_rate"),
            col("metrics.cost_per_1000_reached")
            .cast("decimal(10,2)")
            .alias("cost_per_1000_reached"),
            col("metrics.clicks_on_music_disc").cast("integer").alias("clicks_on_music_disc"),
            col("metrics.vta_purchase").cast("integer").alias("vta_purchase"),
            col("metrics.frequency").cast("decimal(10,2)").alias("frequency"),
            col("metrics.purchase_rate").cast("decimal(10,2)").alias("purchase_rate"),
            col("metrics.shares").cast("integer").alias("shares"),
            col("metrics.total_purchase_value")
            .cast("decimal(10,2)")
            .alias("total_purchase_value"),
            col("metrics.cost_per_registration")
            .cast("decimal(10,2)")
            .alias("cost_per_registration"),
            col("metrics.comments").cast("integer").alias("comments"),
            col("metrics.app_install").cast("integer").alias("app_install"),
            col("metrics.complete_payment").cast("integer").alias("complete_payment"),
            col("metrics.profile_visits_rate")
            .cast("decimal(10,2)")
            .alias("profile_visits_rate"),
            col("metrics.average_video_play")
            .cast("decimal(10,2)")
            .alias("average_video_play"),
            col("metrics.profile_visits").cast("integer").alias("profile_visits"),
            col("metrics.value_per_complete_payment")
            .cast("decimal(10,2)")
            .alias("value_per_complete_payment"),
            col("metrics.total_app_event_add_to_cart")
            .cast("integer")
            .alias("total_app_event_add_to_cart"),
            col("metrics.average_video_play_per_user")
            .cast("decimal(10,2)")
            .alias("average_video_play_per_user"),
            col("metrics.cost_per_total_app_event_add_to_cart")
            .cast("decimal(10,2)")
            .alias("cost_per_total_app_event_add_to_cart"),
            col("metrics.cost_per_app_install")
            .cast("decimal(10,2)")
            .alias("cost_per_app_install"),
            col("metrics.registration").cast("integer").alias("registration"),
            col("metrics.reach").cast("integer").alias("reach"),
            col("metrics.sales_lead_rate").cast("decimal(10,2)").alias("sales_lead_rate"),
            col("metrics.sales_lead").cast("integer").alias("sales_lead"),
            col("metrics.total_complete_payment_rate")
            .cast("decimal(10,2)")
            .alias("total_complete_payment_rate"),
            col("metrics.total_onsite_shopping_value")
            .cast("decimal(10,2)")
            .alias("total_onsite_shopping_value"),
            col("metrics.video_views_p100").cast("integer").alias("video_views_p100"),
            col("metrics.video_views_p75").cast("integer").alias("video_views_p75"),
            col("metrics.video_watched_2s").cast("integer").alias("video_watched_2s"),
            col("metrics.video_watched_6s").cast("integer").alias("video_watched_6s"),
            col("metrics.video_views_p50").cast("integer").alias("video_views_p50"),
            col("metrics.video_views_p25").cast("integer").alias("video_views_p25"),
            col("metrics.video_play_actions").cast("integer").alias("video_play_actions"),
            col("metrics.onsite_shopping").cast("integer").alias("onsite_shopping"),
            col("metrics.real_time_app_install")
            .cast("decimal(10,2)")
            .alias("real_time_app_install"),
            col("metrics.real_time_app_install_cost")
            .cast("decimal(10,2)")
            .alias("real_time_app_install_cost"),
            col("metrics.cost_per_purchase").cast("decimal(10,2)").alias("cost_per_purchase"),
            col("_airbyte_raw_id"),
            col("_airbyte_extracted_at"),
            col("_airbyte_meta"),
        )
