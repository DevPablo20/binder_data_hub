from pyspark.sql.functions import *
from utils.spark_session import get_spark_session
from utils.constants import *

spark = get_spark_session(True, "CreateSilverTables")


def silver_google_customers():
    try:
        df = spark.read.format("delta").load(BRONZE_CUSTOMERS)

        selected_df = df.select(
            col("customer_id").cast("string").alias("ad_account_id"),
            col("customer_descriptive_name").alias("ad_account_name"),
            col("customer_status").alias("ad_account_status"),
            col("customer_manager").alias("ad_account_is_manager"),
            col("customer_currency_code").alias("ad_account_currecy_code"),
        )

        selected_df.write.format("delta").mode(
            "overwrite").save(SILVER_CUSTOMERS)
    except Exception as e:
        raise Exception(f"Error processing silver_google_customers") from e


def silver_google_campaigns():
    try:
        df = spark.read.format("delta").load(BRONZE_CAMPAIGNS)

        selected_df = df.select(
            col("customer_id").cast("string").alias("ad_account_id"),
            col("campaign_id").cast("string").alias("campaign_id"),
            col("campaign_name"),
            col("campaign_primary_status"),
            col("campaign_serving_status"),
            col("campaign_status"),
            col("campaign_ad_serving_optimization_status").alias(
                "campaign_optimization_status"
            ),
            col("campaign_advertising_channel_type"),
            col("campaign_bidding_strategy_type"),
            col("campaign_start_date"),
            col("campaign_end_date"),
            col("campaign_network_settings_target_content_network").alias(
                "campaign_allow_content"
            ),
            col("campaign_network_settings_target_google_search").alias(
                "campaign_allow_google_search"
            ),
            col("campaign_network_settings_target_google_tv_network").alias(
                "campaign_allow_google_tv"
            ),
            col("campaign_network_settings_target_partner_search_network").alias(
                "campaign_allow_partner_search"
            ),
            col("campaign_network_settings_target_search_network").alias(
                "campaign_allow_search"
            ),
            col("campaign_network_settings_target_youtube").alias(
                "campaign_allow_youtube"),
            col(
                "campaign_video_campaign_settings_video_ad_inventory_control_allow_in_feed"
            ).alias("campaign_allow_in_feed"),
            col(
                "campaign_video_campaign_settings_video_ad_inventory_control_allow_in_stream"
            ).alias("campaign_allow_in_stream"),
            col(
                "campaign_video_campaign_settings_video_ad_inventory_control_allow_shorts"
            ).alias("campaign_allow_shorts"),
        )

        selected_df.write.format("delta").mode(
            "overwrite").save(SILVER_CAMPAIGNS)
    except Exception as e:
        raise Exception(f"Error processing silver_google_campaigns") from e


def silver_google_ad_groups():
    try:
        df = spark.read.format("delta").load(BRONZE_AD_GROUPS)

        selected_df = df.select(
            col("customer_id").cast("string").alias("ad_account_id"),
            col("campaign_id").cast("string").alias("campaign_id"),
            col("ad_group_id").cast("string").alias("ad_group_id"),
            col("ad_group_name"),
            col("ad_group_status"),
            col("ad_group_primary_status"),
            col("ad_group_type"),
        )

        selected_df.write.format("delta").mode(
            "overwrite").save(SILVER_AD_GROUPS)
    except Exception as e:
        raise Exception(f"Error processing silver_google_ad_groups") from e


def silver_google_ads():
    try:
        df = spark.read.format("delta").load(BRONZE_ADS)

        selected_df = df.select(
            col("customer_id").cast("string").alias("ad_account_id"),
            col("ad_id").cast("string").alias("ad_id"),
            col("ad_name"),
            col("ad_type"),
            col("ad_display_url"),
            col("ad_final_urls").alias("ad_urls"),
        )

        selected_df.write.format("delta").mode("overwrite").save(SILVER_ADS)
    except Exception as e:
        raise Exception(f"Error processing silver_google_ads") from e


def silver_google_ad_group_ad_metrics():
    try:
        df = spark.read.format("delta").load(BRONZE_AD_GROUP_AD_METRICS)

        selected_df = df.select(
            col("customer_id").cast("string").alias("ad_account_id"),
            col("campaign_id").cast("string").alias("campaign_id"),
            col("ad_group_id").cast("string").alias("ad_group_id"),
            col("ad_group_ad_ad_id").cast("string").alias("ad_group_ad_ad_id"),
            col("metrics_absolute_top_impression_percentage")
            .cast("decimal(38,2)")
            .alias("metrics_absolute_top_impression_percentage"),
            col("metrics_active_view_cpm")
            .cast("decimal(38,2)")
            .alias("metrics_active_view_cpm"),
            col("metrics_active_view_ctr")
            .cast("decimal(38,2)")
            .alias("metrics_active_view_ctr"),
            col("metrics_active_view_impressions"),
            col("metrics_active_view_measurability")
            .cast("decimal(38,2)")
            .alias("metrics_active_view_measurability"),
            col("metrics_active_view_measurable_cost_micros"),
            col("metrics_active_view_measurable_impressions"),
            col("metrics_active_view_viewability")
            .cast("decimal(38,2)")
            .alias("metrics_active_view_viewability"),
            col("metrics_all_conversions")
            .cast("decimal(38,2)")
            .alias("metrics_all_conversions"),
            col("metrics_all_conversions_by_conversion_date")
            .cast("decimal(38,2)")
            .alias("metrics_all_conversions_by_conversion_date"),
            col("metrics_all_conversions_from_interactions_rate")
            .cast("decimal(38,2)")
            .alias("metrics_all_conversions_from_interactions_rate"),
            col("metrics_all_conversions_value")
            .cast("decimal(38,2)")
            .alias("metrics_all_conversions_value"),
            col("metrics_all_conversions_value_by_conversion_date")
            .cast("decimal(38,2)")
            .alias("metrics_all_conversions_value_by_conversion_date"),
            col("metrics_all_new_customer_lifetime_value")
            .cast("decimal(38,2)")
            .alias("metrics_all_new_customer_lifetime_value"),
            col("metrics_average_cart_size")
            .cast("decimal(38,2)")
            .alias("metrics_average_cart_size"),
            col("metrics_average_cost").cast(
                "decimal(38,2)").alias("metrics_average_cost"),
            col("metrics_average_cpc").cast(
                "decimal(38,2)").alias("metrics_average_cpc"),
            col("metrics_average_cpe").cast(
                "decimal(38,2)").alias("metrics_average_cpe"),
            col("metrics_average_cpm").cast(
                "decimal(38,2)").alias("metrics_average_cpm"),
            col("metrics_average_cpv").cast(
                "decimal(38,2)").alias("metrics_average_cpv"),
            col("metrics_average_order_value_micros"),
            col("metrics_average_page_views")
            .cast("decimal(38,2)")
            .alias("metrics_average_page_views"),
            col("metrics_average_time_on_site")
            .cast("decimal(38,2)")
            .alias("metrics_average_time_on_site"),
            col("metrics_bounce_rate").cast(
                "decimal(38,2)").alias("metrics_bounce_rate"),
            col("metrics_clicks"),
            col("metrics_conversions").cast(
                "decimal(38,2)").alias("metrics_conversions"),
            col("metrics_conversions_by_conversion_date")
            .cast("decimal(38,2)")
            .alias("metrics_conversions_by_conversion_date"),
            col("metrics_conversions_from_interactions_rate")
            .cast("decimal(38,2)")
            .alias("metrics_conversions_from_interactions_rate"),
            col("metrics_conversions_value")
            .cast("decimal(38,2)")
            .alias("metrics_conversions_value"),
            col("metrics_conversions_value_by_conversion_date")
            .cast("decimal(38,2)")
            .alias("metrics_conversions_value_by_conversion_date"),
            col("metrics_cost_micros").cast(
                "decimal(38,2)").alias("metrics_cost_micros"),
            col("metrics_cost_of_goods_sold_micros"),
            col("metrics_cost_per_all_conversions")
            .cast("decimal(38,2)")
            .alias("metrics_cost_per_all_conversions"),
            col("metrics_cost_per_conversion")
            .cast("decimal(38,2)")
            .alias("metrics_cost_per_conversion"),
            col("metrics_cost_per_current_model_attributed_conversion")
            .cast("decimal(38,2)")
            .alias("metrics_cost_per_current_model_attributed_conversion"),
            col("metrics_cross_device_conversions")
            .cast("decimal(38,2)")
            .alias("metrics_cross_device_conversions"),
            col("metrics_cross_sell_cost_of_goods_sold_micros"),
            col("metrics_cross_sell_gross_profit_micros"),
            col("metrics_cross_sell_revenue_micros"),
            col("metrics_cross_sell_units_sold")
            .cast("decimal(38,2)")
            .alias("metrics_cross_sell_units_sold"),
            col("metrics_ctr").cast("decimal(38,2)").alias("metrics_ctr"),
            col("metrics_current_model_attributed_conversions")
            .cast("decimal(38,2)")
            .alias("metrics_current_model_attributed_conversions"),
            col("metrics_current_model_attributed_conversions_value")
            .cast("decimal(38,2)")
            .alias("metrics_current_model_attributed_conversions_value"),
            col("metrics_engagement_rate")
            .cast("decimal(38,2)")
            .alias("metrics_engagement_rate"),
            col("metrics_engagements"),
            col("metrics_gmail_forwards"),
            col("metrics_gmail_saves"),
            col("metrics_gmail_secondary_clicks"),
            col("metrics_gross_profit_margin")
            .cast("decimal(38,2)")
            .alias("metrics_gross_profit_margin"),
            col("metrics_gross_profit_micros"),
            col("metrics_impressions"),
            col("metrics_interaction_event_types"),
            col("metrics_interaction_rate")
            .cast("decimal(38,2)")
            .alias("metrics_interaction_rate"),
            col("metrics_interactions"),
            col("metrics_lead_cost_of_goods_sold_micros"),
            col("metrics_lead_gross_profit_micros"),
            col("metrics_lead_revenue_micros"),
            col("metrics_lead_units_sold")
            .cast("decimal(38,2)")
            .alias("metrics_lead_units_sold"),
            col("metrics_new_customer_lifetime_value")
            .cast("decimal(38,2)")
            .alias("metrics_new_customer_lifetime_value"),
            col("metrics_orders").cast(
                "decimal(38,2)").alias("metrics_orders"),
            col("metrics_percent_new_visitors")
            .cast("decimal(38,2)")
            .alias("metrics_percent_new_visitors"),
            col("metrics_revenue_micros"),
            col("metrics_top_impression_percentage")
            .cast("decimal(38,2)")
            .alias("metrics_top_impression_percentage"),
            col("metrics_units_sold").cast(
                "decimal(38,2)").alias("metrics_units_sold"),
            col("metrics_value_per_all_conversions")
            .cast("decimal(38,2)")
            .alias("metrics_value_per_all_conversions"),
            col("metrics_value_per_all_conversions_by_conversion_date")
            .cast("decimal(38,2)")
            .alias("metrics_value_per_all_conversions_by_conversion_date"),
            col("metrics_value_per_conversion")
            .cast("decimal(38,2)")
            .alias("metrics_value_per_conversion"),
            col("metrics_value_per_conversions_by_conversion_date")
            .cast("decimal(38,2)")
            .alias("metrics_value_per_conversions_by_conversion_date"),
            col("metrics_value_per_current_model_attributed_conversion")
            .cast("decimal(38,2)")
            .alias("metrics_value_per_current_model_attributed_conversion"),
            col("metrics_video_quartile_p100_rate")
            .cast("decimal(38,4)")
            .alias("metrics_video_quartile_p100_rate"),
            col("metrics_video_quartile_p25_rate")
            .cast("decimal(38,4)")
            .alias("metrics_video_quartile_p25_rate"),
            col("metrics_video_quartile_p50_rate")
            .cast("decimal(38,4)")
            .alias("metrics_video_quartile_p50_rate"),
            col("metrics_video_quartile_p75_rate")
            .cast("decimal(38,4)")
            .alias("metrics_video_quartile_p75_rate"),
            col("metrics_video_view_rate")
            .cast("decimal(38,4)")
            .alias("metrics_video_view_rate"),
            col("metrics_video_views"),
            col("metrics_view_through_conversions"),
            col("segments_date"),
            col("segments_ad_network_type"),
        )

        selected_df.write.format("delta").mode(
            "overwrite").save(SILVER_AD_GROUP_AD_METRICS)
    except Exception as e:
        raise Exception(
            f"Error processing silver_google_ad_group_ad_metrics") from e


def silver_google_geographic_constants():
    try:
        df = spark.read.format("delta").load(BRONZE_GEO_CONSTANTS)
        selected_df = df.filter(col("geo_target_constant_target_type") == "State").select(
            col("geo_target_constant_id").cast("string").alias("state_id"),
            col("geo_target_constant_name").alias("state_name"),
            col("geo_target_constant_target_type").alias("geographic_type"),
        )

        selected_df.write.format("delta").mode(
            "overwrite").save(SILVER_GEO_CONSTANTS)
    except Exception as e:
        raise Exception(f"Error processing silver_google_geo_constants") from e


def silver_google_geographic_metrics():
    try:
        df = spark.read.format("delta").load(BRONZE_GEO_METRICS)

        selected_df = df.select(
            col("customer_id").cast("string").alias("ad_account_id"),
            col("campaign_id").cast("string").alias("campaign_id"),
            col("ad_group_id").cast("string").alias("ad_group_id"),
            col("metrics_all_conversions")
            .cast("decimal(38,2)")
            .alias("metrics_all_conversions"),
            col("metrics_all_conversions_from_interactions_rate")
            .cast("decimal(38,2)")
            .alias("metrics_all_conversions_from_interactions_rate"),
            col("metrics_all_conversions_value")
            .cast("decimal(38,2)")
            .alias("metrics_all_conversions_value"),
            col("metrics_average_cost").cast(
                "decimal(38,2)").alias("metrics_average_cost"),
            col("metrics_average_cpc").cast(
                "decimal(38,2)").alias("metrics_average_cpc"),
            col("metrics_average_cpm").cast(
                "decimal(38,2)").alias("metrics_average_cpm"),
            col("metrics_average_cpv").cast(
                "decimal(38,2)").alias("metrics_average_cpv"),
            col("metrics_clicks"),
            col("metrics_conversions").cast(
                "decimal(38,2)").alias("metrics_conversions"),
            col("metrics_conversions_from_interactions_rate")
            .cast("decimal(38,2)")
            .alias("metrics_conversions_from_interactions_rate"),
            col("metrics_conversions_value")
            .cast("decimal(38,2)")
            .alias("metrics_conversions_value"),
            (col("metrics_cost_micros") / 1000000)
            .cast("decimal(38,2)")
            .alias("metrics_cost"),
            col("metrics_cost_per_all_conversions")
            .cast("decimal(38,2)")
            .alias("metrics_cost_per_all_conversions"),
            col("metrics_cost_per_conversion")
            .cast("decimal(38,2)")
            .alias("metrics_cost_per_conversion"),
            col("metrics_cross_device_conversions")
            .cast("decimal(38,2)")
            .alias("metrics_cross_device_conversions"),
            col("metrics_ctr").cast("decimal(38,2)").alias("metrics_ctr"),
            col("metrics_impressions"),
            col("metrics_interaction_event_types"),
            col("metrics_interaction_rate")
            .cast("decimal(38,2)")
            .alias("metrics_interaction_rate"),
            col("metrics_interactions"),
            col("metrics_top_impression_percentage")
            .cast("decimal(38,2)")
            .alias("metrics_top_impression_percentage"),
            col("metrics_value_per_all_conversions")
            .cast("decimal(38,2)")
            .alias("metrics_value_per_all_conversions"),
            col("metrics_value_per_conversion")
            .cast("decimal(38,2)")
            .alias("metrics_value_per_conversion"),
            col("metrics_video_view_rate")
            .cast("decimal(38,2)")
            .alias("metrics_video_view_rate"),
            col("metrics_video_views"),
            col("metrics_view_through_conversions"),
            col("segments_ad_network_type"),
            regexp_extract(
                col("segments_geo_target_region"), r"geoTargetConstants/(\d+)", 1
            )
            .cast("string")
            .alias("state_id"),
            col("segments_date").alias("date"),
        )

        selected_df.write.format("delta").mode(
            "overwrite").save(SILVER_GEO_METRICS)
    except Exception as e:
        raise Exception(
            f"Error processing silver_google_geographic_metrics") from e


def silver_google_age_metrics():
    try:
        df = spark.read.format("delta").load(BRONZE_AGE_METRICS)

        selected_df = df.select(
            col("customer_id").cast("string").alias("ad_account_id"),
            col("campaign_id").cast("string").alias("campaign_id"),
            col("ad_group_id").cast("string").alias("ad_group_id"),
            col("ad_group_criterion_age_range_type"),
            col("ad_group_criterion_criterion_id")
            .cast("string")
            .alias("ad_group_criterion_criterion_id"),
            col("metrics_active_view_cpm")
            .cast("decimal(38,2)")
            .alias("metrics_active_view_cpm"),
            col("metrics_active_view_ctr")
            .cast("decimal(38,2)")
            .alias("metrics_active_view_ctr"),
            col("metrics_active_view_impressions").alias(
                "metrics_active_view_impressions"),
            col("metrics_active_view_measurability")
            .cast("decimal(38,2)")
            .alias("metrics_active_view_measurability"),
            col("metrics_active_view_measurable_cost_micros").alias(
                "metrics_active_view_measurable_cost_micros"
            ),
            col("metrics_active_view_measurable_impressions").alias(
                "metrics_active_view_measurable_impressions"
            ),
            col("metrics_active_view_viewability")
            .cast("decimal(38,2)")
            .alias("metrics_active_view_viewability"),
            col("metrics_all_conversions")
            .cast("decimal(38,2)")
            .alias("metrics_all_conversions"),
            col("metrics_all_conversions_from_interactions_rate")
            .cast("decimal(38,2)")
            .alias("metrics_all_conversions_from_interactions_rate"),
            col("metrics_all_conversions_from_interactions_value_per_interaction")
            .cast("decimal(38,2)")
            .alias("metrics_all_conversions_from_interactions_value_per_interaction"),
            col("metrics_all_conversions_value")
            .cast("decimal(38,2)")
            .alias("metrics_all_conversions_value"),
            col("metrics_all_conversions_value_per_cost")
            .cast("decimal(38,2)")
            .alias("metrics_all_conversions_value_per_cost"),
            col("metrics_average_cost").cast(
                "decimal(38,2)").alias("metrics_average_cost"),
            col("metrics_average_cpc").cast(
                "decimal(38,2)").alias("metrics_average_cpc"),
            col("metrics_average_cpe").cast(
                "decimal(38,2)").alias("metrics_average_cpe"),
            col("metrics_average_cpm").cast(
                "decimal(38,2)").alias("metrics_average_cpm"),
            col("metrics_average_cpv").cast(
                "decimal(38,2)").alias("metrics_average_cpv"),
            col("metrics_clicks").alias("metrics_clicks"),
            col("metrics_conversions").cast(
                "decimal(38,2)").alias("metrics_conversions"),
            col("metrics_conversions_from_interactions_rate")
            .cast("decimal(38,2)")
            .alias("metrics_conversions_from_interactions_rate"),
            col("metrics_conversions_value")
            .cast("decimal(38,2)")
            .alias("metrics_conversions_value"),
            col("metrics_cost_micros").cast(
                "decimal(38,2)").alias("metrics_cost_micros"),
            col("metrics_cost_per_all_conversions")
            .cast("decimal(38,2)")
            .alias("metrics_cost_per_all_conversions"),
            col("metrics_cost_per_conversion")
            .cast("decimal(38,2)")
            .alias("metrics_cost_per_conversion"),
            col("metrics_cross_device_conversions")
            .cast("decimal(38,2)")
            .alias("metrics_cross_device_conversions"),
            col("metrics_ctr").cast("decimal(38,2)").alias("metrics_ctr"),
            col("metrics_engagement_rate")
            .cast("decimal(38,2)")
            .alias("metrics_engagement_rate"),
            col("metrics_engagements").alias("metrics_engagements"),
            col("metrics_gmail_forwards").alias("metrics_gmail_forwards"),
            col("metrics_gmail_saves").alias("metrics_gmail_saves"),
            col("metrics_gmail_secondary_clicks").alias(
                "metrics_gmail_secondary_clicks"),
            col("metrics_impressions").alias("metrics_impressions"),
            col("metrics_view_through_conversions").alias(
                "metrics_view_through_conversions"
            ),
            col("metrics_video_views").alias("metrics_video_views"),
            col("metrics_video_view_rate")
            .cast("decimal(38,2)")
            .alias("metrics_video_view_rate"),
            col("metrics_video_quartile_p75_rate")
            .cast("decimal(38,4)")
            .alias("metrics_video_quartile_p75_rate"),
            col("metrics_video_quartile_p50_rate")
            .cast("decimal(38,4)")
            .alias("metrics_video_quartile_p50_rate"),
            col("metrics_video_quartile_p25_rate")
            .cast("decimal(38,4)")
            .alias("metrics_video_quartile_p25_rate"),
            col("metrics_video_quartile_p100_rate")
            .cast("decimal(38,4)")
            .alias("metrics_video_quartile_p100_rate"),
            col("metrics_value_per_conversion")
            .cast("decimal(38,2)")
            .alias("metrics_value_per_conversion"),
            col("metrics_value_per_all_conversions")
            .cast("decimal(38,2)")
            .alias("metrics_value_per_all_conversions"),
            col("metrics_interactions").alias("metrics_interactions"),
            col("metrics_interaction_rate")
            .cast("decimal(38,2)")
            .alias("metrics_interaction_rate"),
            col("metrics_interaction_event_types").alias(
                "metrics_interaction_event_types"),
            col("segments_date").alias("date"),
            col("segments_ad_network_type"),
        )

        selected_df.write.format("delta").mode(
            "overwrite").save(SILVER_AGE_METRICS)
    except Exception as e:
        raise Exception(f"Error processing silver_google_age_metrics") from e


def silver_google_gender_metrics():
    try:
        df = spark.read.format("delta").load(BRONZE_GENDER_METRICS)

        selected_df = df.select(
            col("customer_id").cast("string").alias("ad_account_id"),
            col("campaign_id").cast("string").alias("campaign_id"),
            col("ad_group_id").cast("string").alias("ad_group_id"),
            col("ad_group_criterion_criterion_id")
            .cast("string")
            .alias("ad_group_criterion_criterion_id"),
            col("ad_group_criterion_gender_type"),
            col("metrics_active_view_cpm")
            .cast("decimal(38,2)")
            .alias("metrics_active_view_cpm"),
            col("metrics_active_view_ctr")
            .cast("decimal(38,2)")
            .alias("metrics_active_view_ctr"),
            col("metrics_active_view_impressions").alias(
                "metrics_active_view_impressions"),
            col("metrics_active_view_measurability")
            .cast("decimal(38,2)")
            .alias("metrics_active_view_measurability"),
            col("metrics_active_view_measurable_cost_micros").alias(
                "metrics_active_view_measurable_cost_micros"
            ),
            col("metrics_active_view_measurable_impressions").alias(
                "metrics_active_view_measurable_impressions"
            ),
            col("metrics_active_view_viewability")
            .cast("decimal(38,2)")
            .alias("metrics_active_view_viewability"),
            col("metrics_all_conversions")
            .cast("decimal(38,2)")
            .alias("metrics_all_conversions"),
            col("metrics_all_conversions_from_interactions_rate")
            .cast("decimal(38,2)")
            .alias("metrics_all_conversions_from_interactions_rate"),
            col("metrics_all_conversions_from_interactions_value_per_interaction")
            .cast("decimal(38,2)")
            .alias("metrics_all_conversions_from_interactions_value_per_interaction"),
            col("metrics_all_conversions_value")
            .cast("decimal(38,2)")
            .alias("metrics_all_conversions_value"),
            col("metrics_all_conversions_value_per_cost")
            .cast("decimal(38,2)")
            .alias("metrics_all_conversions_value_per_cost"),
            col("metrics_average_cost").cast(
                "decimal(38,2)").alias("metrics_average_cost"),
            col("metrics_average_cpc").cast(
                "decimal(38,2)").alias("metrics_average_cpc"),
            col("metrics_average_cpe").cast(
                "decimal(38,2)").alias("metrics_average_cpe"),
            col("metrics_average_cpm").cast(
                "decimal(38,2)").alias("metrics_average_cpm"),
            col("metrics_average_cpv").cast(
                "decimal(38,2)").alias("metrics_average_cpv"),
            col("metrics_clicks").alias("metrics_clicks"),
            col("metrics_conversions").cast(
                "decimal(38,2)").alias("metrics_conversions"),
            col("metrics_conversions_from_interactions_rate")
            .cast("decimal(38,2)")
            .alias("metrics_conversions_from_interactions_rate"),
            col("metrics_conversions_value")
            .cast("decimal(38,2)")
            .alias("metrics_conversions_value"),
            col("metrics_cost_micros").alias("metrics_cost_micros"),
            col("metrics_cost_per_all_conversions")
            .cast("decimal(38,2)")
            .alias("metrics_cost_per_all_conversions"),
            col("metrics_cost_per_conversion")
            .cast("decimal(38,2)")
            .alias("metrics_cost_per_conversion"),
            col("metrics_cross_device_conversions")
            .cast("decimal(38,2)")
            .alias("metrics_cross_device_conversions"),
            col("metrics_ctr").cast("decimal(38,2)").alias("metrics_ctr"),
            col("metrics_engagement_rate")
            .cast("decimal(38,2)")
            .alias("metrics_engagement_rate"),
            col("metrics_engagements").alias("metrics_engagements"),
            col("metrics_gmail_forwards").alias("metrics_gmail_forwards"),
            col("metrics_gmail_saves").alias("metrics_gmail_saves"),
            col("metrics_gmail_secondary_clicks").alias(
                "metrics_gmail_secondary_clicks"),
            col("metrics_impressions").alias("metrics_impressions"),
            col("metrics_interaction_event_types").alias(
                "metrics_interaction_event_types"),
            col("metrics_interaction_rate")
            .cast("decimal(38,2)")
            .alias("metrics_interaction_rate"),
            col("metrics_interactions").alias("metrics_interactions"),
            col("metrics_search_impression_share")
            .cast("decimal(38,2)")
            .alias("metrics_search_impression_share"),
            col("metrics_value_per_all_conversions")
            .cast("decimal(38,2)")
            .alias("metrics_value_per_all_conversions"),
            col("metrics_value_per_conversion")
            .cast("decimal(38,2)")
            .alias("metrics_value_per_conversion"),
            col("metrics_video_quartile_p100_rate")
            .cast("decimal(38,4)")
            .alias("metrics_video_quartile_p100_rate"),
            col("metrics_video_quartile_p25_rate")
            .cast("decimal(38,4)")
            .alias("metrics_video_quartile_p25_rate"),
            col("metrics_video_quartile_p50_rate")
            .cast("decimal(38,4)")
            .alias("metrics_video_quartile_p50_rate"),
            col("metrics_video_quartile_p75_rate")
            .cast("decimal(38,4)")
            .alias("metrics_video_quartile_p75_rate"),
            col("metrics_video_view_rate")
            .cast("decimal(38,2)")
            .alias("metrics_video_view_rate"),
            col("metrics_video_views").alias("metrics_video_views"),
            col("metrics_view_through_conversions").alias(
                "metrics_view_through_conversions"
            ),
            col("segments_date").alias("date"),
            col("segments_ad_network_type"),
        )

        selected_df.write.format("delta").mode(
            "overwrite").save(SILVER_GENDER_METRICS)
    except Exception as e:
        raise Exception(
            f"Error processing silver_google_gender_metrics") from e


def execute_silver_dlt():
    try:
        silver_google_customers()
        silver_google_campaigns()
        silver_google_ad_groups()
        silver_google_ads()
        silver_google_ad_group_ad_metrics()
        silver_google_geographic_constants()
        silver_google_geographic_metrics()
        silver_google_age_metrics()
        silver_google_gender_metrics()
    except Exception as e:
        raise Exception(f'Error while executing silver dlt') from e


if __name__ == "__main__":
    execute_silver_dlt()
