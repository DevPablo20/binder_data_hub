from pyspark.sql.functions import *
from utils.spark_session import get_spark_session
from utils.constants import *

spark = get_spark_session(True, "CreateGoldBasicTables")


def gold_google_daily_metrics():
    try:
        silver_customers_df = spark.read.format("delta").load(SILVER_CUSTOMERS)
        silver_campaigns_df = spark.read.format("delta").load(SILVER_CAMPAIGNS)
        silver_ad_groups_df = spark.read.format("delta").load(SILVER_AD_GROUPS)
        silver_ads_df = spark.read.format("delta").load(SILVER_ADS)
        silver_ad_group_ad_metrics_df = spark.read.format("delta").load(
            SILVER_AD_GROUP_AD_METRICS
        )

        selected_silver_ad_group_ad_metrics_df = silver_ad_group_ad_metrics_df.select(
            col("ad_account_id"),
            col("campaign_id"),
            col("ad_group_id"),
            col("ad_group_ad_ad_id"),
            col("metrics_active_view_impressions")
            .cast("integer")
            .alias("active_view_impressions"),
            (col("metrics_active_view_measurable_cost_micros") / 1000000)
            .cast("decimal(38,2)")
            .alias("active_view_measurable_cost"),
            col("metrics_active_view_measurable_impressions").alias(
                "active_view_measurable_impressions"
            ),
            col("metrics_all_conversions").cast(
                "integer").alias("all_conversions"),
            col("metrics_clicks").cast("integer").alias("clicks"),
            col("metrics_conversions").cast("integer").alias("conversions"),
            (col("metrics_cost_micros") / 1000000).cast("decimal(38,2)").alias("cost"),
            col("metrics_cross_device_conversions")
            .cast("integer")
            .alias("cross_device_conversions"),
            col("metrics_engagements").alias("engagements"),
            col("metrics_impressions").alias("impressions"),
            col("metrics_interactions").alias("interactions"),
            col("metrics_orders").alias("orders"),
            col("metrics_units_sold").alias("units_sold"),
            col("metrics_video_quartile_p100_rate").alias(
                "video_quartile_p100_rate"),
            col("metrics_video_quartile_p25_rate").alias(
                "video_quartile_p25_rate"),
            col("metrics_video_quartile_p50_rate").alias(
                "video_quartile_p50_rate"),
            col("metrics_video_quartile_p75_rate").alias(
                "video_quartile_p75_rate"),
            col("metrics_video_views").alias("video_views"),
            col("metrics_view_through_conversions").alias(
                "view_through_conversions"),
            col("segments_date").alias("date"),
            col("segments_ad_network_type").alias("ad_network_type"),
        )

        added_df = (
            selected_silver_ad_group_ad_metrics_df.withColumn(
                "video_views_p25",
                (col("impressions") * col("video_quartile_p25_rate")).cast("integer"),
            )
            .withColumn(
                "video_views_p50",
                (col("impressions") * col("video_quartile_p50_rate")).cast("integer"),
            )
            .withColumn(
                "video_views_p75",
                (col("impressions") * col("video_quartile_p75_rate")).cast("integer"),
            )
            .withColumn(
                "video_views_p100",
                (col("impressions") * col("video_quartile_p100_rate")).cast("integer"),
            )
        ).drop(
            col("video_quartile_p100_rate"),
            col("video_quartile_p25_rate"),
            col("video_quartile_p50_rate"),
            col("video_quartile_p75_rate"),
        )

        joined_df = (
            added_df.alias("metrics")
            .join(
                silver_ads_df.alias("ad"),
                on=(
                    (col("ad.ad_id") == col("metrics.ad_group_ad_ad_id"))
                    & (col("ad.ad_account_id") == col("metrics.ad_account_id"))
                ),
            )
            .join(
                silver_ad_groups_df.alias("gp"),
                on=(
                    (col("metrics.ad_group_id") == col("gp.ad_group_id"))
                    & (col("metrics.ad_account_id") == col("gp.ad_account_id"))
                ),
            )
            .join(
                silver_campaigns_df.alias("cp"),
                on=(
                    (col("gp.campaign_id") == col("cp.campaign_id"))
                    & (col("gp.ad_account_id") == col("cp.ad_account_id"))
                ),
            )
            .join(
                silver_customers_df.alias("aa"),
                on=(col("aa.ad_account_id") == col("cp.ad_account_id")),
            )
        )

        final_df = joined_df.select(
            col("aa.ad_account_id"),
            col("aa.ad_account_name"),
            col("aa.ad_account_status"),
            col("aa.ad_account_is_manager"),
            col("aa.ad_account_currecy_code"),
            col("cp.campaign_id"),
            col("cp.campaign_name"),
            col("cp.campaign_primary_status"),
            col("cp.campaign_serving_status"),
            col("cp.campaign_status"),
            col("cp.campaign_optimization_status"),
            col("cp.campaign_advertising_channel_type"),
            col("cp.campaign_bidding_strategy_type"),
            col("cp.campaign_start_date"),
            col("cp.campaign_end_date"),
            col("gp.ad_group_id"),
            col("gp.ad_group_name"),
            col("gp.ad_group_status"),
            col("gp.ad_group_primary_status"),
            col("gp.ad_group_type"),
            col("ad.ad_id"),
            col("ad.ad_name"),
            col("ad.ad_type"),
            col("ad.ad_display_url"),
            col("ad.ad_urls"),
            col("metrics.active_view_impressions"),
            col("metrics.active_view_measurable_cost"),
            col("metrics.active_view_measurable_impressions"),
            col("metrics.all_conversions"),
            col("metrics.clicks"),
            col("metrics.conversions"),
            col("metrics.cost"),
            col("metrics.cross_device_conversions"),
            col("metrics.engagements"),
            col("metrics.impressions"),
            col("metrics.interactions"),
            col("metrics.orders"),
            col("metrics.units_sold"),
            col("metrics.video_views_p100"),
            col("metrics.video_views_p25"),
            col("metrics.video_views_p50"),
            col("metrics.video_views_p75"),
            col("metrics.video_views"),
            col("metrics.view_through_conversions"),
            col("metrics.date"),
            col("metrics.ad_network_type"),
        )
        final_df = (
            final_df.withColumn(
                "buying_type",
                when(
                    (instr(lower(col("campaign_bidding_strategy_type")), "cpv") >= 1)
                    | (
                        instr(
                            lower(col("campaign_bidding_strategy_type")), "view") >= 1
                    ),
                    "CPV",
                )
                .when(
                    (instr(lower(col("campaign_bidding_strategy_type")), "cpc") >= 1)
                    | (
                        instr(lower(col("campaign_bidding_strategy_type")), "click")
                        >= 1
                    ),
                    "CPC",
                )
                .when(
                    (instr(lower(col("campaign_bidding_strategy_type")), "cpm") >= 1)
                    | (
                        instr(
                            lower(col("campaign_bidding_strategy_type")
                                  ), "impression"
                        )
                        >= 1
                    ),
                    "CPM",
                )
                .when(
                    (
                        instr(
                            lower(col("campaign_bidding_strategy_type")
                                  ), "target_spend"
                        )
                        >= 1
                    ),
                    "CPC",
                )
                .when(
                    (instr(lower(col("campaign_bidding_strategy_type")), "cpa") >= 1),
                    "CPA",
                )
                .when(
                    (
                        instr(
                            lower(col("campaign_bidding_strategy_type")),
                            "maximize_conversions",
                        )
                        >= 1
                    ),
                    "CPA",
                )
                .when(instr(lower(col("ad_network_type")), "search") >= 1, "CPC")
                .otherwise("unknown"),
            )
            .withColumn("platform", lit("google"))
            .withColumn(
                "sub_platform",
                when(
                    (col("campaign_advertising_channel_type") == "VIDEO")
                    & (col("ad_network_type") == "YOUTUBE"),
                    "youtube",
                )
                .when(
                    (col("ad_network_type") == "CONTENT")
                    & col("campaign_advertising_channel_type").isin(
                        "VIDEO", "DISPLAY", "SEARCH"
                    ),
                    "gdn",
                )
                .when(
                    (col("campaign_advertising_channel_type") == "SEARCH")
                    & (col("ad_network_type").isin("SEARCH", "SEARCH_PARTNERS")),
                    "search",
                )
                .when(
                    (col("campaign_advertising_channel_type") == "DEMAND_GEN")
                    & (col("ad_network_type") == "GOOGLE_OWNED_CHANNELS"),
                    "demand",
                )
                .otherwise("unknown"),
            )
        )

        final_df.write.format("delta").mode(
            "overwrite").save(GOLD_DAILY_METRICS)
    except Exception as e:
        raise Exception(
            f"Error while processing gold_google_daily_metrics") from e


def gold_google_geo_daily_metrics():
    try:
        silver_geographic_metrics = spark.read.format(
            "delta").load(SILVER_GEO_METRICS)

        selected_geographic_metrics = silver_geographic_metrics.select(
            col("ad_account_id"),
            col("campaign_id"),
            col("ad_group_id"),
            col("state_id"),
            col("metrics_all_conversions").alias("all_conversions"),
            col("metrics_clicks").alias("clicks"),
            col("metrics_conversions").alias("conversions"),
            col("metrics_cost").cast("decimal(38,2)").alias("cost"),
            col("metrics_cross_device_conversions").alias(
                "cross_device_conversions"),
            col("metrics_impressions").alias("impressions"),
            col("metrics_interactions").alias("interactions"),
            col("metrics_video_views").alias("video_views"),
            col("metrics_view_through_conversions").alias(
                "view_through_conversions"),
            col("segments_ad_network_type").alias("ad_network_type"),
            col("date"),
        )

        silver_geographic_constants = spark.read.format("delta").load(
            SILVER_GEO_CONSTANTS
        )

        silver_ad_groups = spark.read.format("delta").load(SILVER_AD_GROUPS)

        silver_campaigns = spark.read.format("delta").load(SILVER_CAMPAIGNS)

        silver_ad_accounts = spark.read.format("delta").load(SILVER_CUSTOMERS)

        joined_df = (
            selected_geographic_metrics.join(
                silver_geographic_constants, on="state_id")
            .join(silver_ad_groups, on=["ad_group_id", "campaign_id", "ad_account_id"])
            .join(silver_campaigns, on=["campaign_id", "ad_account_id"])
            .join(silver_ad_accounts, on=["ad_account_id"])
        )

        final_df = joined_df.select(
            col("ad_account_id"),
            col("ad_account_name"),
            col("ad_account_status"),
            col("ad_account_is_manager"),
            col("ad_account_currecy_code"),
            col("campaign_id"),
            col("campaign_name"),
            col("campaign_primary_status"),
            col("campaign_serving_status"),
            col("campaign_status"),
            col("campaign_optimization_status"),
            col("campaign_advertising_channel_type"),
            col("campaign_bidding_strategy_type"),
            col("campaign_start_date"),
            col("campaign_end_date"),
            col("ad_group_id"),
            col("ad_group_name"),
            col("ad_group_status"),
            col("ad_group_primary_status"),
            col("ad_group_type"),
            col("state_id"),
            col("state_name"),
            col("geographic_type"),
            col("all_conversions"),
            col("clicks"),
            col("conversions"),
            col("cost"),
            col("cross_device_conversions"),
            col("impressions"),
            col("interactions"),
            col("video_views"),
            col("view_through_conversions"),
            col("ad_network_type"),
            col("date"),
        )

        final_df = (
            final_df.withColumn(
                "buying_type",
                when(
                    (instr(lower(col("campaign_bidding_strategy_type")), "cpv") >= 1)
                    | (
                        instr(
                            lower(col("campaign_bidding_strategy_type")), "view") >= 1
                    ),
                    "CPV",
                )
                .when(
                    (instr(lower(col("campaign_bidding_strategy_type")), "cpc") >= 1)
                    | (
                        instr(lower(col("campaign_bidding_strategy_type")), "click")
                        >= 1
                    ),
                    "CPC",
                )
                .when(
                    (instr(lower(col("campaign_bidding_strategy_type")), "cpm") >= 1)
                    | (
                        instr(
                            lower(col("campaign_bidding_strategy_type")
                                  ), "impression"
                        )
                        >= 1
                    ),
                    "CPM",
                )
                .when(
                    (
                        instr(
                            lower(col("campaign_bidding_strategy_type")
                                  ), "target_spend"
                        )
                        >= 1
                    ),
                    "CPC",
                )
                .when(
                    (instr(lower(col("campaign_bidding_strategy_type")), "cpa") >= 1),
                    "CPA",
                )
                .when(
                    (
                        instr(
                            lower(col("campaign_bidding_strategy_type")),
                            "maximize_conversions",
                        )
                        >= 1
                    ),
                    "CPA",
                )
                .when(instr(lower(col("ad_network_type")), "search") >= 1, "CPC")
                .otherwise("unknown"),
            )
            .withColumn("platform", lit("google"))
            .withColumn(
                "sub_platform",
                when(
                    (col("campaign_advertising_channel_type") == "VIDEO")
                    & (col("ad_network_type") == "YOUTUBE"),
                    "youtube",
                )
                .when(
                    (col("ad_network_type") == "CONTENT")
                    & col("campaign_advertising_channel_type").isin(
                        "VIDEO", "DISPLAY", "SEARCH"
                    ),
                    "gdn",
                )
                .when(
                    (col("campaign_advertising_channel_type") == "SEARCH")
                    & (col("ad_network_type").isin("SEARCH", "SEARCH_PARTNERS")),
                    "search",
                )
                .when(
                    (col("campaign_advertising_channel_type") == "DEMAND_GEN")
                    & (col("ad_network_type") == "GOOGLE_OWNED_CHANNELS"),
                    "demand",
                )
                .otherwise("unknown"),
            )
        )

        final_df.write.format("delta").mode(
            "overwrite").save(GOLD_DAILY_GEO_METRICS)
    except Exception as e:
        raise Exception(
            f"Error while processing gold_google_geo_daily_metrics") from e


def gold_google_age_daily_metrics():
    try:
        silver_age_metrics_df = spark.read.format(
            "delta").load(SILVER_AGE_METRICS)
        silver_customers_df = spark.read.format("delta").load(SILVER_CUSTOMERS)
        silver_campaigns_df = spark.read.format("delta").load(SILVER_CAMPAIGNS)
        silver_ad_groups_df = spark.read.format("delta").load(SILVER_AD_GROUPS)

        selected_age_metrics = silver_age_metrics_df.select(
            col("ad_account_id"),
            col("campaign_id"),
            col("ad_group_id"),
            col("ad_group_criterion_age_range_type"),
            col("ad_group_criterion_criterion_id").alias("age_id"),
            col("metrics_active_view_impressions").alias(
                "active_view_impressions"),
            (col("metrics_active_view_measurable_cost_micros") / 1000000)
            .cast("decimal(38,2)")
            .alias("active_view_measurable_cost"),
            col("metrics_active_view_measurable_impressions").alias(
                "active_view_measurable_impressions"
            ),
            col("metrics_all_conversions").cast(
                "integer").alias("all_conversions"),
            col("metrics_clicks").alias("clicks"),
            col("metrics_conversions").cast("integer").alias("conversions"),
            (col("metrics_cost_micros") / 1000000).cast("decimal(38,2)").alias("cost"),
            col("metrics_cross_device_conversions")
            .cast("integer")
            .alias("cross_device_conversions"),
            col("metrics_engagements").alias("engagements"),
            col("metrics_impressions").alias("impressions"),
            col("metrics_view_through_conversions").alias(
                "view_through_conversions"),
            col("metrics_video_views").alias("video_views"),
            col("metrics_video_quartile_p75_rate").alias(
                "video_quartile_p75_rate"),
            col("metrics_video_quartile_p50_rate").alias(
                "video_quartile_p50_rate"),
            col("metrics_video_quartile_p25_rate").alias(
                "video_quartile_p25_rate"),
            col("metrics_video_quartile_p100_rate").alias(
                "video_quartile_p100_rate"),
            col("metrics_interactions").alias("interactions"),
            col("date"),
            col("segments_ad_network_type").alias("ad_network_type"),
        )

        added_df = (
            selected_age_metrics.withColumn(
                "video_views_p25",
                (col("impressions") * col("video_quartile_p25_rate")).cast("integer"),
            )
            .withColumn(
                "video_views_p50",
                (col("impressions") * col("video_quartile_p50_rate")).cast("integer"),
            )
            .withColumn(
                "video_views_p75",
                (col("impressions") * col("video_quartile_p75_rate")).cast("integer"),
            )
            .withColumn(
                "video_views_p100",
                (col("impressions") * col("video_quartile_p100_rate")).cast("integer"),
            )
        ).drop(
            col("video_quartile_p100_rate"),
            col("video_quartile_p25_rate"),
            col("video_quartile_p50_rate"),
            col("video_quartile_p75_rate"),
        )

        joined_df = (
            added_df.alias("age")
            .join(
                silver_ad_groups_df.alias("gp"),
                on=["ad_account_id", "campaign_id", "ad_group_id"],
            )
            .join(
                silver_campaigns_df.alias("cp"),
                on=["campaign_id", "ad_account_id"],
            )
            .join(
                silver_customers_df.alias("aa"),
                on=["ad_account_id"],
            )
        )

        final_df = joined_df.select(
            col("ad_account_id"),
            col("ad_account_name"),
            col("ad_account_status"),
            col("ad_account_is_manager"),
            col("ad_account_currecy_code"),
            col("campaign_id"),
            col("campaign_name"),
            col("campaign_primary_status"),
            col("campaign_serving_status"),
            col("campaign_status"),
            col("campaign_optimization_status"),
            col("campaign_advertising_channel_type"),
            col("campaign_bidding_strategy_type"),
            col("campaign_start_date"),
            col("campaign_end_date"),
            col("ad_group_id"),
            col("ad_group_name"),
            col("ad_group_status"),
            col("ad_group_primary_status"),
            col("ad_group_type"),
            col("age_id"),
            col("ad_group_criterion_age_range_type").alias("age_range"),
            col("active_view_impressions"),
            col("active_view_measurable_cost"),
            col("active_view_measurable_impressions"),
            col("all_conversions"),
            col("clicks"),
            col("conversions"),
            col("cost"),
            col("cross_device_conversions"),
            col("engagements"),
            col("impressions"),
            col("view_through_conversions"),
            col("video_views"),
            col("interactions"),
            col("video_views_p25"),
            col("video_views_p50"),
            col("video_views_p75"),
            col("video_views_p100"),
            col("date"),
            col("ad_network_type"),
        )

        final_df = (
            final_df.withColumn(
                "buying_type",
                when(
                    (instr(lower(col("campaign_bidding_strategy_type")), "cpv") >= 1)
                    | (
                        instr(
                            lower(col("campaign_bidding_strategy_type")), "view") >= 1
                    ),
                    "CPV",
                )
                .when(
                    (instr(lower(col("campaign_bidding_strategy_type")), "cpc") >= 1)
                    | (
                        instr(lower(col("campaign_bidding_strategy_type")), "click")
                        >= 1
                    ),
                    "CPC",
                )
                .when(
                    (instr(lower(col("campaign_bidding_strategy_type")), "cpm") >= 1)
                    | (
                        instr(
                            lower(col("campaign_bidding_strategy_type")
                                  ), "impression"
                        )
                        >= 1
                    ),
                    "CPM",
                )
                .when(
                    (
                        instr(
                            lower(col("campaign_bidding_strategy_type")
                                  ), "target_spend"
                        )
                        >= 1
                    ),
                    "CPC",
                )
                .when(
                    (instr(lower(col("campaign_bidding_strategy_type")), "cpa") >= 1),
                    "CPA",
                )
                .when(
                    (
                        instr(
                            lower(col("campaign_bidding_strategy_type")),
                            "maximize_conversions",
                        )
                        >= 1
                    ),
                    "CPA",
                )
                .when(instr(lower(col("ad_network_type")), "search") >= 1, "CPC")
                .otherwise("unknown"),
            )
            .withColumn("platform", lit("google"))
            .withColumn(
                "sub_platform",
                when(
                    (col("campaign_advertising_channel_type") == "VIDEO")
                    & (col("ad_network_type") == "YOUTUBE"),
                    "youtube",
                )
                .when(
                    (col("ad_network_type") == "CONTENT")
                    & col("campaign_advertising_channel_type").isin(
                        "VIDEO", "DISPLAY", "SEARCH"
                    ),
                    "gdn",
                )
                .when(
                    (col("campaign_advertising_channel_type") == "SEARCH")
                    & (col("ad_network_type").isin("SEARCH", "SEARCH_PARTNERS")),
                    "search",
                )
                .when(
                    (col("campaign_advertising_channel_type") == "DEMAND_GEN")
                    & (col("ad_network_type") == "GOOGLE_OWNED_CHANNELS"),
                    "demand",
                )
                .otherwise("unknown"),
            )
        )

        final_df.write.format("delta").mode(
            "overwrite").save(GOLD_DAILY_AGE_METRICS)
    except Exception as e:
        raise Exception(
            f"Error while processing gold_google_age_daily_metrics") from e


def google_google_gender_daily_metrics():
    try:
        silver_customers_df = spark.read.format("delta").load(SILVER_CUSTOMERS)
        silver_campaigns_df = spark.read.format("delta").load(SILVER_CAMPAIGNS)
        silver_ad_groups_df = spark.read.format("delta").load(SILVER_AD_GROUPS)
        silver_gender_metrics_df = spark.read.format("delta").load(
            SILVER_GENDER_METRICS
        )

        selected_gender_metrics_df = silver_gender_metrics_df.select(
            col("ad_account_id"),
            col("campaign_id"),
            col("ad_group_id"),
            col("ad_group_criterion_criterion_id").alias("gender_id"),
            col("ad_group_criterion_gender_type").alias("gender_type"),
            col("metrics_active_view_impressions").alias(
                "active_view_impressions"),
            (col("metrics_active_view_measurable_cost_micros") / 1000000)
            .cast("decimal(38,2)")
            .alias("active_view_measurable_cost"),
            col("metrics_active_view_measurable_impressions").alias(
                "active_view_measurable_impressions"
            ),
            col("metrics_all_conversions").alias("all_conversions"),
            col("metrics_clicks").alias("clicks"),
            col("metrics_conversions").alias("conversions"),
            (col("metrics_cost_micros") / 1000000).cast("decimal(38,2)").alias("cost"),
            col("metrics_engagements").alias("engagements"),
            col("metrics_impressions").alias("impressions"),
            col("metrics_interactions").alias("interactions"),
            col("metrics_search_impression_share").alias(
                "search_impression_share"),
            col("metrics_video_quartile_p100_rate").alias(
                "video_quartile_p100_rate"),
            col("metrics_video_quartile_p25_rate").alias(
                "video_quartile_p25_rate"),
            col("metrics_video_quartile_p50_rate").alias(
                "video_quartile_p50_rate"),
            col("metrics_video_quartile_p75_rate").alias(
                "video_quartile_p75_rate"),
            col("metrics_video_views").alias("video_views"),
            col("metrics_view_through_conversions").alias(
                "view_through_conversions"),
            col("date"),
            col("segments_ad_network_type").alias("ad_network_type"),
        )

        added_df = (
            selected_gender_metrics_df.withColumn(
                "video_views_p25",
                (col("impressions") * col("video_quartile_p25_rate")).cast("integer"),
            )
            .withColumn(
                "video_views_p50",
                (col("impressions") * col("video_quartile_p50_rate")).cast("integer"),
            )
            .withColumn(
                "video_views_p75",
                (col("impressions") * col("video_quartile_p75_rate")).cast("integer"),
            )
            .withColumn(
                "video_views_p100",
                (col("impressions") * col("video_quartile_p100_rate")).cast("integer"),
            )
        ).drop(
            col("video_quartile_p100_rate"),
            col("video_quartile_p25_rate"),
            col("video_quartile_p50_rate"),
            col("video_quartile_p75_rate"),
        )

        joined_df = (
            added_df.join(
                silver_ad_groups_df, on=[
                    "ad_group_id", "campaign_id", "ad_account_id"]
            )
            .join(silver_campaigns_df, on=["campaign_id", "ad_account_id"])
            .join(silver_customers_df, on=["ad_account_id"])
        )

        final_df = joined_df.select(
            col("ad_account_id"),
            col("ad_account_name"),
            col("ad_account_status"),
            col("ad_account_is_manager"),
            col("ad_account_currecy_code"),
            col("campaign_id"),
            col("campaign_name"),
            col("campaign_primary_status"),
            col("campaign_serving_status"),
            col("campaign_status"),
            col("campaign_optimization_status"),
            col("campaign_advertising_channel_type"),
            col("campaign_bidding_strategy_type"),
            col("campaign_start_date"),
            col("campaign_end_date"),
            col("ad_group_id"),
            col("ad_group_name"),
            col("ad_group_status"),
            col("ad_group_primary_status"),
            col("ad_group_type"),
            col("gender_id"),
            col("gender_type"),
            col("active_view_impressions"),
            col("active_view_measurable_cost"),
            col("active_view_measurable_impressions"),
            col("all_conversions"),
            col("clicks"),
            col("conversions"),
            col("cost"),
            col("engagements"),
            col("impressions"),
            col("interactions"),
            col("search_impression_share"),
            col("video_views"),
            col("view_through_conversions"),
            col("video_views_p25"),
            col("video_views_p50"),
            col("video_views_p75"),
            col("video_views_p100"),
            col("date"),
            col("ad_network_type"),
        )

        final_df = (
            final_df.withColumn(
                "buying_type",
                when(
                    (instr(lower(col("campaign_bidding_strategy_type")), "cpv") >= 1)
                    | (
                        instr(
                            lower(col("campaign_bidding_strategy_type")), "view") >= 1
                    ),
                    "CPV",
                )
                .when(
                    (instr(lower(col("campaign_bidding_strategy_type")), "cpc") >= 1)
                    | (
                        instr(lower(col("campaign_bidding_strategy_type")), "click")
                        >= 1
                    ),
                    "CPC",
                )
                .when(
                    (instr(lower(col("campaign_bidding_strategy_type")), "cpm") >= 1)
                    | (
                        instr(
                            lower(col("campaign_bidding_strategy_type")
                                  ), "impression"
                        )
                        >= 1
                    ),
                    "CPM",
                )
                .when(
                    (
                        instr(
                            lower(col("campaign_bidding_strategy_type")
                                  ), "target_spend"
                        )
                        >= 1
                    ),
                    "CPC",
                )
                .when(
                    (instr(lower(col("campaign_bidding_strategy_type")), "cpa") >= 1),
                    "CPA",
                )
                .when(
                    (
                        instr(
                            lower(col("campaign_bidding_strategy_type")),
                            "maximize_conversions",
                        )
                        >= 1
                    ),
                    "CPA",
                )
                .when(instr(lower(col("ad_network_type")), "search") >= 1, "CPC")
                .otherwise("unknown"),
            )
            .withColumn("platform", lit("google"))
            .withColumn(
                "sub_platform",
                when(
                    (col("campaign_advertising_channel_type") == "VIDEO")
                    & (col("ad_network_type") == "YOUTUBE"),
                    "youtube",
                )
                .when(
                    (col("ad_network_type") == "CONTENT")
                    & col("campaign_advertising_channel_type").isin(
                        "VIDEO", "DISPLAY", "SEARCH"
                    ),
                    "gdn",
                )
                .when(
                    (col("campaign_advertising_channel_type") == "SEARCH")
                    & (col("ad_network_type").isin("SEARCH", "SEARCH_PARTNERS")),
                    "search",
                )
                .when(
                    (col("campaign_advertising_channel_type") == "DEMAND_GEN")
                    & (col("ad_network_type") == "GOOGLE_OWNED_CHANNELS"),
                    "demand",
                )
                .otherwise("unknown"),
            )
        )

        final_df.write.format("delta").mode(
            "overwrite").save(GOLD_DAILY_GENDER_METRICS)
    except Exception as e:
        raise Exception(
            f"Error while processing gold_google_gender_daily_metrics"
        ) from e


def execute_gold_basic_dlt():
    try:
        gold_google_daily_metrics()
        gold_google_geo_daily_metrics()
        gold_google_age_daily_metrics()
        google_google_gender_daily_metrics()
    except Exception as e:
        raise Exception(f"Error while executing gold_basic dlt") from e


if __name__ == "__main__":
    execute_gold_basic_dlt()
