from pyspark.sql.functions import *
from utils.spark_session import get_spark_session
from utils.constants import *

spark = get_spark_session(True, "CreateGoldBuyingTypeTables")


def gold_google_campaign_daily_metrics_by_buying_type():
    try:
        df = spark.read.format("delta").load(GOLD_DAILY_METRICS)

        grouped_df = df.groupBy(
            col("buying_type"), col("sub_platform"), col(
                "campaign_id"), col("date")
        ).agg(
            first(col("ad_account_id")).alias("ad_account_id"),
            first(col("ad_account_name")).alias("ad_account_name"),
            first(col("ad_account_status")).alias("ad_account_status"),
            first(col("ad_account_is_manager")).alias("ad_account_is_manager"),
            first(col("ad_account_currecy_code")).alias(
                "ad_account_currecy_code"),
            first(col("campaign_name")).alias("campaign_name"),
            first(col("campaign_primary_status")).alias(
                "campaign_primary_status"),
            first(col("campaign_serving_status")).alias(
                "campaign_serving_status"),
            first(col("campaign_status")).alias("campaign_status"),
            first(col("campaign_optimization_status")).alias(
                "campaign_optimization_status"
            ),
            first(col("campaign_advertising_channel_type")).alias(
                "campaign_advertising_channel_type"
            ),
            first(col("campaign_bidding_strategy_type")).alias(
                "campaign_bidding_strategy_type"
            ),
            first(col("campaign_start_date")).alias("campaign_start_date"),
            first(col("campaign_end_date")).alias("campaign_end_date"),
            sum(col("active_view_impressions")).alias(
                "active_view_impressions"),
            sum(col("active_view_measurable_cost")).alias(
                "active_view_measurable_cost"
            ),
            sum(col("active_view_measurable_impressions")).alias(
                "active_view_measurable_impressions"
            ),
            sum(col("all_conversions")).alias("all_conversions"),
            sum(col("clicks")).alias("clicks"),
            sum(col("conversions")).alias("conversions"),
            sum(col("cost")).alias("cost"),
            sum(col("cross_device_conversions")).alias(
                "cross_device_conversions"),
            sum(col("engagements")).alias("engagements"),
            sum(col("impressions")).alias("impressions"),
            sum(col("interactions")).alias("interactions"),
            sum(col("orders")).alias("orders"),
            sum(col("units_sold")).alias("units_sold"),
            sum(col("video_views_p25")).cast(
                "integer").alias("video_views_p25"),
            sum(col("video_views_p50")).cast(
                "integer").alias("video_views_p50"),
            sum(col("video_views_p75")).cast(
                "integer").alias("video_views_p75"),
            sum(col("video_views_p100")).cast(
                "integer").alias("video_views_p100"),
            sum(col("video_views")).alias("video_views"),
            sum(col("view_through_conversions")).alias(
                "view_through_conversions"),
        )

        final_df = (
            grouped_df.withColumn(
                "engagement_rate",
                (col("engagements") / col("impressions")).cast("decimal(10,2)"),
            )
            .withColumn("cpc", (col("cost") / col("clicks")).cast("decimal(10,2)"))
            .withColumn(
                "cpm", (col("cost") / col("impressions")
                        * 1000).cast("decimal(10,2)")
            )
            .withColumn(
                "ctr", (col("clicks") / col("impressions")
                        ).cast("decimal(10,2)")
            )
            .withColumn("cpv", (col("cost") / col("video_views")).cast("decimal(10,2)"))
            .withColumn(
                "vtr", (col("video_views") / col("impressions")
                        ).cast("decimal(10,2)")
            )
            .withColumn(
                "vtrc",
                (col("video_views_p100") / col("video_views")).cast("decimal(10,2)"),
            )
        )

        final_df.write.format("delta").mode("overwrite").save(
            GOLD_CAMPAIGN_DAILY_METRICS_BY_BUYING_TYPE
        )
    except Exception as e:
        raise Exception(
            f"Error while processing gold_google_campaign_daily_metrics_by_buying_type"
        ) from e


def gold_google_campaign_daily_geo_metrics_by_buying_type():
    try:
        df = spark.read.format("delta").load(GOLD_DAILY_GEO_METRICS)

        grouped_df = df.groupBy(
            col("buying_type"),
            col("sub_platform"),
            col("campaign_id"),
            col("date"),
            col("state_id"),
        ).agg(
            first(col("ad_account_id")).alias("ad_account_id"),
            first(col("ad_account_name")).alias("ad_account_name"),
            first(col("ad_account_status")).alias("ad_account_status"),
            first(col("ad_account_is_manager")).alias("ad_account_is_manager"),
            first(col("ad_account_currecy_code")).alias(
                "ad_account_currecy_code"),
            first(col("campaign_name")).alias("campaign_name"),
            first(col("campaign_primary_status")).alias(
                "campaign_primary_status"),
            first(col("campaign_serving_status")).alias(
                "campaign_serving_status"),
            first(col("campaign_status")).alias("campaign_status"),
            first(col("state_name")).alias("state_name"),
            sum(col("all_conversions")).alias("all_conversions"),
            sum(col("clicks")).alias("clicks"),
            sum(col("conversions")).alias("conversions"),
            sum(col("cost")).alias("cost"),
            sum(col("cross_device_conversions")).alias(
                "cross_device_conversions"),
            sum(col("impressions")).alias("impressions"),
            sum(col("interactions")).alias("interactions"),
            sum(col("video_views")).alias("video_views"),
            sum(col("view_through_conversions")).alias(
                "view_through_conversions"),
        )

        final_df = (
            grouped_df.withColumn(
                "cpc", (col("cost") / col("clicks")).cast("decimal(10,2)")
            )
            .withColumn(
                "cpm", (col("cost") / col("impressions")
                        * 1000).cast("decimal(10,2)")
            )
            .withColumn(
                "ctr", (col("clicks") / col("impressions")
                        ).cast("decimal(10,2)")
            )
            .withColumn("cpv", (col("cost") / col("video_views")).cast("decimal(10,2)"))
            .withColumn(
                "vtr", (col("video_views") / col("impressions")
                        ).cast("decimal(10,2)")
            )
        )

        final_df.write.format("delta").mode("overwrite").save(
            GOLD_CAMPAIGN_DAILY_GEO_METRICS_BY_BUYING_TYPE
        )

    except Exception as e:
        raise Exception(
            f"Error while processing gold_google_campaign_daily_geo_metrics_by_buying_type"
        ) from e


def gold_google_campaign_daily_age_metrics_by_buying_type():
    try:
        df = spark.read.format("delta").load(GOLD_DAILY_AGE_METRICS)

        grouped_df = df.groupBy(
            col("buying_type"),
            col("sub_platform"),
            col("campaign_id"),
            col("date"),
            col("age_id"),
        ).agg(
            first(col("ad_account_id")).alias("ad_account_id"),
            first(col("ad_account_name")).alias("ad_account_name"),
            first(col("ad_account_status")).alias("ad_account_status"),
            first(col("ad_account_is_manager")).alias("ad_account_is_manager"),
            first(col("ad_account_currecy_code")).alias(
                "ad_account_currecy_code"),
            first(col("campaign_name")).alias("campaign_name"),
            first(col("campaign_primary_status")).alias(
                "campaign_primary_status"),
            first(col("campaign_serving_status")).alias(
                "campaign_serving_status"),
            first(col("campaign_status")).alias("campaign_status"),
            first(col("age_range")).alias("age_range"),
            sum(col("active_view_impressions")).alias(
                "active_view_impressions"),
            sum(col("active_view_measurable_cost")).alias(
                "active_view_measurable_cost"
            ),
            sum(col("active_view_measurable_impressions")).alias(
                "active_view_measurable_impressions"
            ),
            sum(col("all_conversions")).alias("all_conversions"),
            sum(col("clicks")).alias("clicks"),
            sum(col("conversions")).alias("conversions"),
            sum(col("cost")).alias("cost"),
            sum(col("cross_device_conversions")).alias(
                "cross_device_conversions"),
            sum(col("engagements")).alias("engagements"),
            sum(col("impressions")).alias("impressions"),
            sum(col("view_through_conversions")).alias(
                "view_through_conversions"),
            sum(col("video_views")).alias("video_views"),
            sum(col("interactions")).alias("interactions"),
            sum(col("video_views_p25")).alias("video_views_p25"),
            sum(col("video_views_p50")).alias("video_views_p50"),
            sum(col("video_views_p75")).alias("video_views_p75"),
            sum(col("video_views_p100")).alias("video_views_p100"),
        )

        final_df = (
            grouped_df.withColumn(
                "engagement_rate",
                (col("engagements") / col("impressions")).cast("decimal(10,2)"),
            )
            .withColumn("cpc", (col("cost") / col("clicks")).cast("decimal(10,2)"))
            .withColumn(
                "cpm", (col("cost") / col("impressions")
                        * 1000).cast("decimal(10,2)")
            )
            .withColumn(
                "ctr", (col("clicks") / col("impressions")
                        ).cast("decimal(10,2)")
            )
            .withColumn("cpv", (col("cost") / col("video_views")).cast("decimal(10,2)"))
            .withColumn(
                "vtr", (col("video_views") / col("impressions")
                        ).cast("decimal(10,2)")
            )
            .withColumn(
                "vtrc",
                (col("video_views_p100") / col("video_views")).cast("decimal(10,2)"),
            )
        )

        final_df.write.format("delta").mode("overwrite").save(
            GOLD_CAMPAIGN_DAILY_AGE_METRICS_BY_BUYING_TYPE
        )

    except Exception as e:
        raise Exception(
            f"Error while processing gold_google_campaign_daily_age_metrics_by_buying_type"
        ) from e


def gold_google_campaign_daily_gender_metrics_by_buying_type():
    try:
        df = spark.read.format("delta").load(GOLD_DAILY_GENDER_METRICS)

        grouped_df = df.groupBy(
            col("buying_type"),
            col("sub_platform"),
            col("campaign_id"),
            col("date"),
            col("gender_id"),
        ).agg(
            first(col("ad_account_id")).alias("ad_account_id"),
            first(col("ad_account_name")).alias("ad_account_name"),
            first(col("ad_account_status")).alias("ad_account_status"),
            first(col("ad_account_is_manager")).alias("ad_account_is_manager"),
            first(col("ad_account_currecy_code")).alias(
                "ad_account_currecy_code"),
            first(col("campaign_name")).alias("campaign_name"),
            first(col("campaign_primary_status")).alias(
                "campaign_primary_status"),
            first(col("campaign_serving_status")).alias(
                "campaign_serving_status"),
            first(col("campaign_status")).alias("campaign_status"),
            first(col("gender_type")).alias("gender_type"),
            sum(col("active_view_impressions")).alias(
                "active_view_impressions"),
            sum(col("active_view_measurable_cost")).alias(
                "active_view_measurable_cost"
            ),
            sum(col("active_view_measurable_impressions")).alias(
                "active_view_measurable_impressions"
            ),
            sum(col("all_conversions")).alias("all_conversions"),
            sum(col("clicks")).alias("clicks"),
            sum(col("conversions")).alias("conversions"),
            sum(col("cost")).alias("cost"),
            sum(col("engagements")).alias("engagements"),
            sum(col("impressions")).alias("impressions"),
            sum(col("interactions")).alias("interactions"),
            sum(col("search_impression_share")).alias(
                "search_impression_share"),
            sum(col("video_views")).alias("video_views"),
            sum(col("view_through_conversions")).alias(
                "view_through_conversions"),
            sum(col("video_views_p25")).alias("video_views_p25"),
            sum(col("video_views_p50")).alias("video_views_p50"),
            sum(col("video_views_p75")).alias("video_views_p75"),
            sum(col("video_views_p100")).alias("video_views_p100"),
        )

        final_df = (
            grouped_df.withColumn(
                "engagement_rate",
                (col("engagements") / col("impressions")).cast("decimal(10,2)"),
            )
            .withColumn("cpc", (col("cost") / col("clicks")).cast("decimal(10,2)"))
            .withColumn(
                "cpm", (col("cost") / col("impressions")
                        * 1000).cast("decimal(10,2)")
            )
            .withColumn(
                "ctr", (col("clicks") / col("impressions")
                        ).cast("decimal(10,2)")
            )
            .withColumn("cpv", (col("cost") / col("video_views")).cast("decimal(10,2)"))
            .withColumn(
                "vtr", (col("video_views") / col("impressions")
                        ).cast("decimal(10,2)")
            )
            .withColumn(
                "vtrc",
                (col("video_views_p100") / col("video_views")).cast("decimal(10,2)"),
            )
        )

        final_df.write.format("delta").mode("overwrite").save(
            GOLD_CAMPAIGN_DAILY_GENDER_METRICS_BY_BUYING_TYPE
        )

    except Exception as e:
        raise Exception(
            f"Error while processing gold_google_campaign_daily_gender_metrics_by_buying_type"
        ) from e


def execute_gold_campaign_by_buying_type_dlt():
    try:
        gold_google_campaign_daily_metrics_by_buying_type()
        gold_google_campaign_daily_geo_metrics_by_buying_type()
        gold_google_campaign_daily_age_metrics_by_buying_type()
        gold_google_campaign_daily_gender_metrics_by_buying_type()
    except Exception as e:
        raise Exception(
            f"Error while executing gold_campaign_by_buying_type dlt"
        ) from e


if __name__ == "__main__":
    execute_gold_campaign_by_buying_type_dlt()
