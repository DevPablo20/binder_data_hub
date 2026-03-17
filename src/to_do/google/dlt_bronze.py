from pyspark.sql.functions import *
from transform.transform_utils import remove_duplicated_data
from utils.spark_session import get_spark_session
from utils.constants import *

spark = get_spark_session(True, "CreateBronzeTables")


def bronze_google_customers():
    try:
        df = spark.read.format("parquet").load(CUSTOMERS_PARQUET)
        filtered_df = remove_duplicated_data(df, ["customer_id"])

        filtered_df.write.format("delta").mode(
            "overwrite").save(BRONZE_CUSTOMERS)
    except Exception as e:
        raise Exception(f"Error processing bronze_google_customers") from e


def bronze_google_campaigns():
    try:
        df = spark.read.format("parquet").load(CAMPAIGNS_PARQUET)
        filtered_df = remove_duplicated_data(
            df, ["customer_id", "campaign_id"])

        filtered_df.write.format("delta").mode(
            "overwrite").save(BRONZE_CAMPAIGNS)
    except Exception as e:
        raise Exception(f"Error processing bronze_google_campaigns") from e


def bronze_google_ad_groups():
    try:
        df = spark.read.format("parquet").load(AD_GROUPS_PARQUET)
        filtered_df = remove_duplicated_data(
            df, ["customer_id", "campaign_id", "ad_group_id"]
        )

        filtered_df.write.format("delta").mode(
            "overwrite").save(BRONZE_AD_GROUPS)
    except Exception as e:
        raise Exception(f"Error processing bronze_google_ad_groups") from e


def bronze_google_ads():
    try:
        df = spark.read.format("parquet").load(ADS_PARQUET)
        filtered_df = remove_duplicated_data(df, ["customer_id", "ad_id"])

        filtered_df.write.format("delta").mode("overwrite").save(BRONZE_ADS)
    except Exception as e:
        raise Exception(f"Error processing bronze_google_ads") from e


def bronze_google_ad_group_ad_metrics():
    try:
        df = spark.read.format("parquet").load(AD_GROUP_AD_METRICS_PARQUET)
        filtered_df = remove_duplicated_data(
            df,
            [
                "customer_id",
                "campaign_id",
                "ad_group_id",
                "ad_group_ad_ad_id",
                "segments_date",
                "segments_ad_network_type",
            ],
        )
        filtered_df.write.format("delta").mode(
            "overwrite").save(BRONZE_AD_GROUP_AD_METRICS)
    except Exception as e:
        raise Exception(f"Error processing bronze_google_ad_metrics") from e


def bronze_google_geographic_constants():
    try:
        df = spark.read.format("parquet").load(GEO_CONSTANTS_PARQUET)
        filtered_df = remove_duplicated_data(df, ["geo_target_constant_id"])

        filtered_df.write.format("delta").mode("overwrite").save(
            BRONZE_GEO_CONSTANTS
        )
    except Exception as e:
        raise Exception(
            f"Error processing bronze_google_geographic_constants") from e


def bronze_google_geo_metrics():
    try:
        df = spark.read.format("parquet").load(GEO_METRICS_PARQUET)

        filtered_df = remove_duplicated_data(
            df,
            [
                "segments_ad_network_type",
                "segments_date",
                "segments_geo_target_region",
                "ad_group_id",
            ],
        )

        filtered_df.write.format("delta").mode(
            "overwrite").save(BRONZE_GEO_METRICS)
    except Exception as e:
        raise Exception(f"Error processing bronze_google_geo_metrics") from e


def bronze_google_age_metrics():
    try:
        df = spark.read.format("parquet").load(AGE_METRICS_PARQUET)

        filtered_df = remove_duplicated_data(
            df,
            [
                "ad_group_id",
                "segments_date",
                "segments_ad_network_type",
                "ad_group_criterion_criterion_id",
            ],
        )

        filtered_df.write.format("delta").mode(
            "overwrite").save(BRONZE_AGE_METRICS)
    except Exception as e:
        raise Exception(f"Error processing bronze_google_age_metrics") from e


def bronze_google_gender_metrics():
    try:
        df = spark.read.format("parquet").load(GENDER_METRICS_PARQUET)

        filtered_df = remove_duplicated_data(
            df,
            [
                "ad_group_id",
                "segments_date",
                "segments_ad_network_type",
                "ad_group_criterion_criterion_id",
            ],
        )

        filtered_df.write.format("delta").mode(
            "overwrite").save(BRONZE_GENDER_METRICS)
    except Exception as e:
        raise Exception(
            f"Error processing bronze_google_gender_metrics") from e


def execute_bronze_dlt():
    try:
        bronze_google_customers()
        bronze_google_campaigns()
        bronze_google_ad_groups()
        bronze_google_ads()
        bronze_google_ad_group_ad_metrics()
        bronze_google_geographic_constants()
        bronze_google_geo_metrics()
        bronze_google_age_metrics()
        bronze_google_gender_metrics()
    except Exception as e:
        raise Exception(f'Error while executing bronze dlt') from e


if __name__ == "__main__":
    execute_bronze_dlt()
