"""
SparkSession com suporte a Delta Lake e conexão S3A para MinIO.
"""

from pyspark.sql import SparkSession

from src.config import settings

# Delta + S3A (hadoop-aws + aws-java-sdk) em um único spark.jars.packages
# para que o Ivy baixe tudo e todos os JARs entrem no classpath.
SPARK_JARS_PACKAGES = (
    "io.delta:delta-spark_4.1_2.13:4.1.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)


def get_spark_session(
    app_name: str = "binder-data-transform",
    master: str = "local[*]",
) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.jars.packages", SPARK_JARS_PACKAGES)
        # Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # MinIO (S3-compatible). Forçar provider v1: Spark 4 pode injetar SDK v2 (EnvironmentVariableCredentialsProvider)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.access.key", settings.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.secret_key)
        .config("spark.hadoop.fs.s3a.endpoint", settings.endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Valores numéricos: hadoop-aws 3.3.x usa getLong() e não entende "60s" que o Spark 4 pode injetar
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.acquisition.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.idle.time", "60000")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
    )
    return builder.getOrCreate()

