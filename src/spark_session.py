"""
SparkSession com suporte a Delta Lake e conexão S3A para MinIO.
"""
from pyspark.sql import SparkSession

from src.config import settings


def get_spark_session(
    app_name: str = "binder-data-transform",
    master: str = "local[*]",
) -> SparkSession:
    """
    Cria SparkSession com Delta e MinIO (S3A).
    As credenciais e o endpoint vêm de src.config.settings (variáveis de ambiente).
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        # Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # MinIO (S3-compatible)
        .config("spark.hadoop.fs.s3a.access.key", settings.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.secret_key)
        .config("spark.hadoop.fs.s3a.endpoint", settings.endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Evitar conflito com hadoop-aws (alguns ambientes)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )

    return builder.getOrCreate()
