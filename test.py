from src.spark_session import get_spark_session
from src.config import settings

spark = get_spark_session(app_name="minio-read-test")
path = settings.s3a_uri("raw", "airbyte/tiktok/ads")
print(f"Lendo: {path}")
try:
    df = spark.read.parquet(path)
    print("Linhas:", df.count())
    print(df.printSchema())
    print("OK: Spark leu o bucket no MinIO.")
except Exception as e:
    if "UNABLE_TO_INFER_SCHEMA" in str(e) or "Unable to infer schema" in str(e):
        print("OK: Conexão Spark + MinIO funcionando.")
        print("(Nenhum Parquet em path ou path vazio; verifique raw/airbyte/tiktok no MinIO.)")
    else:
        raise
finally:
    spark.stop()