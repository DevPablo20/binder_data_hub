"""
Teste de leitura de Parquet no bucket MinIO (S3A).
Uso: python -m scripts.test_minio_read
     ou, na raiz do projeto: python scripts/test_minio_read.py
"""
import sys
from pathlib import Path

# Garante que a raiz do projeto está no path
_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from utils.spark_session import get_spark_session

# Caminho no bucket raw (MinIO)
PARQUET_PATH = "s3a://raw/data/gads_custom_ad_groups/2026_03_05_1772744843599_0.parquet"


def main():
    spark = get_spark_session(True, "TestMinIORead")
    print(f"Lendo: {PARQUET_PATH}\n")

    df = spark.read.format("parquet").load(PARQUET_PATH)
    print("Schema:")
    df.printSchema()
    print("\nContagem de linhas:", df.count())
    print("\nAmostra (5 linhas):")
    df.show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
