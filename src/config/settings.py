"""
Configuração carregada do ambiente (.env).
Usada para conexão MinIO (leitura raw, escrita bronze/silver/gold).
"""
import os
from pathlib import Path

from dotenv import load_dotenv

# Carrega .env a partir da raiz do projeto (onde costuma estar o .env)
_env_path = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(_env_path)


def _get(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


class MinIOSettings:
    """Credenciais e endpoints MinIO (S3-compatible)."""

    endpoint: str = _get("MINIO_ENDPOINT", "http://localhost:9000")
    access_key: str = _get("MINIO_ROOT_USER", "minioadmin")
    secret_key: str = _get("MINIO_ROOT_PASSWORD", "minioadmin")

    # Buckets / prefixos por camada
    bucket_raw: str = _get("MINIO_BUCKET_RAW", "raw")
    bucket_bronze: str = _get("MINIO_BUCKET_BRONZE", "bronze")
    bucket_silver: str = _get("MINIO_BUCKET_SILVER", "silver")
    bucket_gold: str = _get("MINIO_BUCKET_GOLD", "gold")

    def s3a_uri(self, bucket: str, path: str = "") -> str:
        """URI S3A para uso no Spark (ex.: s3a://raw/ ou s3a://bronze/table)."""
        p = path.strip("/")
        return f"s3a://{bucket}/{p}" if p else f"s3a://{bucket}"


# Singleton de configuração
settings = MinIOSettings()
