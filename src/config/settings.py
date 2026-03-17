import os
from pathlib import Path

from dotenv import load_dotenv

_env_path = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(_env_path)


def _get(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


class MinIOSettings:

    endpoint: str = _get("MINIO_ENDPOINT", "http://localhost:9000")
    access_key: str = _get("MINIO_ROOT_USER", "minioadmin")
    secret_key: str = _get("MINIO_ROOT_PASSWORD", "minioadmin")

    bucket_raw: str = _get("MINIO_BUCKET_RAW", "raw")
    bucket_bronze: str = _get("MINIO_BUCKET_BRONZE", "bronze")
    bucket_silver: str = _get("MINIO_BUCKET_SILVER", "silver")
    bucket_gold: str = _get("MINIO_BUCKET_GOLD", "gold")

    def s3a_uri(self, bucket: str, path: str = "") -> str:
        p = path.strip("/")
        return f"s3a://{bucket}/{p}" if p else f"s3a://{bucket}"


settings = MinIOSettings()
