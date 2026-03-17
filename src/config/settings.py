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

    def bucket_for_layer(self, layer: str) -> str:
        """Retorna o bucket MinIO para a camada (raw, bronze, silver, gold)."""
        layer = layer.lower()
        if layer == "raw":
            return self.bucket_raw
        if layer == "bronze":
            return self.bucket_bronze
        if layer == "silver":
            return self.bucket_silver
        if layer == "gold":
            return self.bucket_gold
        raise ValueError(f"Camada inválida: {layer}. Use raw, bronze, silver ou gold.")

    def s3a_uri(self, bucket: str, path: str = "") -> str:
        p = path.strip("/")
        return f"s3a://{bucket}/{p}" if p else f"s3a://{bucket}"


settings = MinIOSettings()
