import os
from pathlib import Path

# Carrega .env na raiz do projeto (permite definir JAVA_HOME no .env)
_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=_env_path)

# PySpark 3.5+ exige Java 17 (class version 61). Preferir Java 17 sobre 11/8.
if not os.environ.get("JAVA_HOME"):
    for candidate in (
        "/usr/lib/jvm/java-17-openjdk-amd64",
        "/usr/lib/jvm/java-17-openjdk-arm64",
        "/usr/lib/jvm/default-java",
        "/usr/lib/jvm/java-11-openjdk-amd64",
        "/usr/lib/jvm/java-8-openjdk-amd64",
    ):
        if Path(candidate).exists():
            os.environ["JAVA_HOME"] = candidate
            break
    else:
        _jvm = Path("/usr/lib/jvm")
        if _jvm.exists():
            # Preferir pastas que contenham "17" no nome (Java 17)
            for _path in sorted(_jvm.iterdir()):
                if "17" in _path.name and (_path / "bin" / "java").exists():
                    os.environ["JAVA_HOME"] = str(_path)
                    break
            if not os.environ.get("JAVA_HOME"):
                for _path in sorted(_jvm.iterdir()):
                    if (_path / "bin" / "java").exists():
                        os.environ["JAVA_HOME"] = str(_path)
                        break
    if not os.environ.get("JAVA_HOME"):
        raise RuntimeError(
            "JAVA_HOME não definido e nenhum JDK encontrado. "
            "PySpark 3.5+ exige Java 17. Instale: sudo apt install openjdk-17-jdk"
        )

from pyspark.sql import SparkSession

S3A_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)


def get_spark_session(create_new: bool, app_name: str) -> SparkSession:
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
    secret_key = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", S3A_PACKAGES)
        # MinIO / S3
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Usa credenciais da config (access.key/secret.key). Evita ClassNotFoundException
        # do provider padrão (AWS SDK v2) quando só temos aws-java-sdk-bundle v1 no classpath.
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # Valores numéricos explícitos (em ms): evita NumberFormatException quando o S3A
        # recebe "60s" (ex.: fs.s3a.connection.acquisition.timeout / idle.time) e tenta Long
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "200000")
        .config("spark.hadoop.fs.s3a.connection.acquisition.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.idle.time", "60000")
        .config("spark.hadoop.fs.s3a.threads.max", "256")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        .config("spark.hadoop.fs.s3a.max.total.tasks", "1000")
        # initMultipartUploads: purge age em segundos (evita "24h" → NumberFormatException)
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
    )

    if create_new:
        return builder.getOrCreate()
    return builder.getOrCreate()
