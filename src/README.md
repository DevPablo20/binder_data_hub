# Camada de transformação (Spark + Delta)

Leitura da camada **Raw** (Parquet no MinIO) e escrita nas camadas **Bronze**, **Silver** e **Gold** (Delta no MinIO). Orquestração futura via Airflow.

## Estrutura

- **config/settings.py** – Configuração MinIO (endpoint, credenciais, buckets) a partir do `.env`.
- **spark_session.py** – `SparkSession` com Delta Lake e S3A para MinIO.
- **io/reader.py** – Leitura de Parquet da Raw.
- **io/writer.py** – Escrita em Delta (ou Parquet) em Bronze/Silver/Gold.
- **transformers/** – Raw → Bronze por plataforma (classe abstrata + Google, Pinterest, TikTok).
- **pipelines/run.py** – Pipeline base: Raw → Bronze → Silver → Gold (transformações placeholder).
- **pipelines/run_bronze.py** – Executa apenas Raw → Bronze para uma plataforma.

## Pré-requisitos

- Python 3.10+
- Variáveis de ambiente (ou `.env` na raiz do projeto): `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `MINIO_ENDPOINT`; opcionalmente `MINIO_BUCKET_RAW`, `MINIO_BUCKET_BRONZE`, etc.
- MinIO com os buckets `raw`, `bronze`, `silver`, `gold` (o `docker-compose` do projeto já cria esses buckets via `minio-init`).

## Uso

Na raiz do projeto, com o MinIO no ar e dados em Parquet em `raw/` (ou em um subpath):

```bash
# Opcional: definir path dentro de raw e nome da tabela
export RAW_PATH=""           # subpath em raw (ex.: schema/table)
export TABLE_NAME="default_table"

python -m src.pipelines.run
```

Para rodar apenas Raw → Bronze com a estrutura de transformers (uma plataforma por vez):

```bash
python -m src.pipelines.run_bronze pinterest   # ou google | tiktok
```

Para rodar de dentro do diretório do projeto (para o `config` achar o `.env`):

```bash
cd /var/www/binder_data_hub
python -m src.pipelines.run
```

## Conexão MinIO

- **Origem (leitura):** `s3a://<MINIO_BUCKET_RAW>/<path>` (Parquet).
- **Destino (escrita):** `s3a://bronze/`, `s3a://silver/`, `s3a://gold/` (Delta).

O endpoint e as credenciais vêm de `MINIO_ENDPOINT`, `MINIO_ROOT_USER` e `MINIO_ROOT_PASSWORD`. Em ambiente Docker (serviço junto ao MinIO), use por exemplo `MINIO_ENDPOINT=http://minio:9000`; em máquina local, `http://localhost:9000`.
