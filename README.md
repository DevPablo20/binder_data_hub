# binder_data_hub — TikTok Ads medallion ETL

Local ETL app: **Airbyte** extracts TikTok data into MinIO **raw**, **Spark** transforms through bronze → silver → gold, and **Airflow** schedules the pipeline.

## Stack

| Component | Role |
|-----------|------|
| Airbyte (`abctl`) | Extract TikTok Ads → `raw/airbyte/tiktok/` |
| MinIO | Medallion storage (`raw`, `bronze`, `silver`, `gold`) |
| Spark + Delta | Transformations in `src/transformers/` |
| Airflow | Orchestration via `dags/tiktok/tiktok_medallion_daily.py` |

## Project layout

```
binder_data_hub/
├── dags/tiktok/              # Airflow DAGs
├── src/
│   ├── transformers/tiktok/  # bronze, silver, gold
│   ├── pipelines/            # CLI: run_bronze | run_silver | run_gold
│   └── jobs/                 # thin entrypoints for Airflow
├── infra/minio/              # MinIO data volume
├── infra/airflow/            # Airflow Dockerfile
├── requirements/
│   ├── spark.txt
│   └── airflow.txt
└── airbyte/                  # abctl install docs
```

## Prerequisites

- Docker + Docker Compose
- Python 3.10+ (for local Spark runs outside containers)
- Java 17 (required by Spark)

## Setup

1. Copy env file and set a Fernet key for Airflow:

```bash
cp .env.example .env
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# paste result into AIRFLOW_FERNET_KEY in .env
```

2. Install Spark dependencies (local runs):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements/spark.txt
```

3. Start infra:

```bash
docker compose up -d
```

- MinIO API: http://localhost:9000 (console :9001)
- Airflow UI: http://localhost:8081 (default `admin` / `admin` from `.env`)

4. Install Airbyte and sync TikTok → MinIO `raw` bucket (see [airbyte/README.md](airbyte/README.md)).

## Medallion paths (TikTok)

| Layer | Path | Format |
|-------|------|--------|
| Raw | `raw/airbyte/tiktok/{stream}/` | Parquet |
| Bronze | `bronze/tiktok/{table}/` | Delta |
| Silver | `silver/tiktok/{table}/` | Delta |
| Gold | `gold/tiktok/{fact}/` | Delta |

Current streams: **advertisers**, **campaigns**, **ad_groups**, **ads**, **ads_reports_daily** under `airbyte/tiktok/`.

Gold output: **campaign_daily_metrics** (`gold/tiktok/campaign_daily_metrics`) — campaign-level daily rollups joined from all silver tables.

## Run pipelines manually

From repo root with `MINIO_ENDPOINT=http://localhost:9000`:

```bash
python -m src.pipelines.run_bronze tiktok
python -m src.pipelines.run_silver tiktok
python -m src.pipelines.run_gold tiktok
```

Inside Airflow containers, `MINIO_ENDPOINT=http://minio:9000` is set automatically.

## Airflow DAG

DAG id: **`tiktok_medallion_daily`**

```
sync_raw → bronze_tiktok → silver_tiktok → gold_tiktok
```

`sync_raw` is a placeholder — run Airbyte sync manually until `TIKTOK_AIRBYTE_JOB_ID` is wired to `AirbyteTriggerSyncOperator` (template commented in the DAG).

## Bridge ID contract (binder_app)

Silver normalizes columns for future Bridge joins:

| Silver column | Bridge mapping |
|---------------|----------------|
| `ad_account_id` | `platform_account.external_account_id` |
| `campaign_id` | `platform_object_map.external_id` (`campaign`) |
| `ad_group_id` | `platform_object_map.external_id` (`ad_set`) |
| `ad_id` | `platform_object_map.external_id` (`ad`) |

## Tests

```bash
pip install -r requirements/spark.txt pytest
pytest tests/
```
