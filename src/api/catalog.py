import json
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession

from src.api.deps import get_spark
from src.catalog.tiktok_registry import (
    SUPPORTED_LAYERS,
    TIKTOK_TABLES,
    TableEntry,
    get_table,
    tables_for_layer,
)
from src.config import settings
from src.io.reader import read_delta

router = APIRouter(prefix="/catalog", tags=["catalog"])

PREVIEW_MAX_LIMIT = 100


def _bridge_mapping_payload(entry: TableEntry) -> list[dict[str, str]] | None:
    if not entry.bridge_mappings:
        return None
    return [
        {"column": mapping.column, "target": mapping.target}
        for mapping in entry.bridge_mappings
    ]


def _table_summary(entry: TableEntry) -> dict[str, Any]:
    bucket = settings.bucket_for_layer(entry.layer)
    return {
        "layer": entry.layer,
        "name": entry.name,
        "path": entry.path,
        "uri": settings.s3a_uri(bucket, entry.path),
        "format": "delta",
        "bridgeMapping": _bridge_mapping_payload(entry),
    }


def _validate_layer(layer: str) -> str:
    layer = layer.lower()
    if layer not in SUPPORTED_LAYERS:
        raise HTTPException(
            status_code=400,
            detail=f"Layer '{layer}' is not supported. Use silver or gold.",
        )
    return layer


def _resolve_table(layer: str, table: str) -> TableEntry:
    layer = _validate_layer(layer)
    entry = get_table(layer, table)
    if entry is None:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown table '{table}' for layer '{layer}'.",
        )
    return entry


def _load_dataframe(spark: SparkSession, entry: TableEntry) -> DataFrame:
    try:
        return read_delta(spark, entry.layer, entry.path)
    except AnalysisException as exc:
        raise HTTPException(
            status_code=404,
            detail=f"Table not found in MinIO: {entry.path}",
        ) from exc
    except Exception as exc:
        message = str(exc).lower()
        if "path does not exist" in message or "does not exist" in message:
            raise HTTPException(
                status_code=404,
                detail=f"Table not found in MinIO: {entry.path}",
            ) from exc
        raise


def _schema_columns(df: DataFrame) -> list[dict[str, Any]]:
    return [
        {
            "name": field.name,
            "type": field.dataType.simpleString(),
            "nullable": field.nullable,
        }
        for field in df.schema.fields
    ]


def _preview_rows(df: DataFrame, limit: int) -> list[dict[str, Any]]:
    return [json.loads(row) for row in df.limit(limit).toJSON().collect()]


@router.get("")
def list_catalog() -> dict[str, Any]:
    layers = sorted({entry.layer for entry in TIKTOK_TABLES})
    return {
        "platform": "tiktok",
        "layers": layers,
        "tables": [_table_summary(entry) for entry in TIKTOK_TABLES],
    }


@router.get("/{layer}")
def list_layer_tables(layer: str) -> dict[str, Any]:
    layer = _validate_layer(layer)
    entries = tables_for_layer(layer)
    return {
        "platform": "tiktok",
        "layer": layer,
        "tables": [_table_summary(entry) for entry in entries],
    }


@router.get("/{layer}/tiktok/{table}/schema")
def get_table_schema(
    layer: str,
    table: str,
    spark: SparkSession = Depends(get_spark),
) -> dict[str, Any]:
    entry = _resolve_table(layer, table)
    df = _load_dataframe(spark, entry)
    bucket = settings.bucket_for_layer(entry.layer)
    return {
        "platform": "tiktok",
        "layer": entry.layer,
        "name": entry.name,
        "path": entry.path,
        "uri": settings.s3a_uri(bucket, entry.path),
        "format": "delta",
        "bridgeMapping": _bridge_mapping_payload(entry),
        "columns": _schema_columns(df),
    }


@router.get("/{layer}/tiktok/{table}/details")
def get_table_details(
    layer: str,
    table: str,
    spark: SparkSession = Depends(get_spark),
) -> dict[str, Any]:
    entry = _resolve_table(layer, table)
    df = _load_dataframe(spark, entry)
    bucket = settings.bucket_for_layer(entry.layer)
    return {
        "platform": "tiktok",
        "layer": entry.layer,
        "name": entry.name,
        "path": entry.path,
        "uri": settings.s3a_uri(bucket, entry.path),
        "format": "delta",
        "bridgeMapping": _bridge_mapping_payload(entry),
        "rowCount": df.count(),
        "columns": _schema_columns(df),
    }


@router.get("/{layer}/tiktok/{table}/preview")
def preview_table(
    layer: str,
    table: str,
    limit: int = Query(default=10, ge=1, le=PREVIEW_MAX_LIMIT),
    spark: SparkSession = Depends(get_spark),
) -> dict[str, Any]:
    entry = _resolve_table(layer, table)
    df = _load_dataframe(spark, entry)
    return {
        "platform": "tiktok",
        "layer": entry.layer,
        "name": entry.name,
        "path": entry.path,
        "limit": limit,
        "rows": _preview_rows(df, limit),
    }
