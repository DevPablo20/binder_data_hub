from fastapi.testclient import TestClient

from src.api.main import app
from src.catalog.tiktok_registry import TIKTOK_TABLES

client = TestClient(app)


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_catalog_lists_silver_and_gold_tables():
    response = client.get("/catalog")
    assert response.status_code == 200
    payload = response.json()
    assert payload["platform"] == "tiktok"
    assert payload["layers"] == ["gold", "silver"]
    assert len(payload["tables"]) == len(TIKTOK_TABLES)


def test_catalog_rejects_unsupported_layer():
    response = client.get("/catalog/raw")
    assert response.status_code == 400
    assert "silver or gold" in response.json()["detail"]


def test_catalog_unknown_table_returns_404():
    response = client.get("/catalog/silver/tiktok/unknown/schema")
    assert response.status_code == 404
