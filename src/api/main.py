from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.api.catalog import router as catalog_router
from src.api.deps import shutdown_spark


@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield
    shutdown_spark()


app = FastAPI(
    title="Binder Data Hub Query API",
    description="Catalog, schema, and preview for TikTok silver and gold Delta tables.",
    lifespan=lifespan,
)
app.include_router(catalog_router)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
