"""
FastAPI Application Entry Point
Mounts API routers, CORS middleware, startup pipeline seeding,
and static frontend dashboard files.
"""

from contextlib import asynccontextmanager
from pathlib import Path
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from backend.routers import cryptos, analytics, pipeline, lakehouse
from backend.database import db_manager
from pipeline.orchestrator import orchestrator

logger = logging.getLogger("backend.main")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

BASE_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Check if database has data; if empty, run initial seeding pipeline
    logger.info("Initializing Crypto Intelligence Platform Backend...")
    try:
        tables = db_manager.get_tables_list()
        price_table = next((t for t in tables if t["name"] == "fact_price_history"), None)
        if not price_table or price_table["row_count"] == 0:
            logger.info("Empty database detected. Triggering initial bootstrap ETL pipeline...")
            orchestrator.execute_pipeline(days=30)
            logger.info("Initial bootstrap ETL pipeline completed successfully.")
        else:
            logger.info(f"Database contains existing data ({price_table['row_count']} price records).")
    except Exception as e:
        logger.error(f"Startup initialization error: {e}")

    yield

    # Shutdown
    logger.info("Shutting down Crypto Intelligence Platform Backend.")


app = FastAPI(
    title="Crypto Market Trends & Trading Signal Intelligence Platform",
    description="Production-grade Data Engineering REST API serving Star Schema Warehouse analytics, Airflow DAG metrics, and Raw MongoDB Lakehouse documents.",
    version="1.0.0",
    lifespan=lifespan
)

# Enable CORS for local development and dashboard access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register API Routers
app.include_router(cryptos.router)
app.include_router(analytics.router)
app.include_router(pipeline.router)
app.include_router(lakehouse.router)


@app.get("/health", tags=["Health"])
def health_check():
    return {
        "status": "HEALTHY",
        "service": "Crypto Intelligence API",
        "has_postgres": db_manager.has_postgres,
        "has_mongo": db_manager.has_mongo
    }


# Mount Static Frontend Files if frontend directory exists
if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")

    @app.get("/", include_in_schema=False)
    async def serve_index():
        return FileResponse(FRONTEND_DIR / "index.html")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)
