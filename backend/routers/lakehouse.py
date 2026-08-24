"""
Lakehouse Explorer API Router
Provides transparent querying for Relational Data Warehouse tables (PostgreSQL/SQLite)
and Raw Non-Relational Document Store payloads (MongoDB/JSON Lakehouse).
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Dict, Any
from backend.database import db_manager
from backend.schemas import TableSchemaInfo, LakehouseTableResponse

router = APIRouter(prefix="/api/v1/lakehouse", tags=["Lakehouse Explorer"])


@router.get("/postgres/tables", response_model=List[TableSchemaInfo])
def list_warehouse_tables():
    """
    Returns list of all Data Warehouse Star Schema tables and analytical views.
    """
    return db_manager.get_tables_list()


@router.get("/postgres/{table_name}", response_model=LakehouseTableResponse)
def get_warehouse_table_data(
    table_name: str,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0)
):
    """
    Queries and returns tabular data for a specific relational warehouse table or view.
    """
    allowed_tables = [
        "dim_cryptos", "dim_signal_types", "dim_dates",
        "fact_price_history", "fact_signals", "fact_news_sentiment",
        "v_latest_trading_signals", "v_crypto_market_summary"
    ]
    if table_name not in allowed_tables:
        raise HTTPException(status_code=400, detail=f"Invalid table name. Allowed: {allowed_tables}")

    count_rows = db_manager.query_warehouse(f"SELECT COUNT(*) as cnt FROM {table_name}")
    total_rows = count_rows[0]["cnt"] if count_rows else 0

    data_rows = db_manager.query_warehouse(f"SELECT * FROM {table_name} LIMIT ? OFFSET ?", (limit, offset))
    columns = list(data_rows[0].keys()) if data_rows else []

    return LakehouseTableResponse(
        table_name=table_name,
        total_rows=total_rows,
        columns=columns,
        data=data_rows
    )


@router.get("/mongodb/collections")
def list_mongo_collections():
    """
    Returns available MongoDB collections / raw lakehouse entities.
    """
    return [
        {"name": "raw_crypto_market", "description": "Raw OHLCV API JSON responses before transformation", "store": "MongoDB / Lakehouse"},
        {"name": "raw_news_feed", "description": "Raw uncleaned crypto headlines and metadata", "store": "MongoDB / Lakehouse"},
        {"name": "pipeline_audit_logs", "description": "Raw pipeline execution audit envelopes", "store": "MongoDB / Lakehouse"}
    ]


@router.get("/mongodb/{collection_name}")
def get_raw_mongodb_documents(
    collection_name: str,
    limit: int = Query(20, ge=1, le=100)
):
    """
    Fetches raw JSON documents directly from MongoDB / Lakehouse store.
    """
    allowed_cols = ["raw_crypto_market", "raw_news_feed", "pipeline_audit_logs"]
    if collection_name not in allowed_cols:
        raise HTTPException(status_code=400, detail=f"Invalid collection. Allowed: {allowed_cols}")

    docs = db_manager.get_raw_lakehouse_records(collection_name, limit=limit)
    return {
        "collection": collection_name,
        "count": len(docs),
        "documents": docs
    }
