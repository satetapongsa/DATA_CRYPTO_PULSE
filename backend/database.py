"""
Database Manager & Warehouse Query Abstraction Layer
Provides unified querying for PostgreSQL Star Schema with transparent SQLite fallback,
and MongoDB Document Store with transparent JSON Lakehouse fallback.
"""

import sqlite3
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional

from pipeline.config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    SQLITE_DB_PATH,
    JSON_LAKEHOUSE_DIR,
    MONGO_HOST,
    MONGO_PORT,
    MONGO_DB,
    MONGO_USER,
    MONGO_PASSWORD,
)

logger = logging.getLogger("backend.database")


class DatabaseManager:
    def __init__(self):
        self.pg_pool = None
        self.mongo_client = None
        self._init_postgres()
        self._init_mongo()

    def _init_postgres(self):
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                connect_timeout=2
            )
            conn.close()
            logger.info("PostgreSQL database connection verified.")
            self.has_postgres = True
        except Exception:
            self.has_postgres = False
            logger.info("Using SQLite Data Warehouse for local querying.")

    def _init_mongo(self):
        try:
            import pymongo
            connection_string = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin"
            client = pymongo.MongoClient(connection_string, serverSelectionTimeoutMS=1500)
            client.admin.command('ping')
            self.mongo_client = client[MONGO_DB]
            self.has_mongo = True
            logger.info("MongoDB Lakehouse connection verified.")
        except Exception:
            self.has_mongo = False
            self.mongo_client = None
            logger.info("Using JSON-Lakehouse store for local raw querying.")

    def get_sqlite_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn

    def query_warehouse(self, query_sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """
        Executes a SQL query against PostgreSQL if available, otherwise SQLite.
        Returns list of dicts.
        """
        # We ensure SQLite query compatibility by default
        try:
            conn = self.get_sqlite_conn()
            cur = conn.cursor()
            cur.execute(query_sql, params)
            rows = [dict(r) for r in cur.fetchall()]
            conn.close()
            return rows
        except Exception as e:
            logger.error(f"SQL Warehouse query error: {e} | Query: {query_sql}")
            return []

    def get_tables_list(self) -> List[Dict[str, Any]]:
        """
        Returns all tables and views with row counts.
        """
        tables = [
            {"name": "dim_cryptos", "type": "DIMENSION", "description": "Cryptocurrency assets metadata dimension"},
            {"name": "dim_signal_types", "type": "DIMENSION", "description": "Trading signal classification dimension"},
            {"name": "dim_dates", "type": "DIMENSION", "description": "Conformed Date Dimension (YYYYMMDD)"},
            {"name": "fact_price_history", "type": "FACT", "description": "Historical OHLCV + SMA, EMA, RSI, MACD"},
            {"name": "fact_signals", "type": "FACT", "description": "Algorithmic trading signals & confidence"},
            {"name": "fact_news_sentiment", "type": "FACT", "description": "NLP sentiment scored news events"},
            {"name": "v_latest_trading_signals", "type": "VIEW", "description": "Analytical view of joined latest signals"},
            {"name": "v_crypto_market_summary", "type": "VIEW", "description": "Market ticker summary with indicators"}
        ]

        conn = self.get_sqlite_conn()
        cur = conn.cursor()
        for t in tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {t['name']}")
                t["row_count"] = cur.fetchone()[0]
            except Exception:
                t["row_count"] = 0
        conn.close()
        return tables

    def get_raw_lakehouse_records(self, collection_name: str, limit: int = 25) -> List[Dict[str, Any]]:
        """
        Fetches raw JSON documents from MongoDB or local Lakehouse files.
        """
        if self.has_mongo and self.mongo_client is not None:
            try:
                cursor = self.mongo_client[collection_name].find({}, {"_id": 0}).sort("ingested_at", -1).limit(limit)
                return list(cursor)
            except Exception as e:
                logger.warning(f"Mongo query error: {e}")

        # Local JSON lakehouse fallback
        json_files = sorted(list(JSON_LAKEHOUSE_DIR.glob("*.json")), reverse=True)
        results = []
        key_mapping = {
            "raw_crypto_market": "prices",
            "raw_news_feed": "news",
            "pipeline_audit_logs": "metadata"
        }
        target_key = key_mapping.get(collection_name, "prices")

        for fpath in json_files[:5]:
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    val = data.get(target_key)
                    if isinstance(val, list):
                        results.extend(val)
                    elif isinstance(val, dict):
                        results.append(val)
            except Exception as e:
                logger.warning(f"Error reading JSON file {fpath}: {e}")

        return results[:limit]


# Global singleton DB Manager
db_manager = DatabaseManager()
