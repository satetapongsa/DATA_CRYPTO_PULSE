"""
Raw Storage Layer (Data Lakehouse Ingestion)
Persists raw JSON/BSON payloads into MongoDB and local JSON Lakehouse files
for lineage, auditability, and replay capability.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

from pipeline.config import (
    MONGO_HOST,
    MONGO_PORT,
    MONGO_DB,
    MONGO_USER,
    MONGO_PASSWORD,
    JSON_LAKEHOUSE_DIR,
)

logger = logging.getLogger("pipeline.raw_storage")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


class RawDataLakeStore:
    def __init__(self):
        self.mongo_client = None
        self.db = None
        self._init_mongo_connection()

    def _init_mongo_connection(self):
        try:
            import pymongo
            connection_string = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin"
            self.mongo_client = pymongo.MongoClient(
                connection_string,
                serverSelectionTimeoutMS=2000
            )
            # Test ping
            self.mongo_client.admin.command('ping')
            self.db = self.mongo_client[MONGO_DB]
            logger.info(f"Connected to MongoDB Data Lakehouse at {MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}")
        except Exception as e:
            logger.warning(f"MongoDB not available ({e}). Using File-based Local Lakehouse Store.")
            self.mongo_client = None
            self.db = None

    def store_raw_batch(self, raw_data_batch: Dict[str, Any]) -> Dict[str, Any]:
        """
        Stores the raw extraction batch into MongoDB collections and file-based JSON Lakehouse.
        """
        batch_id = raw_data_batch["metadata"]["batch_id"]
        ingested_at = datetime.now(timezone.utc).isoformat()
        
        # 1. Always store into Local JSON Lakehouse File for persistent zero-dependency review
        file_path = JSON_LAKEHOUSE_DIR / f"{batch_id}.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(raw_data_batch, f, indent=2)
            
        logger.info(f"Persisted raw lakehouse batch to file: {file_path}")

        mongo_inserted_counts = {"raw_crypto_market": 0, "raw_news_feed": 0}

        # 2. Store to MongoDB if connected
        if self.db is not None:
            try:
                # Store market price records
                price_docs = []
                for p in raw_data_batch.get("prices", []):
                    doc = dict(p)
                    doc["batch_id"] = batch_id
                    doc["ingested_at"] = ingested_at
                    price_docs.append(doc)
                if price_docs:
                    res_p = self.db.raw_crypto_market.insert_many(price_docs)
                    mongo_inserted_counts["raw_crypto_market"] = len(res_p.inserted_ids)

                # Store news feed records
                news_docs = []
                for n in raw_data_batch.get("news", []):
                    doc = dict(n)
                    doc["batch_id"] = batch_id
                    doc["ingested_at"] = ingested_at
                    news_docs.append(doc)
                if news_docs:
                    res_n = self.db.raw_news_feed.insert_many(news_docs)
                    mongo_inserted_counts["raw_news_feed"] = len(res_n.inserted_ids)

                # Log pipeline audit in Mongo
                self.db.pipeline_audit_logs.insert_one({
                    "batch_id": batch_id,
                    "event": "RAW_INGESTION_COMPLETED",
                    "execution_timestamp": ingested_at,
                    "records_stored": mongo_inserted_counts
                })
                logger.info(f"Stored {mongo_inserted_counts} documents to MongoDB.")
            except Exception as e:
                logger.error(f"Error persisting to MongoDB: {e}")

        return {
            "batch_id": batch_id,
            "status": "STORED",
            "file_lakehouse_path": str(file_path),
            "mongo_records": mongo_inserted_counts
        }

    def get_latest_raw_documents(self, collection_name: str, limit: int = 25) -> List[Dict[str, Any]]:
        """
        Retrieves recent raw records from MongoDB, falling back to local file lakehouse.
        """
        if self.db is not None and collection_name in self.db.list_collection_names():
            try:
                cursor = self.db[collection_name].find({}, {"_id": 0}).sort("ingested_at", -1).limit(limit)
                return list(cursor)
            except Exception as e:
                logger.warning(f"Error fetching from MongoDB collection {collection_name}: {e}")

        # Fallback: Read from local JSON files
        json_files = sorted(list(JSON_LAKEHOUSE_DIR.glob("*.json")), reverse=True)
        if not json_files:
            return []
            
        combined_records = []
        key_map = {
            "raw_crypto_market": "prices",
            "raw_news_feed": "news",
            "pipeline_audit_logs": "metadata"
        }
        target_key = key_map.get(collection_name, "prices")

        for jf in json_files[:3]:
            try:
                with open(jf, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    items = data.get(target_key, [])
                    if isinstance(items, list):
                        combined_records.extend(items)
                    elif isinstance(items, dict):
                        combined_records.append(items)
            except Exception as e:
                logger.warning(f"Error reading file {jf}: {e}")

        return combined_records[:limit]


# Global singleton instance
raw_lake_store = RawDataLakeStore()

def store_raw_crypto_data(raw_data_batch: Dict[str, Any]) -> Dict[str, Any]:
    return raw_lake_store.store_raw_batch(raw_data_batch)


if __name__ == "__main__":
    from pipeline.extract import extract_all_crypto_data
    sample_batch = extract_all_crypto_data(["BTC"], days=3)
    res = store_raw_crypto_data(sample_batch)
    print("Storage result:", res)
