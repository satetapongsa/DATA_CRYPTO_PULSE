"""
Pipeline Configuration Module
Loads configuration from environment variables with sensible defaults.
Supports automatic dual-mode (Docker PostgreSQL/MongoDB or Local Embedded).
"""

import os
from pathlib import Path

# Base directories
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
SQLITE_DB_PATH = DATA_DIR / "crypto_warehouse.db"
JSON_LAKEHOUSE_DIR = DATA_DIR / "lakehouse_raw"

# Ensure data directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
JSON_LAKEHOUSE_DIR.mkdir(parents=True, exist_ok=True)

# Application Settings
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
DATA_STORAGE_MODE = os.getenv("DATA_STORAGE_MODE", "auto")  # 'postgres_mongo', 'local_embedded', 'auto'

# PostgreSQL Data Warehouse Settings
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_DB = os.getenv("POSTGRES_DB", "crypto_warehouse")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres_secure_pass")

# MongoDB Raw Lakehouse Settings
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_DB = os.getenv("MONGO_DB", "crypto_lakehouse")
MONGO_USER = os.getenv("MONGO_USER", "root")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "mongo_secure_pass")

# Tracked Crypto Assets
DEFAULT_SYMBOLS = ["BTC", "ETH", "SOL", "BNB", "ADA", "XRP"]
CRYPTO_SYMBOLS = os.getenv("CRYPTO_SYMBOLS", ",".join(DEFAULT_SYMBOLS)).split(",")
CRYPTO_SYMBOLS = [s.strip().upper() for s in CRYPTO_SYMBOLS if s.strip()]

# API Settings
BINANCE_API_URL = os.getenv("BINANCE_API_URL", "https://api.binance.com/api/v3")
COINGECKO_API_URL = os.getenv("COINGECKO_API_URL", "https://api.coingecko.com/api/v3")
ENABLE_LIVE_API = os.getenv("ENABLE_LIVE_API", "true").lower() == "true"
TIMEFRAME_DAYS = int(os.getenv("TIMEFRAME_DAYS", 30))
