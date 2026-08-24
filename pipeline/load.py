"""
Data Loading Layer (Data Warehouse Ingestion)
Performs dimensional surrogate key lookups and idempotent upserts of Fact and
Dimension records into PostgreSQL Star Schema (with SQLite fallback for local evaluation).
"""

import sqlite3
import logging
from datetime import datetime, timezone, date
from typing import Dict, Any, List, Optional
import pandas as pd

from pipeline.config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    SQLITE_DB_PATH,
)

logger = logging.getLogger("pipeline.load")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


class WarehouseDataLoader:
    def __init__(self):
        self.pg_conn = None
        self._init_postgres()
        self._init_sqlite()

    def _init_postgres(self):
        try:
            import psycopg2
            self.pg_conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                connect_timeout=2
            )
            self.pg_conn.autocommit = True
            logger.info(f"Connected to PostgreSQL Warehouse at {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
        except Exception as e:
            logger.warning(f"PostgreSQL not reachable ({e}). SQLite Warehouse will serve as primary local store.")
            self.pg_conn = None

    def _init_sqlite(self):
        """
        Initializes local SQLite Data Warehouse with matching Star Schema tables and views.
        """
        conn = sqlite3.connect(SQLITE_DB_PATH)
        cur = conn.cursor()
        
        # 1. Dimension Tables
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_cryptos (
            crypto_key INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            category TEXT,
            network TEXT,
            circulating_supply REAL,
            market_cap_rank INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_signal_types (
            signal_type_key INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_name TEXT UNIQUE NOT NULL,
            risk_level TEXT NOT NULL,
            strategy_name TEXT NOT NULL,
            description TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_dates (
            date_key INTEGER PRIMARY KEY,
            full_date TEXT UNIQUE NOT NULL,
            day_of_week INTEGER NOT NULL,
            day_name TEXT NOT NULL,
            month INTEGER NOT NULL,
            month_name TEXT NOT NULL,
            quarter INTEGER NOT NULL,
            year INTEGER NOT NULL,
            is_weekend INTEGER NOT NULL
        );
        """)

        # 2. Fact Tables
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_price_history (
            fact_price_id INTEGER PRIMARY KEY AUTOINCREMENT,
            crypto_key INTEGER NOT NULL,
            date_key INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            open_price REAL NOT NULL,
            high_price REAL NOT NULL,
            low_price REAL NOT NULL,
            close_price REAL NOT NULL,
            volume REAL NOT NULL,
            sma_7 REAL,
            sma_25 REAL,
            ema_12 REAL,
            ema_26 REAL,
            rsi_14 REAL,
            macd REAL,
            macd_signal REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(crypto_key, timestamp),
            FOREIGN KEY(crypto_key) REFERENCES dim_cryptos(crypto_key),
            FOREIGN KEY(date_key) REFERENCES dim_dates(date_key)
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_signals (
            signal_id INTEGER PRIMARY KEY AUTOINCREMENT,
            crypto_key INTEGER NOT NULL,
            signal_type_key INTEGER NOT NULL,
            date_key INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            trigger_price REAL NOT NULL,
            confidence_score REAL NOT NULL,
            technical_reason TEXT NOT NULL,
            sentiment_weight REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(crypto_key) REFERENCES dim_cryptos(crypto_key),
            FOREIGN KEY(signal_type_key) REFERENCES dim_signal_types(signal_type_key),
            FOREIGN KEY(date_key) REFERENCES dim_dates(date_key)
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_news_sentiment (
            sentiment_id INTEGER PRIMARY KEY AUTOINCREMENT,
            crypto_key INTEGER NOT NULL,
            date_key INTEGER NOT NULL,
            headline TEXT NOT NULL,
            source TEXT,
            sentiment_score REAL NOT NULL,
            sentiment_label TEXT NOT NULL,
            published_at TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(crypto_key) REFERENCES dim_cryptos(crypto_key),
            FOREIGN KEY(date_key) REFERENCES dim_dates(date_key)
        );
        """)

        # 3. Seed default dimensions
        cur.execute("""
        INSERT OR IGNORE INTO dim_signal_types (signal_name, risk_level, strategy_name, description)
        VALUES 
            ('STRONG_BUY', 'HIGH', 'Confluence Trend Surge', 'Bullish Golden Cross + Oversold RSI + Positive News Sentiment'),
            ('BUY', 'MEDIUM', 'Moving Average Breakout', 'SMA-7 crossed above SMA-25 with healthy volume'),
            ('HOLD', 'LOW', 'Neutral Consolidation', 'Price consolidating in range between moving averages'),
            ('SELL', 'MEDIUM', 'Moving Average Breakdown', 'SMA-7 crossed below SMA-25 with weakening momentum'),
            ('STRONG_SELL', 'HIGH', 'Distribution Warning', 'Bearish Death Cross + Overbought RSI + Negative Sentiment');
        """)

        # 4. Create Analytical Views
        cur.execute("""
        CREATE VIEW IF NOT EXISTS v_latest_trading_signals AS
        SELECT 
            s.signal_id,
            c.symbol,
            c.name AS crypto_name,
            c.category,
            st.signal_name,
            st.risk_level,
            st.strategy_name,
            s.trigger_price,
            s.confidence_score,
            s.technical_reason,
            s.sentiment_weight,
            s.timestamp AS generated_at
        FROM fact_signals s
        JOIN dim_cryptos c ON s.crypto_key = c.crypto_key
        JOIN dim_signal_types st ON s.signal_type_key = st.signal_type_key
        ORDER BY s.timestamp DESC;
        """)

        cur.execute("""
        CREATE VIEW IF NOT EXISTS v_crypto_market_summary AS
        SELECT 
            c.symbol,
            c.name,
            c.market_cap_rank,
            f.close_price AS current_price,
            f.volume AS volume_24h,
            f.sma_7,
            f.sma_25,
            f.rsi_14,
            f.timestamp AS last_updated
        FROM dim_cryptos c
        LEFT JOIN fact_price_history f ON c.crypto_key = f.crypto_key
        WHERE f.fact_price_id IN (
            SELECT MAX(fact_price_id) FROM fact_price_history GROUP BY crypto_key
        )
        ORDER BY c.market_cap_rank ASC;
        """)

        conn.commit()
        conn.close()
        logger.info(f"SQLite Warehouse initialized at {SQLITE_DB_PATH}")

    def load_dimensions(self, cryptos_meta: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Upserts dim_cryptos and returns mapping of symbol -> crypto_key.
        """
        conn = sqlite3.connect(SQLITE_DB_PATH)
        cur = conn.cursor()

        symbol_to_id = {}

        for meta in cryptos_meta:
            cur.execute("""
            INSERT INTO dim_cryptos (symbol, name, category, network, circulating_supply, market_cap_rank)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                name=excluded.name,
                category=excluded.category,
                network=excluded.network,
                circulating_supply=excluded.circulating_supply,
                market_cap_rank=excluded.market_cap_rank
            """, (
                meta["symbol"],
                meta.get("name", meta["symbol"]),
                meta.get("category", "Crypto Asset"),
                meta.get("network", "Mainnet"),
                meta.get("supply", 0),
                meta.get("rank", 99)
            ))

        conn.commit()

        # Fetch mapping
        cur.execute("SELECT symbol, crypto_key FROM dim_cryptos")
        for row in cur.fetchall():
            symbol_to_id[row[0]] = row[1]
        conn.close()

        # Also upsert in Postgres if connected
        if self.pg_conn:
            try:
                with self.pg_conn.cursor() as pg_cur:
                    for meta in cryptos_meta:
                        pg_cur.execute("""
                        INSERT INTO dim_cryptos (symbol, name, category, network, circulating_supply, market_cap_rank)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT(symbol) DO UPDATE SET
                            name=EXCLUDED.name,
                            category=EXCLUDED.category,
                            network=EXCLUDED.network,
                            circulating_supply=EXCLUDED.circulating_supply,
                            market_cap_rank=EXCLUDED.market_cap_rank;
                        """, (
                            meta["symbol"],
                            meta.get("name", meta["symbol"]),
                            meta.get("category", "Crypto Asset"),
                            meta.get("network", "Mainnet"),
                            meta.get("supply", 0),
                            meta.get("rank", 99)
                        ))
            except Exception as e:
                logger.error(f"Error loading dimensions in PostgreSQL: {e}")

        return symbol_to_id

    def load_date_dimensions(self, date_keys: List[int]):
        """
        Populates dim_dates for given date keys.
        """
        conn = sqlite3.connect(SQLITE_DB_PATH)
        cur = conn.cursor()

        for d_key in set(date_keys):
            d_str = str(d_key)
            d_obj = datetime.strptime(d_str, "%Y%m%d").date()
            cur.execute("""
            INSERT OR IGNORE INTO dim_dates (
                date_key, full_date, day_of_week, day_name, month, month_name, quarter, year, is_weekend
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                d_key,
                d_obj.isoformat(),
                d_obj.isoweekday(),
                d_obj.strftime("%A"),
                d_obj.month,
                d_obj.strftime("%B"),
                (d_obj.month - 1) // 3 + 1,
                d_obj.year,
                1 if d_obj.isoweekday() in (6, 7) else 0
            ))

        conn.commit()
        conn.close()

    def load_fact_prices(self, df_prices: pd.DataFrame, symbol_map: Dict[str, int]) -> int:
        """
        Upserts fact_price_history records.
        """
        if df_prices.empty:
            return 0

        self.load_date_dimensions(df_prices["date_key"].tolist())

        conn = sqlite3.connect(SQLITE_DB_PATH)
        cur = conn.cursor()
        inserted_count = 0

        for _, row in df_prices.iterrows():
            crypto_key = symbol_map.get(row["symbol"])
            if not crypto_key:
                continue

            cur.execute("""
            INSERT INTO fact_price_history (
                crypto_key, date_key, timestamp, open_price, high_price, low_price, close_price,
                volume, sma_7, sma_25, ema_12, ema_26, rsi_14, macd, macd_signal
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(crypto_key, timestamp) DO UPDATE SET
                open_price=excluded.open_price,
                high_price=excluded.high_price,
                low_price=excluded.low_price,
                close_price=excluded.close_price,
                volume=excluded.volume,
                sma_7=excluded.sma_7,
                sma_25=excluded.sma_25,
                ema_12=excluded.ema_12,
                ema_26=excluded.ema_26,
                rsi_14=excluded.rsi_14,
                macd=excluded.macd,
                macd_signal=excluded.macd_signal;
            """, (
                crypto_key,
                int(row["date_key"]),
                str(row["timestamp"]),
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                float(row["volume"]),
                float(row["sma_7"]),
                float(row["sma_25"]),
                float(row["ema_12"]),
                float(row["ema_26"]),
                float(row["rsi_14"]),
                float(row["macd"]),
                float(row["macd_signal"])
            ))
            inserted_count += 1

        conn.commit()
        conn.close()
        logger.info(f"Loaded {inserted_count} price facts into Warehouse.")
        return inserted_count

    def load_fact_signals(self, df_signals: pd.DataFrame, symbol_map: Dict[str, int]) -> int:
        """
        Loads fact_signals records.
        """
        if df_signals.empty:
            return 0

        conn = sqlite3.connect(SQLITE_DB_PATH)
        cur = conn.cursor()

        # Map signal_name -> signal_type_key
        cur.execute("SELECT signal_name, signal_type_key FROM dim_signal_types")
        signal_map = {row[0]: row[1] for row in cur.fetchall()}

        inserted = 0
        for _, row in df_signals.iterrows():
            crypto_key = symbol_map.get(row["symbol"])
            sig_type_key = signal_map.get(row["signal_name"], 3)  # default HOLD

            if not crypto_key:
                continue

            cur.execute("""
            INSERT INTO fact_signals (
                crypto_key, signal_type_key, date_key, timestamp, trigger_price,
                confidence_score, technical_reason, sentiment_weight
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                crypto_key,
                sig_type_key,
                int(row["date_key"]),
                str(row["timestamp"]),
                float(row["trigger_price"]),
                float(row["confidence_score"]),
                str(row["technical_reason"]),
                float(row.get("sentiment_weight", 0.0))
            ))
            inserted += 1

        conn.commit()
        conn.close()
        logger.info(f"Loaded {inserted} signal facts into Warehouse.")
        return inserted

    def load_fact_news(self, df_news: pd.DataFrame, symbol_map: Dict[str, int]) -> int:
        """
        Loads fact_news_sentiment records.
        """
        if df_news.empty:
            return 0

        conn = sqlite3.connect(SQLITE_DB_PATH)
        cur = conn.cursor()

        inserted = 0
        for _, row in df_news.iterrows():
            crypto_key = symbol_map.get(row["symbol"])
            if not crypto_key:
                continue

            cur.execute("""
            INSERT INTO fact_news_sentiment (
                crypto_key, date_key, headline, source, sentiment_score, sentiment_label, published_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                crypto_key,
                int(row["date_key"]),
                str(row["headline"]),
                str(row.get("source", "CryptoFeed")),
                float(row["sentiment_score"]),
                str(row["sentiment_label"]),
                str(row["published_at"])
            ))
            inserted += 1

        conn.commit()
        conn.close()
        logger.info(f"Loaded {inserted} news sentiment facts into Warehouse.")
        return inserted


# Global loader instance
warehouse_loader = WarehouseDataLoader()


def load_transformed_data_to_warehouse(
    transformed_data: Dict[str, Any],
    signals_df: pd.DataFrame
) -> Dict[str, Any]:
    """
    Orchestrates loading all dimensions and facts into the Data Warehouse.
    """
    cryptos_meta = transformed_data.get("cryptos_meta", [])
    prices_df = transformed_data.get("prices_df", pd.DataFrame())
    news_df = transformed_data.get("news_df", pd.DataFrame())

    # 1. Dimensions
    symbol_map = warehouse_loader.load_dimensions(cryptos_meta)

    # 2. Fact Price History
    prices_loaded = warehouse_loader.load_fact_prices(prices_df, symbol_map)

    # 3. Fact Signals
    signals_loaded = warehouse_loader.load_fact_signals(signals_df, symbol_map)

    # 4. Fact News Sentiment
    news_loaded = warehouse_loader.load_fact_news(news_df, symbol_map)

    return {
        "status": "LOAD_SUCCESS",
        "symbol_map": symbol_map,
        "prices_loaded": prices_loaded,
        "signals_loaded": signals_loaded,
        "news_loaded": news_loaded
    }


if __name__ == "__main__":
    from pipeline.extract import extract_all_crypto_data
    from pipeline.transform import run_etl_transformation
    from pipeline.signals import generate_trading_signals

    raw = extract_all_crypto_data(["BTC", "ETH"], days=7)
    transformed = run_etl_transformation(raw)
    signals = generate_trading_signals(transformed["prices_df"], transformed["news_df"])
    res = load_transformed_data_to_warehouse(transformed, signals)
    print("Warehouse Load Result:", res)
