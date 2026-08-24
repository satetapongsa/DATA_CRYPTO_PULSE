"""
Crypto Assets API Router
Endpoints for tracked cryptocurrencies and market ticker cards.
"""

from fastapi import APIRouter, HTTPException
from typing import List
from backend.database import db_manager
from backend.schemas import CryptoSummary

router = APIRouter(prefix="/api/v1/cryptos", tags=["Cryptocurrencies"])


@router.get("", response_model=List[CryptoSummary])
def get_all_cryptos():
    """
    Retrieves all tracked cryptocurrencies along with latest prices, volume, and indicators.
    """
    query = """
    SELECT 
        c.symbol,
        c.name,
        c.category,
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
    ) OR f.fact_price_id IS NULL
    ORDER BY c.market_cap_rank ASC;
    """
    rows = db_manager.query_warehouse(query)

    # Compute 24h change by comparing with previous candle
    results = []
    for r in rows:
        sym = r["symbol"]
        # Fetch previous price
        prev_rows = db_manager.query_warehouse("""
            SELECT close_price FROM fact_price_history f
            JOIN dim_cryptos c ON f.crypto_key = c.crypto_key
            WHERE c.symbol = ?
            ORDER BY f.timestamp DESC LIMIT 2;
        """, (sym,))

        pct_change = 0.0
        if len(prev_rows) >= 2:
            curr_p = float(prev_rows[0]["close_price"])
            prev_p = float(prev_rows[1]["close_price"])
            if prev_p > 0:
                pct_change = round(((curr_p - prev_p) / prev_p) * 100.0, 2)

        # Get latest news sentiment label
        sent_rows = db_manager.query_warehouse("""
            SELECT sentiment_label FROM fact_news_sentiment s
            JOIN dim_cryptos c ON s.crypto_key = c.crypto_key
            WHERE c.symbol = ?
            ORDER BY s.published_at DESC LIMIT 1;
        """, (sym,))
        sent_label = sent_rows[0]["sentiment_label"] if sent_rows else "NEUTRAL"

        item = {
            "symbol": r["symbol"],
            "name": r["name"],
            "category": r["category"],
            "market_cap_rank": r["market_cap_rank"],
            "current_price": r["current_price"],
            "change_24h_pct": pct_change,
            "volume_24h": r["volume_24h"],
            "sma_7": r["sma_7"],
            "sma_25": r["sma_25"],
            "rsi_14": r["rsi_14"],
            "sentiment_label": sent_label,
            "last_updated": r["last_updated"]
        }
        results.append(item)

    return results
