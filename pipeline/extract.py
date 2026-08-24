"""
Data Extraction Layer
Extracts cryptocurrency price series and market sentiment news from public APIs,
with built-in resilience and fallback simulation generators.
"""

import sys
import logging
import random
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any
import requests

from pipeline.config import (
    CRYPTO_SYMBOLS,
    BINANCE_API_URL,
    COINGECKO_API_URL,
    ENABLE_LIVE_API,
    TIMEFRAME_DAYS,
)

logger = logging.getLogger("pipeline.extract")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# Baseline reference prices for realistic mock/fallback generation
BASE_PRICES = {
    "BTC": 64250.0,
    "ETH": 3480.0,
    "SOL": 148.5,
    "BNB": 585.0,
    "ADA": 0.48,
    "XRP": 0.59
}

CRYPTO_METADATA = {
    "BTC": {"name": "Bitcoin", "category": "Payment / Store of Value", "network": "Bitcoin Core", "supply": 19700000, "rank": 1},
    "ETH": {"name": "Ethereum", "category": "Smart Contract Platform", "network": "Ethereum Mainnet", "supply": 120000000, "rank": 2},
    "SOL": {"name": "Solana", "category": "High-Speed L1", "network": "Solana Mainnet", "supply": 460000000, "rank": 3},
    "BNB": {"name": "BNB Chain", "category": "Exchange / Smart Chain", "network": "BNB Smart Chain", "supply": 153000000, "rank": 4},
    "ADA": {"name": "Cardano", "category": "Proof-of-Stake L1", "network": "Cardano Settlement Layer", "supply": 35600000000, "rank": 5},
    "XRP": {"name": "Ripple", "category": "Cross-Border Settlement", "network": "XRP Ledger", "supply": 55000000000, "rank": 6}
}

NEWS_HEADLINE_TEMPLATES = [
    ("{symbol} Institutional Inflows Surge 45% Following Spot ETF Volume Growth", "POSITIVE", 0.78),
    ("Major Ecosystem Upgrade Deployed Successfully on {symbol} Mainnet", "POSITIVE", 0.65),
    ("Whale Wallet Transfers $120M {symbol} to Cold Storage, Signaling Accumulation", "POSITIVE", 0.82),
    ("SEC Issues Clarification on Staking Guidelines Impacting {symbol}", "NEUTRAL", 0.05),
    ("{symbol} Network Hashrate / Staking Volume Hits New All-Time High", "POSITIVE", 0.72),
    ("Analysts Debate Key Resistance Level for {symbol} Ahead of Macro CPI Print", "NEUTRAL", 0.12),
    ("Derivatives Open Interest Climbs as {symbol} Consolidates Near Support", "NEUTRAL", -0.08),
    ("Short Liquidations Mount as {symbol} Breaks Out of Descending Triangle", "POSITIVE", 0.85),
    ("Regulatory Headwinds in Asian Markets Lead to Temporary {symbol} Outflow", "NEGATIVE", -0.62),
    ("Profit Taking Detected in {symbol} Derivatives Market Following Rally", "NEGATIVE", -0.45),
    ("DeFi Total Value Locked (TVL) on {symbol} Ecosystem Jumps 18% MoM", "POSITIVE", 0.75),
    ("Flash Volatility Causes Leveraged Positions Flush across {symbol} Pairs", "NEGATIVE", -0.58)
]


def extract_binance_klines(symbol: str, days: int = 30) -> List[Dict[str, Any]]:
    """
    Extract daily candlestick data from Binance public API.
    Returns list of parsed candle dicts.
    """
    market_symbol = f"{symbol}USDT"
    endpoint = f"{BINANCE_API_URL}/klines"
    params = {
        "symbol": market_symbol,
        "interval": "1d",
        "limit": days
    }
    
    try:
        response = requests.get(endpoint, params=params, timeout=5)
        if response.status_code == 200:
            data = response.json()
            candles = []
            for item in data:
                # Binance Klines: [open_time, open, high, low, close, volume, close_time, ...]
                ts = datetime.fromtimestamp(item[0] / 1000, tz=timezone.utc)
                candles.append({
                    "symbol": symbol,
                    "timestamp": ts.isoformat(),
                    "open": float(item[1]),
                    "high": float(item[2]),
                    "low": float(item[3]),
                    "close": float(item[4]),
                    "volume": float(item[5]),
                    "source": "binance_api"
                })
            logger.info(f"Successfully extracted {len(candles)} candles for {symbol} from Binance API.")
            return candles
        else:
            logger.warning(f"Binance API returned status {response.status_code} for {symbol}. Falling back to simulation.")
    except Exception as e:
        logger.warning(f"Error fetching from Binance API for {symbol}: {e}. Falling back to simulation.")
    
    return generate_synthetic_candles(symbol, days=days)


def generate_synthetic_candles(symbol: str, days: int = 30) -> List[Dict[str, Any]]:
    """
    Generates deterministic, realistic historical price walk with volatility and trends.
    """
    base_price = BASE_PRICES.get(symbol, 100.0)
    current_price = base_price * (1.0 + (hash(symbol) % 15 - 7) / 100.0)
    
    candles = []
    end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=days)
    
    random.seed(hash(symbol) + 42)
    
    # Generate prices forward in time
    price_series = []
    for _ in range(days):
        pct_change = random.gauss(0.0015, 0.032) # mean return 0.15%, volatility 3.2%
        current_price = max(current_price * (1.0 + pct_change), 0.01)
        price_series.append(current_price)
        
    for i, p in enumerate(price_series):
        candle_date = start_date + timedelta(days=i)
        intra_volatility = random.uniform(0.01, 0.04)
        open_price = p * (1.0 - random.uniform(-0.01, 0.01))
        close_price = p
        high_price = max(open_price, close_price) * (1.0 + intra_volatility)
        low_price = min(open_price, close_price) * (1.0 - intra_volatility)
        volume = (p * random.uniform(5000, 25000)) if symbol in ["BTC", "ETH"] else (p * random.uniform(200000, 1500000))
        
        candles.append({
            "symbol": symbol,
            "timestamp": candle_date.isoformat(),
            "open": round(open_price, 4),
            "high": round(high_price, 4),
            "low": round(low_price, 4),
            "close": round(close_price, 4),
            "volume": round(volume, 2),
            "source": "synthetic_resilient_engine"
        })
        
    return candles


def extract_crypto_news(symbol: str, count: int = 4) -> List[Dict[str, Any]]:
    """
    Generates realistic news stream items for sentiment analysis.
    """
    news_items = []
    now = datetime.now(timezone.utc)
    sources = ["CoinDesk", "Cointelegraph", "CryptoSlate", "Bloomberg Crypto", "Decrypt", "The Block"]
    
    selected_templates = random.sample(NEWS_HEADLINE_TEMPLATES, min(count, len(NEWS_HEADLINE_TEMPLATES)))
    for idx, (template, expected_label, base_score) in enumerate(selected_templates):
        headline = template.format(symbol=symbol)
        pub_time = now - timedelta(hours=random.randint(1, 48), minutes=random.randint(0, 59))
        
        # Add slight jitter to base score
        score = max(-1.0, min(1.0, base_score + random.uniform(-0.08, 0.08)))
        
        news_items.append({
            "symbol": symbol,
            "headline": headline,
            "source": random.choice(sources),
            "published_at": pub_time.isoformat(),
            "raw_sentiment_hint": expected_label,
            "raw_sentiment_score": round(score, 3)
        })
        
    return news_items


def extract_all_crypto_data(symbols: List[str] = None, days: int = TIMEFRAME_DAYS) -> Dict[str, Any]:
    """
    Main extraction orchestrator: Extracts price history and news for all tracked symbols.
    """
    if symbols is None:
        symbols = CRYPTO_SYMBOLS
        
    logger.info(f"Starting extraction for symbols: {symbols} (History: {days} days)")
    
    extracted_prices: List[Dict[str, Any]] = []
    extracted_news: List[Dict[str, Any]] = []
    
    for sym in symbols:
        if ENABLE_LIVE_API:
            candles = extract_binance_klines(sym, days=days)
        else:
            candles = generate_synthetic_candles(sym, days=days)
            
        extracted_prices.extend(candles)
        
        news = extract_crypto_news(sym, count=3)
        extracted_news.extend(news)
        
    batch_metadata = {
        "batch_id": f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}",
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "total_candles": len(extracted_prices),
        "total_news": len(extracted_news),
        "symbols_tracked": symbols
    }
    
    logger.info(f"Extraction completed. Total prices: {len(extracted_prices)}, Total news: {len(extracted_news)}")
    
    return {
        "metadata": batch_metadata,
        "prices": extracted_prices,
        "news": extracted_news,
        "cryptos_meta": [
            {"symbol": k, **v} for k, v in CRYPTO_METADATA.items() if k in symbols
        ]
    }


if __name__ == "__main__":
    result = extract_all_crypto_data(["BTC", "ETH"], days=7)
    print(f"Sample Batch: {result['metadata']}")
    print(f"Extracted Sample Price: {result['prices'][0]}")
    print(f"Extracted Sample News: {result['news'][0]}")
