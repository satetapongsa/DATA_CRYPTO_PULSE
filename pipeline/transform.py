"""
Data Transformation Layer
Performs data cleaning, type validation, date dimension key derivation,
feature engineering for technical indicators (SMA, EMA, RSI, MACD),
and NLP sentiment extraction on unstructured news data.
"""

import re
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Tuple
import pandas as pd
import numpy as np

logger = logging.getLogger("pipeline.transform")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# Sentiment Lexicon weights for crypto domain
BULLISH_KEYWORDS = {
    "surge": 0.8, "bullish": 0.9, "breakout": 0.85, "inflows": 0.7, "etf": 0.6,
    "accumulation": 0.75, "all-time high": 0.95, "ath": 0.95, "rally": 0.8,
    "upgrade": 0.65, "gain": 0.6, "jump": 0.7, "adoption": 0.65, "record": 0.7
}

BEARISH_KEYWORDS = {
    "crash": -0.9, "bearish": -0.85, "breakdown": -0.8, "outflows": -0.7,
    "liquidation": -0.8, "dump": -0.85, "ban": -0.9, "lawsuit": -0.75,
    "fraud": -0.95, "hack": -0.95, "plunge": -0.85, "drop": -0.6, "warning": -0.55
}


def clean_and_derive_date_fields(df: pd.DataFrame, ts_column: str = "timestamp") -> pd.DataFrame:
    """
    Cleans timestamp string, converts to UTC datetime, and generates Star Schema date_key (YYYYMMDD).
    """
    df = df.copy()
    df["datetime_utc"] = pd.to_datetime(df[ts_column], utc=True)
    df["date_key"] = df["datetime_utc"].dt.strftime("%Y%m%d").astype(int)
    df["full_date"] = df["datetime_utc"].dt.date
    df = df.sort_values(by=["symbol", "datetime_utc"]).reset_index(drop=True)
    return df


def calculate_technical_indicators(df_symbol: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates SMA-7, SMA-25, EMA-12, EMA-26, RSI-14, MACD, and MACD Signal Line on price series.
    """
    df = df_symbol.copy().sort_values("datetime_utc").reset_index(drop=True)
    close = df["close"]

    # 1. Simple Moving Averages
    df["sma_7"] = close.rolling(window=7, min_periods=1).mean().round(4)
    df["sma_25"] = close.rolling(window=25, min_periods=1).mean().round(4)

    # 2. Exponential Moving Averages
    df["ema_12"] = close.ewm(span=12, adjust=False).mean().round(4)
    df["ema_26"] = close.ewm(span=26, adjust=False).mean().round(4)

    # 3. MACD Line & Signal Line
    df["macd"] = (df["ema_12"] - df["ema_26"]).round(4)
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean().round(4)

    # 4. RSI (14 periods)
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -1 * delta.clip(upper=0)
    
    avg_gain = gain.rolling(window=14, min_periods=1).mean()
    avg_loss = loss.rolling(window=14, min_periods=1).mean()
    
    # Calculate RS and RSI
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    # Impute initial zero loss cases
    rsi = rsi.fillna(50.0).round(2)
    df["rsi_14"] = rsi

    return df


def transform_price_history(raw_prices: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Transforms raw price records into clean analytics-ready DataFrame with technical features.
    """
    if not raw_prices:
        return pd.DataFrame()

    df = pd.DataFrame(raw_prices)
    
    # Remove duplicates
    df = df.drop_duplicates(subset=["symbol", "timestamp"])
    
    # Enforce numeric types
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
            
    # Clean and derive date_key
    df = clean_and_derive_date_fields(df, ts_column="timestamp")

    # Compute technical indicators per symbol partition
    transformed_dfs = []
    for symbol, group in df.groupby("symbol"):
        transformed_group = calculate_technical_indicators(group)
        transformed_dfs.append(transformed_group)

    final_df = pd.concat(transformed_dfs, ignore_index=True)
    logger.info(f"Transformed {len(final_df)} price records across {final_df['symbol'].nunique()} symbols.")
    return final_df


def analyze_headline_sentiment(headline: str) -> Tuple[float, str]:
    """
    NLP sentiment scoring using crypto-specific regex tokenization and weighted lexicon.
    Returns (score: -1.0 to 1.0, label: POSITIVE/NEUTRAL/NEGATIVE).
    """
    cleaned = re.sub(r"[^\w\s]", " ", headline.lower())
    tokens = cleaned.split()

    score = 0.0
    matches = 0

    for token in tokens:
        if token in BULLISH_KEYWORDS:
            score += BULLISH_KEYWORDS[token]
            matches += 1
        elif token in BEARISH_KEYWORDS:
            score += BEARISH_KEYWORDS[token]
            matches += 1

    if matches > 0:
        normalized_score = max(-1.0, min(1.0, score / matches))
    else:
        normalized_score = 0.0

    if normalized_score >= 0.25:
        label = "POSITIVE"
    elif normalized_score <= -0.25:
        label = "NEGATIVE"
    else:
        label = "NEUTRAL"

    return round(normalized_score, 3), label


def transform_news_sentiment(raw_news: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Transforms raw news items with sentiment NLP scores and Star Schema date keys.
    """
    if not raw_news:
        return pd.DataFrame()

    df = pd.DataFrame(raw_news)
    df = df.drop_duplicates(subset=["symbol", "headline"])
    df = clean_and_derive_date_fields(df, ts_column="published_at")

    scores = []
    labels = []
    for _, row in df.iterrows():
        # Combine lexicon score with upstream hint if available
        lex_score, lex_label = analyze_headline_sentiment(row["headline"])
        if "raw_sentiment_score" in row and not pd.isna(row["raw_sentiment_score"]):
            combined_score = round(0.5 * lex_score + 0.5 * float(row["raw_sentiment_score"]), 3)
            if combined_score >= 0.2:
                combined_label = "POSITIVE"
            elif combined_score <= -0.2:
                combined_label = "NEGATIVE"
            else:
                combined_label = "NEUTRAL"
        else:
            combined_score, combined_label = lex_score, lex_label

        scores.append(combined_score)
        labels.append(combined_label)

    df["sentiment_score"] = scores
    df["sentiment_label"] = labels
    logger.info(f"Transformed and scored {len(df)} news items.")
    return df


def run_etl_transformation(raw_batch: Dict[str, Any]) -> Dict[str, Any]:
    """
    Executes the full transformation pipeline.
    """
    transformed_prices_df = transform_price_history(raw_batch.get("prices", []))
    transformed_news_df = transform_news_sentiment(raw_batch.get("news", []))

    return {
        "batch_id": raw_batch["metadata"]["batch_id"],
        "prices_df": transformed_prices_df,
        "news_df": transformed_news_df,
        "cryptos_meta": raw_batch.get("cryptos_meta", [])
    }


if __name__ == "__main__":
    from pipeline.extract import extract_all_crypto_data
    batch = extract_all_crypto_data(["BTC", "ETH"], days=10)
    transformed = run_etl_transformation(batch)
    print("Price Columns:", transformed["prices_df"].columns.tolist())
    print(transformed["prices_df"][["symbol", "close", "sma_7", "rsi_14", "macd"]].tail())
    print(transformed["news_df"][["symbol", "headline", "sentiment_score", "sentiment_label"]].head())
