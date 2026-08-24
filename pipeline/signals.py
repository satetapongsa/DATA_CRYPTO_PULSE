"""
Trading Signal Intelligence Engine
Evaluates multi-factor quantitative criteria (Moving Average Crossovers, RSI Extremes,
MACD Divergence, and NLP Sentiment Confluence) to generate algorithmic trading signals.
"""

import logging
from datetime import datetime, timezone
from typing import List, Dict, Any
import pandas as pd

logger = logging.getLogger("pipeline.signals")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


def evaluate_symbol_signals(
    df_prices: pd.DataFrame,
    df_news: pd.DataFrame,
    symbol: str
) -> List[Dict[str, Any]]:
    """
    Evaluates trading signals for the latest timestamps of a specific symbol.
    """
    sym_prices = df_prices[df_prices["symbol"] == symbol].sort_values("datetime_utc").reset_index(drop=True)
    if len(sym_prices) < 2:
        return []

    # Get recent sentiment for this symbol
    sym_news = df_news[df_news["symbol"] == symbol] if not df_news.empty else pd.DataFrame()
    avg_sentiment = sym_news["sentiment_score"].mean() if not sym_news.empty else 0.0
    if pd.isna(avg_sentiment):
        avg_sentiment = 0.0

    signals = []
    
    # Analyze the most recent 3 candle timestamps for active signals
    lookback = min(3, len(sym_prices))
    for i in range(len(sym_prices) - lookback, len(sym_prices)):
        curr = sym_prices.iloc[i]
        prev = sym_prices.iloc[i - 1] if i > 0 else curr

        close_p = float(curr["close"])
        sma7 = float(curr["sma_7"])
        sma25 = float(curr["sma_25"])
        prev_sma7 = float(prev["sma_7"])
        prev_sma25 = float(prev["sma_25"])
        rsi = float(curr["rsi_14"])
        macd = float(curr["macd"])
        macd_sig = float(curr["macd_signal"])

        # Determine Technical Indicators Condition
        is_golden_cross = (prev_sma7 <= prev_sma25) and (sma7 > sma25)
        is_death_cross = (prev_sma7 >= prev_sma25) and (sma7 < sma25)
        is_bullish_ma = sma7 > sma25
        is_bearish_ma = sma7 < sma25
        is_oversold = rsi <= 35.0
        is_overbought = rsi >= 68.0
        is_macd_bullish = macd > macd_sig

        # Multi-factor score calculation (-100 to +100)
        quant_score = 0.0
        reasons = []

        if is_golden_cross:
            quant_score += 45.0
            reasons.append("Golden Cross (SMA-7 broke above SMA-25)")
        elif is_bullish_ma:
            quant_score += 25.0
            reasons.append(f"Bullish Trend (SMA-7: ${sma7:,.2f} > SMA-25: ${sma25:,.2f})")

        if is_death_cross:
            quant_score -= 45.0
            reasons.append("Death Cross (SMA-7 broke below SMA-25)")
        elif is_bearish_ma:
            quant_score -= 25.0
            reasons.append(f"Bearish Trend (SMA-7: ${sma7:,.2f} < SMA-25: ${sma25:,.2f})")

        if is_oversold:
            quant_score += 30.0
            reasons.append(f"RSI Oversold ({rsi:.1f}) indicating accumulation zone")
        elif is_overbought:
            quant_score -= 30.0
            reasons.append(f"RSI Overbought ({rsi:.1f}) indicating distribution risk")

        if is_macd_bullish:
            quant_score += 15.0
            reasons.append("MACD Momentum positive")
        else:
            quant_score -= 15.0
            reasons.append("MACD Momentum negative")

        # Factor in News Sentiment Confluence
        sentiment_impact = avg_sentiment * 25.0
        quant_score += sentiment_impact
        if avg_sentiment > 0.2:
            reasons.append(f"Bullish News Sentiment (+{avg_sentiment:.2f})")
        elif avg_sentiment < -0.2:
            reasons.append(f"Bearish News Sentiment ({avg_sentiment:.2f})")

        # Classify final signal & confidence
        if quant_score >= 50.0:
            signal_name = "STRONG_BUY"
            confidence = min(98.0, 70.0 + (quant_score - 50.0) * 0.5)
        elif quant_score >= 15.0:
            signal_name = "BUY"
            confidence = min(85.0, 55.0 + (quant_score - 15.0) * 0.7)
        elif quant_score <= -50.0:
            signal_name = "STRONG_SELL"
            confidence = min(98.0, 70.0 + (abs(quant_score) - 50.0) * 0.5)
        elif quant_score <= -15.0:
            signal_name = "SELL"
            confidence = min(85.0, 55.0 + (abs(quant_score) - 15.0) * 0.7)
        else:
            signal_name = "HOLD"
            confidence = max(50.0, 60.0 - abs(quant_score))
            reasons.append("Market in consolidation; no dominant directional edge")

        signal_doc = {
            "symbol": symbol,
            "timestamp": curr["datetime_utc"].isoformat(),
            "date_key": int(curr["date_key"]),
            "signal_name": signal_name,
            "trigger_price": close_p,
            "confidence_score": round(confidence, 2),
            "technical_reason": " | ".join(reasons),
            "sentiment_weight": round(avg_sentiment, 2)
        }
        signals.append(signal_doc)

    return signals


def generate_trading_signals(
    prices_df: pd.DataFrame,
    news_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Generates trading signals across all symbols in the dataset.
    """
    if prices_df.empty:
        return pd.DataFrame()

    all_signals = []
    for symbol in prices_df["symbol"].unique():
        sym_signals = evaluate_symbol_signals(prices_df, news_df, symbol)
        all_signals.extend(sym_signals)

    df_signals = pd.DataFrame(all_signals)
    logger.info(f"Generated {len(df_signals)} trading signals across {len(prices_df['symbol'].unique())} symbols.")
    return df_signals


if __name__ == "__main__":
    from pipeline.extract import extract_all_crypto_data
    from pipeline.transform import run_etl_transformation
    raw = extract_all_crypto_data(["BTC", "ETH", "SOL"], days=15)
    transformed = run_etl_transformation(raw)
    signals_df = generate_trading_signals(transformed["prices_df"], transformed["news_df"])
    print(signals_df[["symbol", "signal_name", "confidence_score", "trigger_price", "technical_reason"]].to_string())
