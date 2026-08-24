-- =============================================================================
-- SEED DATA FOR DIMENSIONS
-- =============================================================================

-- Seed dim_cryptos
INSERT INTO dim_cryptos (symbol, name, category, network, circulating_supply, market_cap_rank)
VALUES 
    ('BTC', 'Bitcoin', 'Payment / Store of Value', 'Bitcoin Core', 19700000, 1),
    ('ETH', 'Ethereum', 'Smart Contract Platform', 'Ethereum Mainnet', 120000000, 2),
    ('SOL', 'Solana', 'High-Speed L1', 'Solana Mainnet', 460000000, 3),
    ('BNB', 'BNB Chain', 'Exchange / Smart Chain', 'BNB Smart Chain', 153000000, 4),
    ('ADA', 'Cardano', 'Proof-of-Stake L1', 'Cardano Settlement Layer', 35600000000, 5),
    ('XRP', 'Ripple', 'Cross-Border Settlement', 'XRP Ledger', 55000000000, 6)
ON CONFLICT (symbol) DO NOTHING;

-- Seed dim_signal_types
INSERT INTO dim_signal_types (signal_name, risk_level, strategy_name, description)
VALUES 
    ('STRONG_BUY', 'HIGH', 'Confluence Trend Surge', 'Bullish Golden Cross + Oversold RSI + Positive News Sentiment'),
    ('BUY', 'MEDIUM', 'Moving Average Breakout', 'SMA-7 crossed above SMA-25 with healthy volume'),
    ('HOLD', 'LOW', 'Neutral Consolidation', 'Price consolidating in range between moving averages'),
    ('SELL', 'MEDIUM', 'Moving Average Breakdown', 'SMA-7 crossed below SMA-25 with weakening momentum'),
    ('STRONG_SELL', 'HIGH', 'Distribution Warning', 'Bearish Death Cross + Overbought RSI + Negative Sentiment')
ON CONFLICT (signal_name) DO NOTHING;

-- Seed initial date dimensions (Past 90 days to next 30 days)
DO $$
DECLARE
    curr_date DATE := CURRENT_DATE - INTERVAL '90 days';
    end_date DATE := CURRENT_DATE + INTERVAL '30 days';
BEGIN
    WHILE curr_date <= end_date LOOP
        INSERT INTO dim_dates (
            date_key,
            full_date,
            day_of_week,
            day_name,
            month,
            month_name,
            quarter,
            year,
            is_weekend
        )
        VALUES (
            TO_CHAR(curr_date, 'YYYYMMDD')::INT,
            curr_date,
            EXTRACT(ISODOW FROM curr_date)::INT,
            TO_CHAR(curr_date, 'Day'),
            EXTRACT(MONTH FROM curr_date)::INT,
            TO_CHAR(curr_date, 'Month'),
            EXTRACT(QUARTER FROM curr_date)::INT,
            EXTRACT(YEAR FROM curr_date)::INT,
            CASE WHEN EXTRACT(ISODOW FROM curr_date) IN (6, 7) THEN TRUE ELSE FALSE END
        )
        ON CONFLICT (date_key) DO NOTHING;

        curr_date := curr_date + INTERVAL '1 day';
    END LOOP;
END $$;
