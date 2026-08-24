-- =============================================================================
-- CRYPTO INTELLIGENCE DATA WAREHOUSE - STAR SCHEMA DEFINITION
-- =============================================================================

-- 1. DIMENSION TABLES
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dim_cryptos (
    crypto_key SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL UNIQUE,
    name VARCHAR(50) NOT NULL,
    category VARCHAR(30) DEFAULT 'Layer-1',
    network VARCHAR(50) DEFAULT 'Mainnet',
    circulating_supply NUMERIC(24, 4),
    market_cap_rank INT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_signal_types (
    signal_type_key SERIAL PRIMARY KEY,
    signal_name VARCHAR(20) NOT NULL UNIQUE,
    risk_level VARCHAR(20) NOT NULL DEFAULT 'MEDIUM',
    strategy_name VARCHAR(50) NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_dates (
    date_key INT PRIMARY KEY, -- Format: YYYYMMDD
    full_date DATE NOT NULL UNIQUE,
    day_of_week INT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(15) NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- 2. FACT TABLES
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fact_price_history (
    fact_price_id BIGSERIAL PRIMARY KEY,
    crypto_key INT NOT NULL REFERENCES dim_cryptos(crypto_key) ON DELETE CASCADE,
    date_key INT NOT NULL REFERENCES dim_dates(date_key),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    open_price NUMERIC(18, 4) NOT NULL,
    high_price NUMERIC(18, 4) NOT NULL,
    low_price NUMERIC(18, 4) NOT NULL,
    close_price NUMERIC(18, 4) NOT NULL,
    volume NUMERIC(24, 4) NOT NULL,
    sma_7 NUMERIC(18, 4),
    sma_25 NUMERIC(18, 4),
    ema_12 NUMERIC(18, 4),
    ema_26 NUMERIC(18, 4),
    rsi_14 NUMERIC(8, 2),
    macd NUMERIC(18, 4),
    macd_signal NUMERIC(18, 4),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_crypto_timestamp UNIQUE (crypto_key, timestamp)
);

CREATE TABLE IF NOT EXISTS fact_signals (
    signal_id BIGSERIAL PRIMARY KEY,
    crypto_key INT NOT NULL REFERENCES dim_cryptos(crypto_key) ON DELETE CASCADE,
    signal_type_key INT NOT NULL REFERENCES dim_signal_types(signal_type_key),
    date_key INT NOT NULL REFERENCES dim_dates(date_key),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    trigger_price NUMERIC(18, 4) NOT NULL,
    confidence_score NUMERIC(5, 2) NOT NULL, -- Range: 0.00 to 100.00
    technical_reason TEXT NOT NULL,
    sentiment_weight NUMERIC(4, 2) DEFAULT 0.0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_news_sentiment (
    sentiment_id BIGSERIAL PRIMARY KEY,
    crypto_key INT NOT NULL REFERENCES dim_cryptos(crypto_key) ON DELETE CASCADE,
    date_key INT NOT NULL REFERENCES dim_dates(date_key),
    headline TEXT NOT NULL,
    source VARCHAR(50) DEFAULT 'CryptoNewsFeed',
    sentiment_score NUMERIC(4, 3) NOT NULL, -- -1.000 to +1.000
    sentiment_label VARCHAR(20) NOT NULL,   -- POSITIVE, NEUTRAL, NEGATIVE
    published_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. INDEXES FOR HIGH PERFORMANCE ANALYTICS
-- -----------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_fact_price_crypto_ts ON fact_price_history (crypto_key, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_fact_price_date ON fact_price_history (date_key);
CREATE INDEX IF NOT EXISTS idx_fact_signals_crypto ON fact_signals (crypto_key, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_fact_signals_type ON fact_signals (signal_type_key);
CREATE INDEX IF NOT EXISTS idx_fact_sentiment_crypto ON fact_news_sentiment (crypto_key, published_at DESC);

-- 4. ANALYTICAL VIEWS
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW v_latest_trading_signals AS
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

CREATE OR REPLACE VIEW v_crypto_market_summary AS
WITH ranked_prices AS (
    SELECT 
        f.crypto_key,
        f.close_price,
        f.volume,
        f.sma_7,
        f.sma_25,
        f.rsi_14,
        f.timestamp,
        ROW_NUMBER() OVER(PARTITION BY f.crypto_key ORDER BY f.timestamp DESC) as rn
    FROM fact_price_history f
)
SELECT 
    c.symbol,
    c.name,
    c.market_cap_rank,
    rp.close_price AS current_price,
    rp.volume AS volume_24h,
    rp.sma_7,
    rp.sma_25,
    rp.rsi_14,
    rp.timestamp AS last_updated
FROM dim_cryptos c
LEFT JOIN ranked_prices rp ON c.crypto_key = rp.crypto_key AND rp.rn = 1
ORDER BY c.market_cap_rank ASC;
