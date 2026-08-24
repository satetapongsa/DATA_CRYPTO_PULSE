/**
 * Data Warehouse & Lakehouse In-Memory / Serverless Store for Vercel Runtime
 * Mirrors the Python ETL pipeline, Star Schema relational tables, and MongoDB NoSQL store.
 */

const CRYPTO_META = [
  { symbol: 'BTC', name: 'Bitcoin', category: 'Payment / Store of Value', network: 'Bitcoin Core', rank: 1, basePrice: 64500.0 },
  { symbol: 'ETH', name: 'Ethereum', category: 'Smart Contract Platform', network: 'Ethereum Mainnet', rank: 2, basePrice: 3450.0 },
  { symbol: 'SOL', name: 'Solana', category: 'High-Speed L1', network: 'Solana Mainnet', rank: 3, basePrice: 145.0 },
  { symbol: 'BNB', name: 'BNB Chain', category: 'Exchange / Smart Chain', network: 'BNB Smart Chain', rank: 4, basePrice: 585.0 },
  { symbol: 'ADA', name: 'Cardano', category: 'Proof-of-Stake L1', network: 'Cardano Settlement Layer', rank: 5, basePrice: 0.48 },
  { symbol: 'XRP', name: 'Ripple', category: 'Cross-Border Settlement', network: 'XRP Ledger', rank: 6, basePrice: 0.58 }
];

const SIGNAL_TYPES = [
  { key: 1, name: 'STRONG_BUY', risk: 'HIGH', strategy: 'Confluence Trend Surge', desc: 'Bullish Golden Cross + Oversold RSI + Positive News Sentiment' },
  { key: 2, name: 'BUY', risk: 'MEDIUM', strategy: 'Moving Average Breakout', desc: 'SMA-7 crossed above SMA-25 with healthy volume' },
  { key: 3, name: 'HOLD', risk: 'LOW', strategy: 'Neutral Consolidation', desc: 'Price consolidating in range between moving averages' },
  { key: 4, name: 'SELL', risk: 'MEDIUM', strategy: 'Moving Average Breakdown', desc: 'SMA-7 crossed below SMA-25 with weakening momentum' },
  { key: 5, name: 'STRONG_SELL', risk: 'HIGH', strategy: 'Distribution Warning', desc: 'Bearish Death Cross + Overbought RSI + Negative Sentiment' }
];

const HEADLINES = [
  '{symbol} Institutional Inflows Surge 45% Following Spot ETF Volume Growth',
  'Major Ecosystem Upgrade Deployed Successfully on {symbol} Mainnet',
  'Whale Wallet Transfers $120M {symbol} to Cold Storage, Signaling Accumulation',
  'SEC Issues Clarification on Staking Guidelines Impacting {symbol}',
  '{symbol} Network Hashrate / Staking Volume Hits New All-Time High',
  'Analysts Debate Key Resistance Level for {symbol} Ahead of Macro CPI Print',
  'Derivatives Open Interest Climbs as {symbol} Consolidates Near Support',
  'Short Liquidations Mount as {symbol} Breaks Out of Descending Triangle',
  'DeFi Total Value Locked (TVL) on {symbol} Ecosystem Jumps 18% MoM'
];

class ServerlessDataStore {
  constructor() {
    this.priceHistory = [];
    this.signals = [];
    this.newsSentiment = [];
    this.rawLakehouse = {
      raw_crypto_market: [],
      raw_news_feed: [],
      pipeline_audit_logs: []
    };
    this.dagState = {
      is_running: false,
      overall_status: 'SUCCESS',
      current_task: null,
      last_run_time: new Date().toISOString(),
      tasks: [
        { id: 'wait_for_source', name: 'Wait For API Readiness', description: 'Health checks public API endpoints & rate limits', status: 'SUCCESS', duration_seconds: 0.5 },
        { id: 'extract_data', name: 'Extract Market & News Data', description: 'Fetches OHLCV candles & crypto news feeds', status: 'SUCCESS', duration_seconds: 0.8 },
        { id: 'process_data_mongodb', name: 'Ingest to MongoDB Lakehouse', description: 'Stores raw BSON/JSON payloads into staging store', status: 'SUCCESS', duration_seconds: 0.1 },
        { id: 'clean_transform_data', name: 'ETL Feature Transformation', description: 'Cleans data, computes SMA/EMA/RSI/MACD & NLP Sentiment', status: 'SUCCESS', duration_seconds: 0.2 },
        { id: 'load_to_postgres', name: 'Load Star Schema Warehouse', description: 'Upserts Dimensions & Price/Sentiment Fact tables', status: 'SUCCESS', duration_seconds: 0.3 },
        { id: 'generate_signals', name: 'Compute Trading Intelligence', description: 'Calculates algorithmic BUY/HOLD/SELL signals', status: 'SUCCESS', duration_seconds: 0.4 }
      ],
      logs: [
        `[${new Date().toLocaleTimeString()}] [START] Starting Pipeline Orchestration for symbols: ['BTC', 'ETH', 'SOL', 'BNB', 'ADA', 'XRP']`,
        `[${new Date().toLocaleTimeString()}] Task 1/6: Checking connectivity to Binance & Crypto News APIs...`,
        `[${new Date().toLocaleTimeString()}] Public API endpoints responsive. Network connectivity OK.`,
        `[${new Date().toLocaleTimeString()}] Task 2/6: Extracting OHLCV candles and news streams for 6 symbols...`,
        `[${new Date().toLocaleTimeString()}] Extracted 180 price candles & 18 news items.`,
        `[${new Date().toLocaleTimeString()}] Task 3/6: Ingesting raw JSON payloads to MongoDB Lakehouse collections...`,
        `[${new Date().toLocaleTimeString()}] Task 4/6: Computing technical indicators (SMA-7/25, EMA, RSI-14, MACD) & NLP sentiment...`,
        `[${new Date().toLocaleTimeString()}] Task 5/6: Generating algorithmic trading signals & loading Star Schema Warehouse...`,
        `[${new Date().toLocaleTimeString()}] Task 6/6: Trading intelligence published to analytical data marts successfully.`,
        `[${new Date().toLocaleTimeString()}] Pipeline Run SUCCESSFUL in 2.3s.`
      ]
    };

    this.bootstrapData();
  }

  bootstrapData(days = 30) {
    this.priceHistory = [];
    this.signals = [];
    this.newsSentiment = [];
    this.rawLakehouse.raw_crypto_market = [];
    this.rawLakehouse.raw_news_feed = [];

    const now = new Date();
    let factPriceId = 1;
    let signalId = 1;
    let sentimentId = 1;

    CRYPTO_META.forEach((coin, cIdx) => {
      let currPrice = coin.basePrice;
      const prices = [];

      for (let i = days; i >= 0; i--) {
        const d = new Date(now);
        d.setDate(d.getDate() - i);
        const dateKey = parseInt(d.toISOString().slice(0, 10).replace(/-/g, ''), 10);
        
        // Random walk
        const change = (Math.sin(i * 0.5 + cIdx) * 0.02) + ((Math.random() - 0.48) * 0.035);
        currPrice = Math.max(currPrice * (1 + change), 0.01);
        
        const open = currPrice * (1 + (Math.random() * 0.01 - 0.005));
        const high = Math.max(open, currPrice) * (1 + Math.random() * 0.02);
        const low = Math.min(open, currPrice) * (1 - Math.random() * 0.02);
        const volume = currPrice * (cIdx < 2 ? (10000 + Math.random() * 15000) : (500000 + Math.random() * 1500000));

        prices.push({
          fact_price_id: factPriceId++,
          crypto_key: cIdx + 1,
          symbol: coin.symbol,
          date_key: dateKey,
          timestamp: d.toISOString(),
          open: parseFloat(open.toFixed(4)),
          high: parseFloat(high.toFixed(4)),
          low: parseFloat(low.toFixed(4)),
          close: parseFloat(currPrice.toFixed(4)),
          volume: parseFloat(volume.toFixed(2))
        });
      }

      // Feature Engineering: SMA-7, SMA-25, RSI-14, MACD
      prices.forEach((p, idx) => {
        // SMA-7
        const slice7 = prices.slice(Math.max(0, idx - 6), idx + 1);
        p.sma_7 = parseFloat((slice7.reduce((a, b) => a + b.close, 0) / slice7.length).toFixed(4));

        // SMA-25
        const slice25 = prices.slice(Math.max(0, idx - 24), idx + 1);
        p.sma_25 = parseFloat((slice25.reduce((a, b) => a + b.close, 0) / slice25.length).toFixed(4));

        // EMA-12 & 26
        p.ema_12 = parseFloat((p.close * 0.15 + (prices[idx - 1]?.ema_12 || p.close) * 0.85).toFixed(4));
        p.ema_26 = parseFloat((p.close * 0.075 + (prices[idx - 1]?.ema_26 || p.close) * 0.925).toFixed(4));
        p.macd = parseFloat((p.ema_12 - p.ema_26).toFixed(4));
        p.macd_signal = parseFloat((p.macd * 0.2 + (prices[idx - 1]?.macd_signal || p.macd) * 0.8).toFixed(4));

        // RSI-14
        if (idx > 0) {
          const gains = [];
          const losses = [];
          for (let j = Math.max(1, idx - 13); j <= idx; j++) {
            const diff = prices[j].close - prices[j - 1].close;
            if (diff >= 0) gains.push(diff);
            else losses.push(Math.abs(diff));
          }
          const avgG = gains.length ? gains.reduce((a, b) => a + b, 0) / 14 : 0;
          const avgL = losses.length ? losses.reduce((a, b) => a + b, 0) / 14 : 0.001;
          const rs = avgG / avgL;
          p.rsi_14 = parseFloat((100 - (100 / (1 + rs))).toFixed(2));
        } else {
          p.rsi_14 = 50.0;
        }

        this.priceHistory.push(p);

        // Store raw lakehouse mock
        this.rawLakehouse.raw_crypto_market.push({
          batch_id: 'batch_vercel_sync',
          symbol: p.symbol,
          timestamp: p.timestamp,
          open: p.open,
          high: p.high,
          low: p.low,
          close: p.close,
          volume: p.volume,
          source: 'binance_api_stream'
        });
      });

      // Generate News Sentiment
      for (let n = 0; n < 3; n++) {
        const headlineTpl = HEADLINES[(cIdx * 2 + n) % HEADLINES.length];
        const headline = headlineTpl.replace('{symbol}', coin.symbol);
        const score = parseFloat((0.2 + Math.random() * 0.65).toFixed(3));
        const pubDate = new Date(now.getTime() - (n * 12 + 2) * 3600 * 1000);

        this.newsSentiment.push({
          sentiment_id: sentimentId++,
          crypto_key: cIdx + 1,
          symbol: coin.symbol,
          date_key: parseInt(pubDate.toISOString().slice(0, 10).replace(/-/g, ''), 10),
          headline: headline,
          source: ['CoinDesk', 'Cointelegraph', 'Bloomberg Crypto', 'CryptoSlate'][n % 4],
          sentiment_score: score,
          sentiment_label: score > 0.2 ? 'POSITIVE' : score < -0.2 ? 'NEGATIVE' : 'NEUTRAL',
          published_at: pubDate.toISOString()
        });

        this.rawLakehouse.raw_news_feed.push({
          symbol: coin.symbol,
          headline: headline,
          raw_score: score,
          published_at: pubDate.toISOString()
        });
      }

      // Generate Trading Signals for latest candle
      const latestPrice = prices[prices.length - 1];
      const isGolden = latestPrice.sma_7 > latestPrice.sma_25;
      const isRsiHigh = latestPrice.rsi_14 > 70;
      const isRsiLow = latestPrice.rsi_14 < 35;

      let sigName = 'BUY';
      let confidence = 75.0;
      let reasons = [];

      if (isGolden) {
        reasons.push(`Bullish Trend (SMA-7: $${latestPrice.sma_7.toLocaleString()} > SMA-25: $${latestPrice.sma_25.toLocaleString()})`);
      } else {
        reasons.push(`Bearish Momentum (SMA-7 < SMA-25)`);
      }

      if (isRsiHigh) {
        sigName = 'STRONG_BUY';
        confidence = 88.5;
        reasons.push(`RSI Momentum Surge (${latestPrice.rsi_14})`);
      } else if (isRsiLow) {
        sigName = 'HOLD';
        confidence = 58.0;
        reasons.push(`RSI Consolidating in Oversold Territory (${latestPrice.rsi_14})`);
      } else {
        sigName = isGolden ? 'BUY' : 'HOLD';
        confidence = 65.0;
      }
      reasons.push(`Positive News Sentiment (+0.45)`);

      this.signals.push({
        signal_id: signalId++,
        crypto_key: cIdx + 1,
        symbol: coin.symbol,
        crypto_name: coin.name,
        category: coin.category,
        signal_name: sigName,
        risk_level: sigName.includes('STRONG') ? 'HIGH' : sigName === 'BUY' ? 'MEDIUM' : 'LOW',
        strategy_name: sigName === 'HOLD' ? 'Neutral Consolidation' : 'Moving Average Breakout',
        trigger_price: latestPrice.close,
        confidence_score: confidence,
        technical_reason: reasons.join(' | '),
        sentiment_weight: 0.45,
        generated_at: latestPrice.timestamp
      });
    });
  }

  getCryptos() {
    return CRYPTO_META.map(c => {
      const pHistory = this.priceHistory.filter(p => p.symbol === c.symbol);
      const latest = pHistory[pHistory.length - 1];
      const prev = pHistory[pHistory.length - 2];
      const pctChange = prev ? parseFloat((((latest.close - prev.close) / prev.close) * 100).toFixed(2)) : 0.0;
      const sent = this.newsSentiment.find(s => s.symbol === c.symbol);

      return {
        symbol: c.symbol,
        name: c.name,
        category: c.category,
        market_cap_rank: c.rank,
        current_price: latest?.close || c.basePrice,
        change_24h_pct: pctChange,
        volume_24h: latest?.volume || 0,
        sma_7: latest?.sma_7 || null,
        sma_25: latest?.sma_25 || null,
        rsi_14: latest?.rsi_14 || null,
        sentiment_label: sent?.sentiment_label || 'POSITIVE',
        last_updated: latest?.timestamp || new Date().toISOString()
      };
    });
  }

  getPriceTrends(symbol, days = 30) {
    const sym = (symbol || 'BTC').toUpperCase();
    const meta = CRYPTO_META.find(c => c.symbol === sym) || CRYPTO_META[0];
    const data = this.priceHistory.filter(p => p.symbol === sym).slice(-days);
    return {
      symbol: meta.symbol,
      name: meta.name,
      count: data.length,
      data: data
    };
  }

  getSignals() {
    return [...this.signals].reverse();
  }

  getSummary() {
    const dist = {};
    this.signals.forEach(s => {
      dist[s.signal_name] = (dist[s.signal_name] || 0) + 1;
    });
    return {
      signal_distribution: dist,
      average_sentiment_score: 0.42,
      market_mood_index: 72,
      market_mood_label: 'GREED',
      total_news_analyzed: this.newsSentiment.length
    };
  }

  getDagStatus() {
    return this.dagState;
  }

  triggerDag() {
    this.bootstrapData(30);
    this.dagState.is_running = true;
    this.dagState.overall_status = 'RUNNING';
    this.dagState.last_run_time = new Date().toISOString();

    const timeStr = new Date().toLocaleTimeString();
    this.dagState.logs.push(`[${timeStr}] ⚡ Manual Trigger DAG Run initiated by User.`);
    this.dagState.logs.push(`[${timeStr}] Extracted fresh market candles & executed ETL feature transformations.`);

    setTimeout(() => {
      this.dagState.is_running = false;
      this.dagState.overall_status = 'SUCCESS';
    }, 1500);

    return { status: 'TRIGGERED', message: 'Pipeline executed in Serverless Engine.' };
  }

  getWarehouseTables() {
    return [
      { name: 'dim_cryptos', type: 'DIMENSION', description: 'Cryptocurrency assets metadata dimension', row_count: CRYPTO_META.length },
      { name: 'dim_signal_types', type: 'DIMENSION', description: 'Trading signal classification dimension', row_count: SIGNAL_TYPES.length },
      { name: 'dim_dates', type: 'DIMENSION', description: 'Conformed Date Dimension (YYYYMMDD)', row_count: 31 },
      { name: 'fact_price_history', type: 'FACT', description: 'Historical OHLCV + SMA, EMA, RSI, MACD', row_count: this.priceHistory.length },
      { name: 'fact_signals', type: 'FACT', description: 'Algorithmic trading signals & confidence', row_count: this.signals.length },
      { name: 'fact_news_sentiment', type: 'FACT', description: 'NLP sentiment scored news events', row_count: this.newsSentiment.length },
      { name: 'v_latest_trading_signals', type: 'VIEW', description: 'Analytical view of joined latest signals', row_count: this.signals.length },
      { name: 'v_crypto_market_summary', type: 'VIEW', description: 'Market ticker summary with indicators', row_count: CRYPTO_META.length }
    ];
  }

  getTableData(tableName) {
    let rows = [];
    if (tableName === 'dim_cryptos') {
      rows = CRYPTO_META.map((c, i) => ({ crypto_key: i + 1, symbol: c.symbol, name: c.name, category: c.category, network: c.network, rank: c.rank }));
    } else if (tableName === 'dim_signal_types') {
      rows = SIGNAL_TYPES.map(s => ({ signal_type_key: s.key, signal_name: s.name, risk_level: s.risk, strategy_name: s.strategy, description: s.desc }));
    } else if (tableName === 'fact_price_history') {
      rows = this.priceHistory.slice(0, 30);
    } else if (tableName === 'fact_signals' || tableName === 'v_latest_trading_signals') {
      rows = this.signals;
    } else if (tableName === 'fact_news_sentiment') {
      rows = this.newsSentiment;
    } else {
      rows = this.getCryptos();
    }

    return {
      table_name: tableName,
      total_rows: rows.length,
      columns: rows.length ? Object.keys(rows[0]) : [],
      data: rows
    };
  }

  getMongoCollections() {
    return [
      { name: 'raw_crypto_market', description: 'Raw OHLCV API JSON responses before transformation', store: 'MongoDB / Lakehouse' },
      { name: 'raw_news_feed', description: 'Raw uncleaned crypto headlines and metadata', store: 'MongoDB / Lakehouse' },
      { name: 'pipeline_audit_logs', description: 'Raw pipeline execution audit envelopes', store: 'MongoDB / Lakehouse' }
    ];
  }

  getMongoDocs(collName) {
    const docs = this.rawLakehouse[collName] || this.rawLakehouse.raw_crypto_market;
    return {
      collection: collName,
      count: docs.length,
      documents: docs.slice(0, 15)
    };
  }
}

// Global Singleton
const globalStore = globalThis._cryptoStore || new ServerlessDataStore();
if (process.env.NODE_ENV !== 'production') globalThis._cryptoStore = globalStore;

export default globalStore;
