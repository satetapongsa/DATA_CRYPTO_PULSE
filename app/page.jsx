'use client';

import React, { useState, useEffect, useRef } from 'react';
import {
  Activity, Play, TrendingUp, Gauge, Database, FileJson, Zap,
  BarChart2, Radio, GitBranch, Table, FileCode, Copy, ArrowRight,
  Wifi, DownloadCloud, Cpu, Sparkles, Terminal, BookOpen, HeartPulse, Loader2
} from 'lucide-react';
import Chart from 'chart.js/auto';

export default function Dashboard() {
  const [cryptos, setCryptos] = useState([]);
  const [selectedSymbol, setSelectedSymbol] = useState('BTC');
  const [marketSummary, setMarketSummary] = useState(null);
  const [signals, setSignals] = useState([]);
  const [dagStatus, setDagStatus] = useState(null);
  const [isTriggering, setIsTriggering] = useState(false);
  
  // Chart Controls
  const [showSma7, setShowSma7] = useState(true);
  const [showSma25, setShowSma25] = useState(true);
  const [showRsi, setShowRsi] = useState(true);

  // Lakehouse Explorer State
  const [lakehouseMode, setLakehouseMode] = useState('sql');
  const [tablesList, setTablesList] = useState([]);
  const [mongoColls, setMongoColls] = useState([]);
  const [selectedEntity, setSelectedEntity] = useState('dim_cryptos');
  const [tableData, setTableData] = useState(null);
  const [mongoDocs, setMongoDocs] = useState([]);

  // Chart Canvas Refs
  const priceChartRef = useRef(null);
  const rsiChartRef = useRef(null);
  const donutChartRef = useRef(null);
  const priceChartInstance = useRef(null);
  const rsiChartInstance = useRef(null);
  const donutChartInstance = useRef(null);
  const consoleLogRef = useRef(null);

  // 1. Initial Data Fetch
  useEffect(() => {
    fetchCryptos();
    fetchSummary();
    fetchSignals();
    fetchDagStatus();
    loadLakehouseMetadata();

    const interval = setInterval(() => {
      fetchCryptos();
      fetchSummary();
      fetchSignals();
      fetchDagStatus();
    }, 5000);

    return () => clearInterval(interval);
  }, []);

  // 2. Fetch Price Trends when Symbol changes
  useEffect(() => {
    if (selectedSymbol) {
      fetchPriceTrends(selectedSymbol);
    }
  }, [selectedSymbol]);

  // 3. Update Lakehouse data when selected entity or mode changes
  useEffect(() => {
    if (lakehouseMode === 'sql' && selectedEntity) {
      fetchTableRows(selectedEntity);
    } else if (lakehouseMode === 'nosql' && selectedEntity) {
      fetchMongoCollection(selectedEntity);
    }
  }, [lakehouseMode, selectedEntity]);

  // Scroll terminal logs on update
  useEffect(() => {
    if (consoleLogRef.current) {
      consoleLogRef.current.scrollTop = consoleLogRef.current.scrollHeight;
    }
  }, [dagStatus?.logs]);

  // API Calls
  const fetchCryptos = async () => {
    try {
      const res = await fetch('/api/v1/cryptos');
      const data = await res.json();
      setCryptos(data);
    } catch (e) {
      console.error('Error fetching cryptos:', e);
    }
  };

  const fetchSummary = async () => {
    try {
      const res = await fetch('/api/v1/analytics/summary');
      const data = await res.json();
      setMarketSummary(data);
      renderDonutChart(data.signal_distribution || {});
    } catch (e) {
      console.error('Error fetching summary:', e);
    }
  };

  const fetchSignals = async () => {
    try {
      const res = await fetch('/api/v1/analytics/signals');
      const data = await res.json();
      setSignals(data);
    } catch (e) {
      console.error('Error fetching signals:', e);
    }
  };

  const fetchDagStatus = async () => {
    try {
      const res = await fetch('/api/v1/pipeline/status');
      const data = await res.json();
      setDagStatus(data);
    } catch (e) {
      console.error('Error fetching DAG status:', e);
    }
  };

  const triggerDag = async () => {
    setIsTriggering(true);
    try {
      await fetch('/api/v1/pipeline/trigger', { method: 'POST' });
      await fetchDagStatus();
      setTimeout(async () => {
        await fetchDagStatus();
        await fetchCryptos();
        await fetchSignals();
        if (selectedSymbol) fetchPriceTrends(selectedSymbol);
        setIsTriggering(false);
      }, 1800);
    } catch (e) {
      console.error('Error triggering DAG:', e);
      setIsTriggering(false);
    }
  };

  const loadLakehouseMetadata = async () => {
    try {
      const [resT, resC] = await Promise.all([
        fetch('/api/v1/lakehouse/postgres/tables'),
        fetch('/api/v1/lakehouse/mongodb/collections')
      ]);
      const tables = await resT.json();
      const colls = await resC.json();
      setTablesList(tables);
      setMongoColls(colls);
      if (tables.length > 0) setSelectedEntity(tables[0].name);
    } catch (e) {
      console.error('Error loading Lakehouse meta:', e);
    }
  };

  const fetchTableRows = async (tName) => {
    try {
      const res = await fetch(`/api/v1/lakehouse/postgres/${tName}`);
      const data = await res.json();
      setTableData(data);
    } catch (e) {
      console.error('Error fetching table rows:', e);
    }
  };

  const fetchMongoCollection = async (cName) => {
    try {
      const res = await fetch(`/api/v1/lakehouse/mongodb/${cName}`);
      const data = await res.json();
      setMongoDocs(data.documents || []);
    } catch (e) {
      console.error('Error fetching mongo docs:', e);
    }
  };

  const fetchPriceTrends = async (sym) => {
    try {
      const res = await fetch(`/api/v1/analytics/price-trends?symbol=${sym}&days=30`);
      const resData = await res.json();
      renderPriceChart(resData);
    } catch (e) {
      console.error('Error loading price trends:', e);
    }
  };

  // Render Price Line & Indicators Chart
  const renderPriceChart = (chartData) => {
    if (!priceChartRef.current) return;
    const ctx = priceChartRef.current.getContext('2d');

    if (priceChartInstance.current) {
      priceChartInstance.current.destroy();
    }

    const labels = chartData.data.map(d => {
      const dt = new Date(d.timestamp);
      return dt.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
    });

    const closePrices = chartData.data.map(d => d.close);
    const sma7 = chartData.data.map(d => d.sma_7);
    const sma25 = chartData.data.map(d => d.sma_25);
    const rsi14 = chartData.data.map(d => d.rsi_14);

    const gradient = ctx.createLinearGradient(0, 0, 0, 300);
    gradient.addColorStop(0, 'rgba(0, 242, 254, 0.25)');
    gradient.addColorStop(1, 'rgba(0, 242, 254, 0.0)');

    priceChartInstance.current = new Chart(ctx, {
      type: 'line',
      data: {
        labels: labels,
        datasets: [
          {
            label: 'Close Price ($)',
            data: closePrices,
            borderColor: '#00f2fe',
            backgroundColor: gradient,
            fill: true,
            tension: 0.35,
            borderWidth: 2.5,
            pointRadius: 3,
            yAxisID: 'y'
          },
          {
            label: 'SMA-7',
            data: sma7,
            borderColor: '#38bdf8',
            borderDash: [4, 4],
            borderWidth: 1.8,
            fill: false,
            pointRadius: 0,
            hidden: !showSma7,
            tension: 0.3,
            yAxisID: 'y'
          },
          {
            label: 'SMA-25',
            data: sma25,
            borderColor: '#9d4edd',
            borderWidth: 2,
            fill: false,
            pointRadius: 0,
            hidden: !showSma25,
            tension: 0.3,
            yAxisID: 'y'
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: 'index', intersect: false },
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: 'rgba(14, 19, 31, 0.95)',
            borderColor: 'rgba(255, 255, 255, 0.15)',
            borderWidth: 1,
            titleFont: { family: 'JetBrains Mono' },
            bodyFont: { family: 'JetBrains Mono' }
          }
        },
        scales: {
          x: {
            grid: { color: 'rgba(255, 255, 255, 0.05)' },
            ticks: { color: '#94a3b8', font: { family: 'JetBrains Mono', size: 10 } }
          },
          y: {
            grid: { color: 'rgba(255, 255, 255, 0.05)' },
            ticks: {
              color: '#94a3b8',
              font: { family: 'JetBrains Mono', size: 10 },
              callback: val => `$${Number(val).toLocaleString()}`
            }
          }
        }
      }
    });

    // Render RSI Sub-Chart
    if (rsiChartRef.current) {
      const rsiCtx = rsiChartRef.current.getContext('2d');
      if (rsiChartInstance.current) rsiChartInstance.current.destroy();

      rsiChartInstance.current = new Chart(rsiCtx, {
        type: 'line',
        data: {
          labels: labels,
          datasets: [{
            label: 'RSI 14',
            data: rsi14,
            borderColor: '#f59e0b',
            borderWidth: 2,
            pointRadius: 0,
            fill: false,
            tension: 0.2
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false } },
          scales: {
            x: { display: false },
            y: {
              min: 0,
              max: 100,
              ticks: { stepSize: 30, color: '#94a3b8', font: { family: 'JetBrains Mono', size: 9 } },
              grid: { color: 'rgba(255, 255, 255, 0.05)' }
            }
          }
        }
      });
    }
  };

  const renderDonutChart = (dist) => {
    if (!donutChartRef.current) return;
    const ctx = donutChartRef.current.getContext('2d');
    if (donutChartInstance.current) donutChartInstance.current.destroy();

    const labels = Object.keys(dist);
    const data = Object.values(dist);
    if (labels.length === 0) return;

    const colorMap = {
      'STRONG_BUY': '#10b981',
      'BUY': '#34d399',
      'HOLD': '#f59e0b',
      'SELL': '#f87171',
      'STRONG_SELL': '#ef4444'
    };

    donutChartInstance.current = new Chart(ctx, {
      type: 'doughnut',
      data: {
        labels: labels,
        datasets: [{
          data: data,
          backgroundColor: labels.map(l => colorMap[l] || '#94a3b8'),
          borderWidth: 0
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: {
            position: 'right',
            labels: { color: '#94a3b8', font: { family: 'JetBrains Mono', size: 10 }, boxWidth: 12 }
          }
        },
        cutout: '70%'
      }
    });
  };

  // Status Helpers
  const isRunning = dagStatus?.is_running || isTriggering;
  const isSuccess = dagStatus?.overall_status === 'SUCCESS' && !isRunning;
  const totalWarehouseRows = tablesList.reduce((sum, t) => sum + (t.row_count || 0), 0);

  return (
    <div className="app-layout">
      {/* Top Navigation Header */}
      <header className="top-header glass-panel">
        <div className="header-left">
          <div className="brand-badge">
            <div className="brand-glow-dot"></div>
            <Activity className="brand-icon" />
          </div>
          <div>
            <h1 className="brand-title">CRYPTO<span className="gradient-text">PULSE</span> <span className="badge-de">VERCEL CLOUD</span></h1>
            <p className="brand-subtitle">Data Lakehouse • Star Schema Warehouse • Airflow DAG Orchestration • Signal Engine</p>
          </div>
        </div>

        <div className="header-right">
          <div className={`pipeline-pulse-badge ${isRunning ? 'state-running' : isSuccess ? 'state-success' : 'state-idle'}`}>
            <span className="pulse-ring"></span>
            <span className="pulse-core"></span>
            <span>{isRunning ? 'Ingesting API Stream...' : isSuccess ? 'Pipeline Synchronized (100%)' : 'Pipeline Standby'}</span>
          </div>
          <button
            className="btn-primary glow-btn"
            onClick={triggerDag}
            disabled={isRunning}
            style={{ opacity: isRunning ? 0.6 : 1 }}
          >
            {isRunning ? <Loader2 className="btn-icon animate-spin" /> : <Play className="btn-icon" />}
            <span>{isRunning ? 'Triggering Ingestion...' : 'Trigger DAG Run'}</span>
          </button>
        </div>
      </header>

      {/* Main Content Area */}
      <main className="main-container">
        {/* ZONE 1: Metrics Overview & Live Ticker Feed */}
        <section className="zone-section" id="zone-metrics">
          <div className="section-header">
            <div className="section-title-wrap">
              <TrendingUp className="section-icon text-cyan" />
              <h2>ZONE 1: Real-Time Market Overview & Asset Health</h2>
            </div>
            <div className="refresh-indicator">
              <span className="text-muted">Auto-refresh: 5s</span>
            </div>
          </div>

          {/* Live Ingestion Stream Status Banner (Red -> Yellow -> Green) */}
          <div className="ingestion-stage-banner glass-card">
            <div className="stage-step">
              <div className={`stage-dot ${isRunning ? 'status-yellow' : isSuccess ? 'status-green' : 'status-red'}`}></div>
              <div className="stage-info">
                <span className="stage-title">1. Public API Stream</span>
                <span className="stage-sub">{isRunning ? 'Extracting Binance & News...' : isSuccess ? 'Binance & News Connected' : 'Standby'}</span>
              </div>
            </div>
            <div className="stage-connector"></div>
            <div className="stage-step">
              <div className={`stage-dot ${isRunning ? 'status-yellow' : isSuccess ? 'status-green' : 'status-red'}`}></div>
              <div className="stage-info">
                <span className="stage-title">2. MongoDB Raw Ingestion</span>
                <span className="stage-sub">{isRunning ? 'Ingesting Raw Payloads...' : isSuccess ? 'MongoDB Lakehouse Loaded' : 'Awaiting Ingest'}</span>
              </div>
            </div>
            <div className="stage-connector"></div>
            <div className="stage-step">
              <div className={`stage-dot ${isRunning ? 'status-yellow' : isSuccess ? 'status-green' : 'status-red'}`}></div>
              <div className="stage-info">
                <span className="stage-title">3. PostgreSQL Star Schema</span>
                <span className="stage-sub">{isRunning ? 'Transforming & Loading...' : isSuccess ? 'PostgreSQL Star Schema OK' : 'Synchronized'}</span>
              </div>
            </div>
            <div className="stage-badge-wrap">
              <span className={`stage-live-badge ${isRunning ? 'badge-yellow' : isSuccess ? 'badge-green' : 'badge-red'}`}>
                {isRunning ? '🟡 INGESTION ACTIVE' : isSuccess ? '🟢 INGESTION SUCCESS' : '🔴 INGESTION STANDBY'}
              </span>
            </div>
          </div>

          {/* Ticker Cards Grid */}
          <div className="ticker-grid">
            {cryptos.map(coin => {
              const isPos = (coin.change_24h_pct || 0) >= 0;
              const isActive = coin.symbol === selectedSymbol;
              return (
                <div
                  key={coin.symbol}
                  className={`ticker-card glass-card ${isActive ? 'active-asset' : ''}`}
                  onClick={() => setSelectedSymbol(coin.symbol)}
                >
                  <div className="ticker-top">
                    <div className="ticker-name-wrap">
                      <span className="ticker-sym">{coin.symbol}</span>
                      <span className="ticker-name">{coin.name}</span>
                    </div>
                    <span className="ticker-rank">#{coin.market_cap_rank || '-'}</span>
                  </div>
                  <div className="ticker-price">
                    ${Number(coin.current_price || 0).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                  </div>
                  <div className="ticker-bottom">
                    <span className={`change-pill ${isPos ? 'positive' : 'negative'}`}>
                      {isPos ? '+' : ''}{coin.change_24h_pct}%
                    </span>
                    <span className="sentiment-tag">{coin.sentiment_label || 'POSITIVE'}</span>
                  </div>
                </div>
              );
            })}
          </div>

          {/* Market Sub Bar */}
          <div className="market-sub-bar glass-card">
            <div className="mood-widget">
              <div className="widget-label"><Gauge className="icon-sm" /> Market Sentiment Index</div>
              <div className="mood-content">
                <div className="mood-score">{marketSummary?.market_mood_index || 72}</div>
                <div className="mood-badge">{marketSummary?.market_mood_label || 'GREED'}</div>
                <div className="mood-bar-container">
                  <div className="mood-bar-fill" style={{ width: `${marketSummary?.market_mood_index || 72}%` }}></div>
                </div>
              </div>
            </div>
            <div className="stat-divider"></div>
            <div className="quick-stat">
              <div className="widget-label"><Database className="icon-sm" /> Star Schema Rows</div>
              <div className="quick-stat-val text-cyan">{totalWarehouseRows.toLocaleString()}</div>
            </div>
            <div className="stat-divider"></div>
            <div className="quick-stat">
              <div className="widget-label"><FileJson className="icon-sm" /> Raw Lakehouse Payloads</div>
              <div className="quick-stat-val text-purple">{mongoColls.length} Collections</div>
            </div>
            <div className="stat-divider"></div>
            <div className="quick-stat">
              <div className="widget-label"><Zap className="icon-sm" /> Active Signal Confluence</div>
              <div className="quick-stat-val text-emerald">{signals.length}</div>
            </div>
          </div>
        </section>

        {/* ZONE 2: Analytics Dashboard (Price Charts & Trading Signals) */}
        <section className="zone-section" id="zone-analytics">
          <div className="section-header">
            <div className="section-title-wrap">
              <BarChart2 className="section-icon text-purple" />
              <h2>ZONE 2: Technical Feature Engineering & Quantitative Signals</h2>
            </div>
            <div className="symbol-tabs">
              {['BTC', 'ETH', 'SOL', 'BNB', 'ADA', 'XRP'].map(sym => (
                <button
                  key={sym}
                  className={`tab-btn ${selectedSymbol === sym ? 'active' : ''}`}
                  onClick={() => setSelectedSymbol(sym)}
                >
                  {sym}
                </button>
              ))}
            </div>
          </div>

          <div className="analytics-grid">
            {/* Main Chart Panel */}
            <div className="chart-panel glass-card">
              <div className="chart-header">
                <div className="chart-title-area">
                  <h3>{selectedSymbol} Price & Engineered Indicator Trends</h3>
                  <span className="chart-subtitle">Features: SMA-7, SMA-25, EMA-12, EMA-26, RSI-14</span>
                </div>
                <div className="chart-controls">
                  <label className="toggle-control">
                    <input type="checkbox" checked={showSma7} onChange={e => setShowSma7(e.target.checked)} />
                    <span className="toggle-label text-cyan">SMA 7</span>
                  </label>
                  <label className="toggle-control">
                    <input type="checkbox" checked={showSma25} onChange={e => setShowSma25(e.target.checked)} />
                    <span className="toggle-label text-purple">SMA 25</span>
                  </label>
                  <label className="toggle-control">
                    <input type="checkbox" checked={showRsi} onChange={e => setShowRsi(e.target.checked)} />
                    <span className="toggle-label text-amber">RSI 14</span>
                  </label>
                </div>
              </div>

              <div className="chart-wrapper">
                <canvas ref={priceChartRef}></canvas>
              </div>

              {showRsi && (
                <div className="rsi-chart-wrapper">
                  <div className="rsi-label">RSI (14) Relative Strength Index</div>
                  <canvas ref={rsiChartRef}></canvas>
                </div>
              )}
            </div>

            {/* Signals Breakdown & Live Feed */}
            <div className="signals-panel glass-card">
              <div className="panel-inner-header">
                <h3><Radio className="icon-sm text-emerald" /> Algorithmic Trading Signals</h3>
                <span className="badge-neutral">{signals.length} Signals</span>
              </div>

              <div className="donut-chart-container">
                <canvas ref={donutChartRef}></canvas>
              </div>

              <div className="signals-list-wrap">
                {signals.slice(0, 6).map((s, idx) => (
                  <div key={idx} className="signal-card">
                    <div className="signal-card-top">
                      <span className="signal-sym-badge">
                        {s.symbol} <span className="text-muted">@ ${Number(s.trigger_price).toLocaleString()}</span>
                      </span>
                      <span className={`signal-badge ${s.signal_name}`}>{s.signal_name}</span>
                    </div>
                    <div className="signal-confidence-bar">
                      <div className="signal-confidence-fill" style={{ width: `${s.confidence_score}%` }}></div>
                    </div>
                    <div className="signal-reason">{s.technical_reason}</div>
                    <div className="signal-meta">
                      <span>Confidence: <strong>{s.confidence_score}%</strong></span>
                      <span>{new Date(s.generated_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </section>

        {/* ZONE 3: Pipeline Orchestration & Airflow DAG Monitor */}
        <section className="zone-section" id="zone-pipeline">
          <div className="section-header">
            <div className="section-title-wrap">
              <GitBranch className="section-icon text-amber" />
              <h2>ZONE 3: Apache Airflow DAG Orchestration & Execution Monitor</h2>
            </div>
            <div className="dag-meta-info">
              <span className="code-tag">DAG: crypto_market_intelligence_pipeline</span>
              <span className="code-tag">Interval: @hourly</span>
            </div>
          </div>

          <div className="dag-monitor-card glass-card">
            {/* Dynamic Multi-Color Pipeline Progress Bar */}
            <div className="dag-progress-container">
              <div className="dag-progress-header">
                <span className="progress-title"><Activity className="icon-sm" /> Pipeline Flow Lifecycle</span>
                <span className={`progress-percentage ${isRunning ? 'text-amber' : 'text-emerald'}`}>
                  {isRunning ? '66% Ingestion in Progress' : '100% Synchronized'}
                </span>
              </div>
              <div className="dag-progress-track">
                <div className="dag-progress-bar" style={{ width: isRunning ? '66%' : '100%' }}></div>
              </div>
            </div>

            {/* Flowchart Nodes Graph */}
            <div className="dag-graph-flow">
              {dagStatus?.tasks?.map((t, idx) => {
                const nodeStatus = isRunning && idx === 1 ? 'RUNNING' : t.status;
                const icons = [Wifi, DownloadCloud, FileJson, Cpu, Database, Sparkles];
                const IconComp = icons[idx] || Activity;
                return (
                  <React.Fragment key={t.id}>
                    <div className={`dag-node node-${nodeStatus.toLowerCase()}`}>
                      <div className="node-badge-idx">{idx + 1}</div>
                      <div className="node-icon-box"><IconComp className="w-4 h-4" /></div>
                      <div className="node-info">
                        <div className="node-title">{t.id}</div>
                        <div className={`node-status status-${nodeStatus.toLowerCase()}`}>{nodeStatus}</div>
                      </div>
                    </div>
                    {idx < dagStatus.tasks.length - 1 && (
                      <div className="dag-arrow arrow-active"><ArrowRight className="w-4 h-4" /></div>
                    )}
                  </React.Fragment>
                );
              })}
            </div>

            {/* Console Log Terminal */}
            <div className="dag-console-wrapper">
              <div className="console-header">
                <div className="console-title"><Terminal className="icon-sm" /> Execution Logs & Audit Stream</div>
                <span className="badge-live-pulse">LIVE STREAM</span>
              </div>
              <div className="console-body" ref={consoleLogRef}>
                {dagStatus?.logs?.map((log, lIdx) => (
                  <div key={lIdx} className="log-line">{log}</div>
                ))}
              </div>
            </div>
          </div>
        </section>

        {/* ZONE 4: Data Lakehouse & Star Schema Explorer */}
        <section className="zone-section" id="zone-lakehouse">
          <div className="section-header">
            <div className="section-title-wrap">
              <Database className="section-icon text-emerald" />
              <h2>ZONE 4: Data Lakehouse & Star Schema Explorer</h2>
            </div>
            <div className="explorer-mode-switch">
              <button
                className={`mode-btn ${lakehouseMode === 'sql' ? 'active' : ''}`}
                onClick={() => setLakehouseMode('sql')}
              >
                <Table className="icon-sm" /> PostgreSQL Star Schema
              </button>
              <button
                className={`mode-btn ${lakehouseMode === 'nosql' ? 'active' : ''}`}
                onClick={() => setLakehouseMode('nosql')}
              >
                <FileCode className="icon-sm" /> MongoDB Raw Lakehouse (NoSQL)
              </button>
            </div>
          </div>

          <div className="lakehouse-container glass-card">
            {/* Sidebar */}
            <div className="lakehouse-sidebar">
              <div className="sidebar-header">{lakehouseMode === 'sql' ? 'SQL Tables & Views' : 'MongoDB Collections'}</div>
              <ul className="entity-list">
                {lakehouseMode === 'sql'
                  ? tablesList.map(t => (
                      <li
                        key={t.name}
                        className={`entity-item ${selectedEntity === t.name ? 'active' : ''}`}
                        onClick={() => setSelectedEntity(t.name)}
                      >
                        <span>{t.name}</span>
                        <span className="entity-count">{t.row_count}</span>
                      </li>
                    ))
                  : mongoColls.map(c => (
                      <li
                        key={c.name}
                        className={`entity-item ${selectedEntity === c.name ? 'active' : ''}`}
                        onClick={() => setSelectedEntity(c.name)}
                      >
                        <span>{c.name}</span>
                        <span className="entity-count">RAW</span>
                      </li>
                    ))}
              </ul>
            </div>

            {/* Display Area */}
            <div className="lakehouse-content-area">
              <div className="content-header">
                <div className="active-entity-info">
                  <h3>{selectedEntity}</h3>
                  <span className="entity-badge">{lakehouseMode === 'sql' ? 'STAR SCHEMA TABLE' : 'MONGODB_RAW'}</span>
                </div>
                <span className="row-count-tag">
                  {lakehouseMode === 'sql' ? `Rows: ${tableData?.total_rows || 0}` : `Documents: ${mongoDocs.length}`}
                </span>
              </div>

              {lakehouseMode === 'sql' ? (
                <div className="table-responsive">
                  <table className="custom-table">
                    <thead>
                      <tr>
                        {tableData?.columns?.map(col => <th key={col}>{col}</th>)}
                      </tr>
                    </thead>
                    <tbody>
                      {tableData?.data?.map((row, rIdx) => (
                        <tr key={rIdx}>
                          {tableData.columns.map(col => (
                            <td key={col}>{row[col] !== null && row[col] !== undefined ? String(row[col]) : <span className="text-muted">null</span>}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="json-viewer-container">
                  <div className="json-viewer-bar">
                    <span>Format: BSON / JSON (Raw Payload Stream)</span>
                    <button
                      className="btn-copy"
                      onClick={() => {
                        navigator.clipboard.writeText(JSON.stringify(mongoDocs, null, 2));
                        alert('Raw Lakehouse JSON copied to clipboard!');
                      }}
                    >
                      <Copy className="icon-sm" /> Copy JSON
                    </button>
                  </div>
                  <pre className="json-code-box">
                    <code>{JSON.stringify(mongoDocs, null, 2)}</code>
                  </pre>
                </div>
              )}
            </div>
          </div>
        </section>
      </main>

      {/* Footer */}
      <footer className="app-footer">
        <div className="footer-content">
          <div>
            <strong>Crypto-Market Trends & Trading Signal Intelligence Platform</strong>
            <p className="text-muted">Data Engineering Portfolio Project • Python ETL • Airflow DAG • PostgreSQL Star Schema • MongoDB NoSQL • Next.js React • Vercel Ready</p>
          </div>
          <div className="footer-links">
            <a href="/api/v1/cryptos" target="_blank" className="footer-link"><BookOpen className="icon-sm" /> API Endpoints</a>
            <a href="/api/v1/pipeline/status" target="_blank" className="footer-link"><HeartPulse className="icon-sm" /> DAG Health</a>
          </div>
        </div>
      </footer>
    </div>
  );
}
