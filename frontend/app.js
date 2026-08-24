/**
 * =============================================================================
 * CRYPTO INTELLIGENCE PLATFORM - DASHBOARD APPLICATION LOGIC
 * =============================================================================
 */

// Global State
const state = {
    selectedSymbol: 'BTC',
    priceChart: null,
    rsiChart: null,
    donutChart: null,
    lakehouseMode: 'sql', // 'sql' or 'nosql'
    selectedEntity: 'dim_cryptos',
    isPipelineRunning: false,
    pollInterval: null
};

// Colors
const PALETTE = {
    cyan: '#00f2fe',
    cyanAlpha: 'rgba(0, 242, 254, 0.2)',
    purple: '#9d4edd',
    purpleAlpha: 'rgba(157, 78, 221, 0.2)',
    emerald: '#10b981',
    emeraldAlpha: 'rgba(16, 185, 129, 0.2)',
    amber: '#f59e0b',
    rose: '#ef4444',
    textMuted: '#94a3b8',
    gridLines: 'rgba(255, 255, 255, 0.05)'
};

// -----------------------------------------------------------------------------
// 1. INITIALIZATION & LIFECYCLE
// -----------------------------------------------------------------------------
document.addEventListener('DOMContentLoaded', async () => {
    lucide.createIcons();
    
    // Initial Data Fetches
    await Promise.all([
        fetchCryptosList(),
        fetchMarketSummary(),
        loadPriceTrends(state.selectedSymbol),
        fetchTradingSignals(),
        fetchPipelineStatus(),
        loadLakehouseEntities()
    ]);

    // Start Real-Time Polling (every 5 seconds)
    state.pollInterval = setInterval(async () => {
        await Promise.all([
            fetchPipelineStatus(),
            fetchMarketSummary(),
            fetchTradingSignals()
        ]);
    }, 5000);
});

// -----------------------------------------------------------------------------
// 2. ZONE 1: MARKET METRICS & TICKERS
// -----------------------------------------------------------------------------
async function fetchCryptosList() {
    try {
        const res = await fetch('/api/v1/cryptos');
        const cryptos = await res.json();
        renderTickerCards(cryptos);
    } catch (err) {
        console.error('Error fetching cryptos:', err);
    }
}

function renderTickerCards(cryptos) {
    const container = document.getElementById('ticker-cards-container');
    if (!container) return;

    container.innerHTML = cryptos.map(coin => {
        const isPos = (coin.change_24h_pct || 0) >= 0;
        const changeSign = isPos ? '+' : '';
        const changeClass = isPos ? 'positive' : 'negative';
        const activeClass = coin.symbol === state.selectedSymbol ? 'active-asset' : '';
        const priceStr = coin.current_price ? `$${Number(coin.current_price).toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}` : '--';
        
        return `
            <div class="ticker-card glass-card ${activeClass}" onclick="selectCryptoSymbol('${coin.symbol}')">
                <div class="ticker-top">
                    <div class="ticker-name-wrap">
                        <span class="ticker-sym">${coin.symbol}</span>
                        <span class="ticker-name">${coin.name}</span>
                    </div>
                    <span class="ticker-rank">#${coin.market_cap_rank || '-'}</span>
                </div>
                <div class="ticker-price">${priceStr}</div>
                <div class="ticker-bottom">
                    <span class="change-pill ${changeClass}">${changeSign}${coin.change_24h_pct || 0}%</span>
                    <span class="sentiment-tag">${coin.sentiment_label || 'NEUTRAL'}</span>
                </div>
            </div>
        `;
    }).join('');
}

async function fetchMarketSummary() {
    try {
        const res = await fetch('/api/v1/analytics/summary');
        const summary = await res.json();

        // Update Mood Widget
        const score = summary.market_mood_index || 50;
        const label = summary.market_mood_label || 'NEUTRAL';
        
        document.getElementById('mood-score-val').textContent = score;
        const badge = document.getElementById('mood-badge-label');
        badge.textContent = label;
        badge.className = `mood-badge ${label.toLowerCase()}`;
        document.getElementById('mood-bar-fill').style.width = `${score}%`;

        // Update Signal Confluence Quick Stat
        const dist = summary.signal_distribution || {};
        const totalSignals = Object.values(dist).reduce((a, b) => a + b, 0);
        document.getElementById('active-signals-count').textContent = totalSignals;

        // Render Donut Chart in Zone 2
        renderSignalDistributionChart(dist);
    } catch (err) {
        console.error('Error fetching market summary:', err);
    }
}

// -----------------------------------------------------------------------------
// 3. ZONE 2: PRICE & TECHNICAL INDICATOR CHARTS
// -----------------------------------------------------------------------------
async function selectCryptoSymbol(symbol) {
    state.selectedSymbol = symbol;
    
    // Update Tab UI
    document.querySelectorAll('#symbol-tabs-container .tab-btn').forEach(btn => {
        if (btn.dataset.symbol === symbol) {
            btn.classList.add('active');
        } else {
            btn.classList.remove('active');
        }
    });

    // Update Ticker Card UI
    document.querySelectorAll('.ticker-card').forEach(card => {
        if (card.querySelector('.ticker-sym')?.textContent === symbol) {
            card.classList.add('active-asset');
        } else {
            card.classList.remove('active-asset');
        }
    });

    await loadPriceTrends(symbol);
}

async function loadPriceTrends(symbol) {
    try {
        const res = await fetch(`/api/v1/analytics/price-trends?symbol=${symbol}&days=30`);
        const result = await res.json();
        
        document.getElementById('chart-asset-title').textContent = `${result.name} (${result.symbol}/USDT) Price & Indicator Trends`;

        const labels = result.data.map(d => {
            const dt = new Date(d.timestamp);
            return dt.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
        });

        const closePrices = result.data.map(d => d.close);
        const sma7 = result.data.map(d => d.sma_7);
        const sma25 = result.data.map(d => d.sma_25);
        const rsi14 = result.data.map(d => d.rsi_14);

        renderPriceChart(labels, closePrices, sma7, sma25);
        renderRsiChart(labels, rsi14);
    } catch (err) {
        console.error('Error loading price trends:', err);
    }
}

function renderPriceChart(labels, closePrices, sma7, sma25) {
    const ctx = document.getElementById('priceTrendsChart').getContext('2d');
    
    if (state.priceChart) {
        state.priceChart.destroy();
    }

    // Create price gradient
    const gradient = ctx.createLinearGradient(0, 0, 0, 300);
    gradient.addColorStop(0, 'rgba(0, 242, 254, 0.25)');
    gradient.addColorStop(1, 'rgba(0, 242, 254, 0.0)');

    state.priceChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Close Price ($)',
                    data: closePrices,
                    borderColor: PALETTE.cyan,
                    backgroundColor: gradient,
                    fill: true,
                    tension: 0.35,
                    borderWidth: 2.5,
                    pointRadius: 3,
                    pointHoverRadius: 6,
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
                    tension: 0.3,
                    yAxisID: 'y'
                },
                {
                    label: 'SMA-25',
                    data: sma25,
                    borderColor: PALETTE.purple,
                    borderWidth: 2,
                    fill: false,
                    pointRadius: 0,
                    tension: 0.3,
                    yAxisID: 'y'
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                mode: 'index',
                intersect: false
            },
            plugins: {
                legend: {
                    display: false
                },
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
                    grid: { color: PALETTE.gridLines },
                    ticks: { color: PALETTE.textMuted, font: { family: 'JetBrains Mono', size: 10 } }
                },
                y: {
                    grid: { color: PALETTE.gridLines },
                    ticks: {
                        color: PALETTE.textMuted,
                        font: { family: 'JetBrains Mono', size: 10 },
                        callback: val => `$${Number(val).toLocaleString()}`
                    }
                }
            }
        }
    });
}

function renderRsiChart(labels, rsiData) {
    const ctx = document.getElementById('rsiChart').getContext('2d');

    if (state.rsiChart) {
        state.rsiChart.destroy();
    }

    state.rsiChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'RSI 14',
                data: rsiData,
                borderColor: PALETTE.amber,
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
                    ticks: {
                        stepSize: 30,
                        color: PALETTE.textMuted,
                        font: { family: 'JetBrains Mono', size: 9 }
                    },
                    grid: { color: PALETTE.gridLines }
                }
            }
        }
    });
}

function toggleChartDataset(datasetLabel) {
    if (!state.priceChart) return;
    const ds = state.priceChart.data.datasets.find(d => d.label === datasetLabel);
    if (ds) {
        ds.hidden = !ds.hidden;
        state.priceChart.update();
    }
}

function toggleRsiChart() {
    const container = document.getElementById('rsi-container');
    container.style.display = container.style.display === 'none' ? 'block' : 'none';
}

function renderSignalDistributionChart(dist) {
    const ctx = document.getElementById('signalDistributionChart').getContext('2d');
    
    const labels = Object.keys(dist);
    const data = Object.values(dist);

    if (labels.length === 0) return;

    if (state.donutChart) {
        state.donutChart.destroy();
    }

    const colorMap = {
        'STRONG_BUY': '#10b981',
        'BUY': '#34d399',
        'HOLD': '#f59e0b',
        'SELL': '#f87171',
        'STRONG_SELL': '#ef4444'
    };

    const backgroundColors = labels.map(l => colorMap[l] || '#94a3b8');

    state.donutChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                data: data,
                backgroundColor: backgroundColors,
                borderWidth: 0
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'right',
                    labels: {
                        color: PALETTE.textMuted,
                        font: { family: 'JetBrains Mono', size: 10 },
                        boxWidth: 12
                    }
                }
            },
            cutout: '70%'
        }
    });
}

async function fetchTradingSignals() {
    try {
        const res = await fetch('/api/v1/analytics/signals?limit=6');
        const signals = await res.json();
        
        const container = document.getElementById('signals-feed-container');
        if (!container) return;

        if (signals.length === 0) {
            container.innerHTML = '<div class="loading-placeholder">No trading signals generated yet. Run pipeline!</div>';
            return;
        }

        container.innerHTML = signals.map(s => {
            const timeStr = new Date(s.generated_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
            return `
                <div class="signal-card">
                    <div class="signal-card-top">
                        <span class="signal-sym-badge">${s.symbol} <span class="text-muted">@ $${Number(s.trigger_price).toLocaleString()}</span></span>
                        <span class="signal-badge ${s.signal_name}">${s.signal_name}</span>
                    </div>
                    <div class="signal-confidence-bar">
                        <div class="signal-confidence-fill" style="width: ${s.confidence_score}%;"></div>
                    </div>
                    <div class="signal-reason">${s.technical_reason}</div>
                    <div class="signal-meta">
                        <span>Confidence: <strong>${s.confidence_score}%</strong></span>
                        <span>${timeStr}</span>
                    </div>
                </div>
            `;
        }).join('');
    } catch (err) {
        console.error('Error fetching signals:', err);
    }
}

// -----------------------------------------------------------------------------
// 4. ZONE 3: AIRFLOW DAG PIPELINE ORCHESTRATOR
// -----------------------------------------------------------------------------
async function fetchPipelineStatus() {
    try {
        const res = await fetch('/api/v1/pipeline/status');
        const status = await res.json();

        // Update Top Header Pulse Badge
        const headerBadgeText = document.getElementById('header-pipeline-status-text');
        const triggerBtn = document.getElementById('btn-trigger-pipeline');

        if (status.is_running) {
            headerBadgeText.textContent = `Running Task: ${status.current_task || '...'}`;
            triggerBtn.disabled = true;
            triggerBtn.style.opacity = '0.6';
        } else {
            headerBadgeText.textContent = `Pipeline: ${status.overall_status}`;
            triggerBtn.disabled = false;
            triggerBtn.style.opacity = '1';
        }

        // Update DAG Flowchart Nodes
        status.tasks.forEach(t => {
            const nodeEl = document.getElementById(`node-${t.id}`);
            if (nodeEl) {
                const statusEl = nodeEl.querySelector('.node-status');
                statusEl.textContent = t.status;
                statusEl.className = `node-status status-${t.status.toLowerCase()}`;
            }
        });

        // Update Console Logs
        const consoleEl = document.getElementById('pipeline-logs-console');
        if (consoleEl && status.recent_logs.length > 0) {
            consoleEl.innerHTML = status.recent_logs.map(log => `<div class="log-line">${log}</div>`).join('');
            consoleEl.scrollTop = consoleEl.scrollHeight;
        }
    } catch (err) {
        console.error('Error fetching pipeline status:', err);
    }
}

async function triggerPipelineRun() {
    try {
        const btn = document.getElementById('btn-trigger-pipeline');
        btn.disabled = true;
        btn.innerHTML = `<i data-lucide="loader" class="btn-icon"></i> <span>Triggering...</span>`;
        lucide.createIcons();

        const res = await fetch('/api/v1/pipeline/trigger', { method: 'POST' });
        const data = await res.json();
        console.log('Trigger response:', data);

        // Immediate poll
        setTimeout(async () => {
            await fetchPipelineStatus();
            btn.innerHTML = `<i data-lucide="play" class="btn-icon"></i> <span>Trigger DAG Run</span>`;
            lucide.createIcons();
        }, 800);
    } catch (err) {
        console.error('Error triggering pipeline:', err);
    }
}

// -----------------------------------------------------------------------------
// 5. ZONE 4: LAKEHOUSE & STAR SCHEMA EXPLORER
// -----------------------------------------------------------------------------
async function loadLakehouseEntities() {
    if (state.lakehouseMode === 'sql') {
        await loadSqlWarehouseTables();
    } else {
        await loadNoSqlCollections();
    }
}

function switchLakehouseMode(mode) {
    state.lakehouseMode = mode;
    
    document.getElementById('btn-mode-sql').classList.toggle('active', mode === 'sql');
    document.getElementById('btn-mode-nosql').classList.toggle('active', mode === 'nosql');

    const sqlContainer = document.getElementById('sql-table-container');
    const nosqlContainer = document.getElementById('nosql-json-container');

    if (mode === 'sql') {
        sqlContainer.style.display = 'block';
        nosqlContainer.style.display = 'none';
        document.getElementById('lakehouse-sidebar-title').textContent = 'SQL Tables & Views';
        loadSqlWarehouseTables();
    } else {
        sqlContainer.style.display = 'none';
        nosqlContainer.style.display = 'block';
        document.getElementById('lakehouse-sidebar-title').textContent = 'MongoDB Raw Lakehouse';
        loadNoSqlCollections();
    }
}

async function loadSqlWarehouseTables() {
    try {
        const res = await fetch('/api/v1/lakehouse/postgres/tables');
        const tables = await res.json();

        // Calculate total warehouse rows for Zone 1
        const totalRows = tables.reduce((sum, t) => sum + (t.row_count || 0), 0);
        document.getElementById('total-warehouse-rows').textContent = totalRows.toLocaleString();

        const sidebarList = document.getElementById('lakehouse-entities-list');
        sidebarList.innerHTML = tables.map(t => `
            <li class="entity-item ${t.name === state.selectedEntity ? 'active' : ''}" onclick="selectLakehouseEntity('${t.name}', '${t.type}')">
                <span>${t.name}</span>
                <span class="entity-count">${t.row_count}</span>
            </li>
        `).join('');

        // Select first table if current not found or on mode switch
        if (!tables.some(t => t.name === state.selectedEntity)) {
            state.selectedEntity = tables[0].name;
        }
        document.getElementById('current-entity-name').textContent = state.selectedEntity;
        const currentT = tables.find(t => t.name === state.selectedEntity);
        document.getElementById('current-entity-badge').textContent = currentT ? currentT.type : 'TABLE';

        await fetchTableData(state.selectedEntity);
    } catch (err) {
        console.error('Error loading warehouse tables:', err);
    }
}

async function loadNoSqlCollections() {
    try {
        const res = await fetch('/api/v1/lakehouse/mongodb/collections');
        const collections = await res.json();

        document.getElementById('total-lakehouse-docs').textContent = `${collections.length} Collections`;

        const sidebarList = document.getElementById('lakehouse-entities-list');
        sidebarList.innerHTML = collections.map(c => `
            <li class="entity-item ${c.name === state.selectedEntity ? 'active' : ''}" onclick="selectLakehouseEntity('${c.name}', 'MONGODB_DOCS')">
                <span>${c.name}</span>
                <span class="entity-count">RAW</span>
            </li>
        `).join('');

        if (!collections.some(c => c.name === state.selectedEntity)) {
            state.selectedEntity = collections[0].name;
        }
        document.getElementById('current-entity-name').textContent = state.selectedEntity;
        document.getElementById('current-entity-badge').textContent = 'MONGODB_RAW';

        await fetchMongoDocuments(state.selectedEntity);
    } catch (err) {
        console.error('Error loading NoSQL collections:', err);
    }
}

async function selectLakehouseEntity(name, type) {
    state.selectedEntity = name;
    document.getElementById('current-entity-name').textContent = name;
    document.getElementById('current-entity-badge').textContent = type;

    // Highlight sidebar item
    document.querySelectorAll('.entity-item').forEach(item => {
        if (item.querySelector('span')?.textContent === name) {
            item.classList.add('active');
        } else {
            item.classList.remove('active');
        }
    });

    if (state.lakehouseMode === 'sql') {
        await fetchTableData(name);
    } else {
        await fetchMongoDocuments(name);
    }
}

async function fetchTableData(tableName) {
    try {
        const res = await fetch(`/api/v1/lakehouse/postgres/${tableName}?limit=30`);
        const data = await res.json();

        document.getElementById('current-entity-rows').textContent = `Rows: ${data.total_rows}`;

        const thead = document.getElementById('lakehouse-table-head');
        const tbody = document.getElementById('lakehouse-table-body');

        // Render Head
        thead.innerHTML = `<tr>${data.columns.map(c => `<th>${c}</th>`).join('')}</tr>`;

        // Render Body
        tbody.innerHTML = data.data.map(row => {
            return `<tr>${data.columns.map(c => `<td>${row[c] !== null && row[c] !== undefined ? row[c] : '<span class="text-muted">null</span>'}</td>`).join('')}</tr>`;
        }).join('');
    } catch (err) {
        console.error('Error fetching table data:', err);
    }
}

async function fetchMongoDocuments(collectionName) {
    try {
        const res = await fetch(`/api/v1/lakehouse/mongodb/${collectionName}?limit=15`);
        const data = await res.json();

        document.getElementById('current-entity-rows').textContent = `Documents: ${data.count}`;
        const codeEl = document.getElementById('lakehouse-json-code');
        codeEl.textContent = JSON.stringify(data.documents, null, 2);
    } catch (err) {
        console.error('Error fetching MongoDB documents:', err);
    }
}

function copyJsonPayload() {
    const codeEl = document.getElementById('lakehouse-json-code');
    navigator.clipboard.writeText(codeEl.textContent).then(() => {
        alert('Raw Lakehouse JSON copied to clipboard!');
    });
}
