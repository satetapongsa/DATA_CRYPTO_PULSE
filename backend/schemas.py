"""
Pydantic Schemas for API Serialization & Type Validation
"""

from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field


class CryptoSummary(BaseModel):
    symbol: str
    name: str
    category: Optional[str] = None
    market_cap_rank: Optional[int] = None
    current_price: Optional[float] = None
    change_24h_pct: Optional[float] = None
    volume_24h: Optional[float] = None
    sma_7: Optional[float] = None
    sma_25: Optional[float] = None
    rsi_14: Optional[float] = None
    sentiment_label: Optional[str] = "NEUTRAL"
    last_updated: Optional[str] = None


class PricePoint(BaseModel):
    timestamp: str
    date_key: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    sma_7: Optional[float] = None
    sma_25: Optional[float] = None
    ema_12: Optional[float] = None
    ema_26: Optional[float] = None
    rsi_14: Optional[float] = None
    macd: Optional[float] = None
    macd_signal: Optional[float] = None


class PriceTrendsResponse(BaseModel):
    symbol: str
    name: str
    count: int
    data: List[PricePoint]


class TradingSignal(BaseModel):
    signal_id: int
    symbol: str
    crypto_name: str
    category: str
    signal_name: str
    risk_level: str
    strategy_name: str
    trigger_price: float
    confidence_score: float
    technical_reason: str
    sentiment_weight: Optional[float] = 0.0
    generated_at: str


class DAGTaskStatus(BaseModel):
    id: str
    name: str
    description: str
    status: str
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    duration_seconds: Optional[float] = 0.0
    error: Optional[str] = None


class PipelineStatusResponse(BaseModel):
    dag_id: str
    schedule_interval: str
    is_running: bool
    overall_status: str
    last_run_time: Optional[str] = None
    current_task: Optional[str] = None
    tasks: List[DAGTaskStatus]
    metrics: Dict[str, Any] = Field(default_factory=dict)
    recent_logs: List[str] = Field(default_factory=list)


class TableSchemaInfo(BaseModel):
    name: str
    type: str
    description: str
    row_count: int


class LakehouseTableResponse(BaseModel):
    table_name: str
    total_rows: int
    columns: List[str]
    data: List[Dict[str, Any]]
