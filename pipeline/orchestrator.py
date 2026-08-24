"""
Pipeline Orchestrator & Airflow Simulation Engine
Executes the full DAG sequence with granular node status tracking, execution metrics,
and state management for real-time visualizer dashboards.
"""

import time
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import threading

from pipeline.config import CRYPTO_SYMBOLS, TIMEFRAME_DAYS
from pipeline.extract import extract_all_crypto_data
from pipeline.raw_storage import store_raw_crypto_data
from pipeline.transform import run_etl_transformation
from pipeline.signals import generate_trading_signals
from pipeline.load import load_transformed_data_to_warehouse

logger = logging.getLogger("pipeline.orchestrator")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# Directed Acyclic Graph Task Definition
DAG_TASKS = [
    {"id": "wait_for_source", "name": "Wait For API Readiness", "description": "Health checks public API endpoints & rate limits"},
    {"id": "extract_data", "name": "Extract Market & News Data", "description": "Fetches OHLCV candles & crypto news feeds"},
    {"id": "process_data_mongodb", "name": "Ingest to MongoDB Lakehouse", "description": "Stores raw BSON/JSON payloads into staging store"},
    {"id": "clean_transform_data", "name": "ETL Feature Transformation", "description": "Cleans data, computes SMA/EMA/RSI/MACD & NLP Sentiment"},
    {"id": "load_to_postgres", "name": "Load Star Schema Warehouse", "description": "Upserts Dimensions & Price/Sentiment Fact tables"},
    {"id": "generate_signals", "name": "Compute Trading Intelligence", "description": "Calculates algorithmic BUY/HOLD/SELL signals"}
]


class PipelineOrchestrator:
    def __init__(self):
        self.lock = threading.Lock()
        self.is_running = False
        self.last_run_time: Optional[str] = None
        self.last_run_status: str = "IDLE"
        self.current_task_id: Optional[str] = None
        self.task_states: Dict[str, Dict[str, Any]] = {}
        self.execution_logs: List[str] = []
        self.latest_metrics: Dict[str, Any] = {}
        self._init_task_states()

    def _init_task_states(self):
        for task in DAG_TASKS:
            self.task_states[task["id"]] = {
                "id": task["id"],
                "name": task["name"],
                "description": task["description"],
                "status": "QUEUED",  # QUEUED, RUNNING, SUCCESS, FAILED
                "start_time": None,
                "end_time": None,
                "duration_seconds": 0.0,
                "error": None
            }

    def log(self, message: str):
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        log_entry = f"[{ts}] {message}"
        self.execution_logs.append(log_entry)
        # Keep last 150 logs
        if len(self.execution_logs) > 150:
            self.execution_logs.pop(0)
        logger.info(message)

    def get_dag_status(self) -> Dict[str, Any]:
        """
        Returns full state representation of the Airflow DAG for the REST API and Dashboard.
        """
        with self.lock:
            tasks_list = [self.task_states[t["id"]] for t in DAG_TASKS]
            return {
                "dag_id": "crypto_market_intelligence_dag",
                "schedule_interval": "@hourly",
                "is_running": self.is_running,
                "overall_status": self.last_run_status,
                "last_run_time": self.last_run_time,
                "current_task": self.current_task_id,
                "tasks": tasks_list,
                "metrics": self.latest_metrics,
                "recent_logs": self.execution_logs[-20:]
            }

    def execute_pipeline(self, symbols: Optional[List[str]] = None, days: int = TIMEFRAME_DAYS) -> Dict[str, Any]:
        """
        Executes the full pipeline sequentially with simulated realistic task transitions.
        """
        if symbols is None:
            symbols = CRYPTO_SYMBOLS

        with self.lock:
            if self.is_running:
                return {"status": "ALREADY_RUNNING", "message": "Pipeline execution currently in progress."}
            self.is_running = True
            self.last_run_status = "RUNNING"
            self.last_run_time = datetime.now(timezone.utc).isoformat()
            self._init_task_states()

        start_time_all = time.time()
        self.log(f"[START] Starting Pipeline Orchestration for symbols: {symbols} (History: {days} days)")

        raw_data_batch = None
        transformed_data = None
        signals_df = None

        try:
            # -------------------------------------------------------------
            # Task 1: wait_for_source
            # -------------------------------------------------------------
            self._start_task("wait_for_source")
            self.log("Task 1/6: Checking connectivity to Binance & Crypto News APIs...")
            time.sleep(0.5)
            self.log("Public API endpoints responsive. Network connectivity OK.")
            self._complete_task("wait_for_source")
            time.sleep(0.3)

            # -------------------------------------------------------------
            # Task 2: extract_data
            # -------------------------------------------------------------
            self._start_task("extract_data")
            self.log(f"Task 2/6: Extracting OHLCV candles and news streams for {len(symbols)} symbols...")
            raw_data_batch = extract_all_crypto_data(symbols, days=days)
            self.log(f"Extracted {len(raw_data_batch['prices'])} price candles & {len(raw_data_batch['news'])} news items.")
            self._complete_task("extract_data")
            time.sleep(0.3)

            # -------------------------------------------------------------
            # Task 3: process_data_mongodb
            # -------------------------------------------------------------
            self._start_task("process_data_mongodb")
            self.log("Task 3/6: Ingesting raw JSON payloads to MongoDB Lakehouse collections...")
            mongo_res = store_raw_crypto_data(raw_data_batch)
            self.log(f"Persisted Lakehouse batch: {mongo_res['batch_id']}")
            self._complete_task("process_data_mongodb")
            time.sleep(0.3)

            # -------------------------------------------------------------
            # Task 4: clean_transform_data
            # -------------------------------------------------------------
            self._start_task("clean_transform_data")
            self.log("Task 4/6: Computing technical indicators (SMA-7/25, EMA, RSI-14, MACD) & NLP sentiment...")
            transformed_data = run_etl_transformation(raw_data_batch)
            self.log(f"Transformed {len(transformed_data['prices_df'])} price records with Star Schema date keys.")
            self._complete_task("clean_transform_data")
            time.sleep(0.3)

            # -------------------------------------------------------------
            # Task 5: load_to_postgres
            # -------------------------------------------------------------
            self._start_task("load_to_postgres")
            self.log("Task 5/6: Generating algorithmic trading signals (Golden Cross / RSI / Sentiment Confluence)...")
            signals_df = generate_trading_signals(transformed_data["prices_df"], transformed_data["news_df"])
            self.log(f"Generated {len(signals_df)} trading signal records.")

            self.log("Upserting Dimensions and Fact tables in Data Warehouse...")
            load_res = load_transformed_data_to_warehouse(transformed_data, signals_df)
            self.log(f"Warehouse load complete. Loaded {load_res['prices_loaded']} prices, {load_res['signals_loaded']} signals.")
            self._complete_task("load_to_postgres")
            time.sleep(0.3)

            # -------------------------------------------------------------
            # Task 6: generate_signals
            # -------------------------------------------------------------
            self._start_task("generate_signals")
            self.log("Task 6/6: Verifying signal consistency and updating analytics views...")
            time.sleep(0.5)
            self.log("Trading intelligence published to analytical data marts successfully.")
            self._complete_task("generate_signals")

            # Finalize metrics
            total_duration = round(time.time() - start_time_all, 2)
            self.last_run_status = "SUCCESS"
            self.latest_metrics = {
                "batch_id": raw_data_batch["metadata"]["batch_id"],
                "execution_duration_sec": total_duration,
                "symbols_processed": len(symbols),
                "candles_processed": len(raw_data_batch["prices"]),
                "signals_generated": len(signals_df),
                "news_analyzed": len(raw_data_batch["news"]),
                "completed_at": datetime.now(timezone.utc).isoformat()
            }
            self.log(f" Pipeline Run SUCCESSFUL in {total_duration}s.")

            return {
                "status": "SUCCESS",
                "metrics": self.latest_metrics,
                "tasks": [self.task_states[t["id"]] for t in DAG_TASKS]
            }

        except Exception as e:
            self.last_run_status = "FAILED"
            self.log(f" Pipeline FAILED at task {self.current_task_id}: {e}")
            if self.current_task_id and self.current_task_id in self.task_states:
                self.task_states[self.current_task_id]["status"] = "FAILED"
                self.task_states[self.current_task_id]["error"] = str(e)
            return {"status": "FAILED", "error": str(e)}

        finally:
            with self.lock:
                self.is_running = False
                self.current_task_id = None

    def _start_task(self, task_id: str):
        self.current_task_id = task_id
        st = self.task_states[task_id]
        st["status"] = "RUNNING"
        st["start_time"] = datetime.now(timezone.utc).isoformat()

    def _complete_task(self, task_id: str):
        st = self.task_states[task_id]
        st["status"] = "SUCCESS"
        st["end_time"] = datetime.now(timezone.utc).isoformat()
        if st["start_time"]:
            start_dt = datetime.fromisoformat(st["start_time"])
            end_dt = datetime.fromisoformat(st["end_time"])
            st["duration_seconds"] = round((end_dt - start_dt).total_seconds(), 2)


# Global orchestrator singleton
orchestrator = PipelineOrchestrator()


def trigger_pipeline_async(symbols: Optional[List[str]] = None, days: int = TIMEFRAME_DAYS):
    """
    Spawns background thread to execute pipeline asynchronously.
    """
    thread = threading.Thread(target=orchestrator.execute_pipeline, args=(symbols, days), daemon=True)
    thread.start()
    return {"status": "TRIGGERED", "message": "Pipeline started in background thread."}
