"""
Pipeline & Airflow DAG Orchestration API Router
Endpoints to monitor DAG status, task nodes, execution logs, and trigger manual ETL runs.
"""

from fastapi import APIRouter, BackgroundTasks, Query
from typing import Dict, Any
from backend.schemas import PipelineStatusResponse
from pipeline.orchestrator import orchestrator, trigger_pipeline_async

router = APIRouter(prefix="/api/v1/pipeline", tags=["Pipeline Orchestrator"])


@router.get("/status", response_model=PipelineStatusResponse)
def get_pipeline_status():
    """
    Returns real-time DAG execution state, task nodes progress, duration metrics, and recent logs.
    """
    return orchestrator.get_dag_status()


@router.post("/trigger")
def trigger_pipeline(
    days: int = Query(30, description="Historical lookback days for data generation")
):
    """
    Triggers immediate execution of the Airflow DAG pipeline asynchronously.
    """
    if orchestrator.is_running:
        return {
            "status": "BUSY",
            "message": "Pipeline is already executing. Please wait for current run to finish."
        }

    trigger_pipeline_async(days=days)
    return {
        "status": "TRIGGERED",
        "message": "Crypto Intelligence Pipeline triggered successfully.",
        "dag_id": "crypto_market_intelligence_pipeline"
    }


@router.get("/logs")
def get_pipeline_logs():
    """
    Retrieves full execution audit logs from the orchestrator.
    """
    return {
        "total_lines": len(orchestrator.execution_logs),
        "logs": orchestrator.execution_logs
    }
