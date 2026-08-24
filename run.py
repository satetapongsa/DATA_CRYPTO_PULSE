"""
=============================================================================
CRYPTO INTELLIGENCE PLATFORM - APPLICATION LAUNCHER
=============================================================================
Launches the FastAPI Backend & Live Interactive Web Dashboard.
Usage:
    python run.py
=============================================================================
"""

import sys
import os
import uvicorn
from pathlib import Path

# Add project root to python path
ROOT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT_DIR))

if __name__ == "__main__":
    print("\n" + "=" * 75)
    print(" [CRYPTO-INTELLIGENCE] MARKET TRENDS & TRADING SIGNAL PLATFORM")
    print("=" * 75)
    print(" - Architecture: Data Lakehouse (MongoDB) + Star Schema Warehouse (PostgreSQL)")
    print(" - Pipeline Orchestrator: Apache Airflow DAG (Simulation & Production)")
    print(" - Feature Engineering: SMA-7, SMA-25, EMA-12, EMA-26, RSI-14, MACD & NLP Sentiment")
    print(" - Serving Layer: FastAPI REST Endpoints + Glassmorphic Cyberpunk Dashboard")
    print("=" * 75)
    print(" -> Web Dashboard:        http://localhost:8000")
    print(" -> FastAPI Swagger Docs: http://localhost:8000/docs")
    print(" -> System Health:        http://localhost:8000/health")
    print("=" * 75 + "\n")

    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=False)
