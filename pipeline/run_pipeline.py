"""
Standalone Pipeline CLI Runner
Usage:
    python -m pipeline.run_pipeline [--symbols BTC,ETH,SOL] [--days 30]
"""

import argparse
import sys
import json
from pipeline.orchestrator import orchestrator
from pipeline.config import CRYPTO_SYMBOLS, TIMEFRAME_DAYS


def main():
    parser = argparse.ArgumentParser(description="Crypto Market Intelligence Pipeline Runner")
    parser.add_argument("--symbols", type=str, default=",".join(CRYPTO_SYMBOLS), help="Comma-separated symbols")
    parser.add_argument("--days", type=int, default=TIMEFRAME_DAYS, help="Historical lookback days")
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    print(f"============================================================")
    print(f" Starting Crypto Intelligence ETL Pipeline Execution")
    print(f" Symbols: {symbols} | Lookback Days: {args.days}")
    print(f"============================================================")

    result = orchestrator.execute_pipeline(symbols=symbols, days=args.days)
    print("\n Execution Result:")
    print(json.dumps(result, indent=2))

    if result["status"] == "SUCCESS":
        print("\n[SUCCESS] Pipeline completed successfully!")
        sys.exit(0)
    else:
        print(f"\n[FAILED] Pipeline failed: {result.get('error')}")
        sys.exit(1)


if __name__ == "__main__":
    main()
