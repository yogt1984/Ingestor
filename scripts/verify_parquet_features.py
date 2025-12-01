#!/usr/bin/env python3
"""
Verification script to ensure all FeaturesSnapshot fields are present in parquet files.
This script checks that all features, especially entropy features, are being persisted.
"""

import polars as pl
import sys
from pathlib import Path
from typing import List, Set

# All expected columns from FeaturesSnapshot
EXPECTED_COLUMNS = {
    # Basic fields
    "timestamp",
    # Orderbook features
    "best_bid", "best_ask", "mid_price", "microprice", "spread", "imbalance",
    "top_bids", "top_asks",
    "pwi_1", "pwi_5", "pwi_25", "pwi_50",
    "bid_slope", "ask_slope",
    "volume_imbalance_top5",
    "bid_depth_ratio", "ask_depth_ratio",
    "bid_volume_001", "ask_volume_001",
    "bid_avg_distance", "ask_avg_distance",
    # Tradeslog features
    "last_trade_price", "trade_imbalance", "vwap_total", "price_change", "avg_trade_size",
    "signed_count_momentum", "trade_rate_10s",
    "order_flow_imbalance", "order_flow_pressure", "order_flow_significance",
    "vwap_10", "vwap_50", "vwap_100", "vwap_1000",
    "aggr_ratio_10", "aggr_ratio_50", "aggr_ratio_100", "aggr_ratio_1000",
    # Illiquidity metrics
    "roll_spread", "amihuds_lambda", "kyles_lambda", "hasbroucks_lambda", "vpin",
    # Entropy metrics - tick entropy (CRITICAL)
    "tick_entropy_1s", "tick_entropy_5s", "tick_entropy_10s", "tick_entropy_15s",
    "tick_entropy_30s", "tick_entropy_1m", "tick_entropy_15m",
    # Entropy metrics - volume tick entropy (CRITICAL)
    "volume_tick_entropy_1s", "volume_tick_entropy_5s", "volume_tick_entropy_10s",
    "volume_tick_entropy_15s", "volume_tick_entropy_30s", "volume_tick_entropy_1m",
    "volume_tick_entropy_15m",
    # Complex vector fields
    "volume_vector", "pwi_vector",
}

ENTROPY_COLUMNS = {
    "tick_entropy_1s", "tick_entropy_5s", "tick_entropy_10s", "tick_entropy_15s",
    "tick_entropy_30s", "tick_entropy_1m", "tick_entropy_15m",
    "volume_tick_entropy_1s", "volume_tick_entropy_5s", "volume_tick_entropy_10s",
    "volume_tick_entropy_15s", "volume_tick_entropy_30s", "volume_tick_entropy_1m",
    "volume_tick_entropy_15m",
}

ILLIQUIDITY_COLUMNS = {
    "roll_spread", "amihuds_lambda", "kyles_lambda", "hasbroucks_lambda", "vpin",
}


def verify_parquet_file(filepath: Path) -> tuple[bool, List[str], Set[str]]:
    """
    Verify that a parquet file contains all expected columns.
    
    Returns:
        (is_valid, missing_columns, extra_columns)
    """
    try:
        df = pl.read_parquet(filepath)
        actual_columns = set(df.columns)
        
        missing = sorted(EXPECTED_COLUMNS - actual_columns)
        extra = sorted(actual_columns - EXPECTED_COLUMNS)
        
        is_valid = len(missing) == 0
        
        return is_valid, missing, extra
    except Exception as e:
        print(f"ERROR: Failed to read {filepath}: {e}", file=sys.stderr)
        return False, [], set()


def check_entropy_data(df: pl.DataFrame) -> dict:
    """Check if entropy columns have non-null data."""
    results = {}
    for col in ENTROPY_COLUMNS:
        if col in df.columns:
            null_count = df[col].null_count()
            total_count = len(df)
            results[col] = {
                "null_count": null_count,
                "total_count": total_count,
                "has_data": null_count < total_count,
            }
        else:
            results[col] = {"error": "Column missing"}
    return results


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Verify that parquet files contain all FeaturesSnapshot fields"
    )
    parser.add_argument(
        "files",
        nargs="+",
        type=Path,
        help="Parquet file(s) to verify"
    )
    parser.add_argument(
        "--check-data",
        action="store_true",
        help="Also check if entropy columns contain non-null data"
    )
    
    args = parser.parse_args()
    
    all_valid = True
    total_files = 0
    valid_files = 0
    
    for filepath in args.files:
        if not filepath.exists():
            print(f"ERROR: File not found: {filepath}", file=sys.stderr)
            all_valid = False
            continue
        
        total_files += 1
        is_valid, missing, extra = verify_parquet_file(filepath)
        
        if is_valid:
            valid_files += 1
            print(f"✓ {filepath.name}: All columns present")
            if extra:
                print(f"  Note: Extra columns found: {', '.join(extra)}")
        else:
            all_valid = False
            print(f"✗ {filepath.name}: MISSING COLUMNS")
            if missing:
                print(f"  Missing: {', '.join(missing)}")
            
            # Highlight missing entropy columns
            missing_entropy = [col for col in missing if col in ENTROPY_COLUMNS]
            if missing_entropy:
                print(f"  ⚠️  CRITICAL: Missing entropy columns: {', '.join(missing_entropy)}")
            
            # Highlight missing illiquidity columns
            missing_illiquidity = [col for col in missing if col in ILLIQUIDITY_COLUMNS]
            if missing_illiquidity:
                print(f"  ⚠️  Missing illiquidity columns: {', '.join(missing_illiquidity)}")
        
        # Check data if requested
        if args.check_data and is_valid:
            try:
                df = pl.read_parquet(filepath)
                entropy_data = check_entropy_data(df)
                
                print(f"  Entropy data check:")
                for col, info in entropy_data.items():
                    if "error" in info:
                        print(f"    {col}: {info['error']}")
                    else:
                        status = "✓" if info["has_data"] else "✗ (all null)"
                        print(f"    {status} {col}: {info['null_count']}/{info['total_count']} null")
            except Exception as e:
                print(f"  ERROR checking data: {e}", file=sys.stderr)
        
        print()
    
    # Summary
    print(f"Summary: {valid_files}/{total_files} files valid")
    
    if not all_valid:
        print("\n⚠️  Some files are missing required columns!")
        sys.exit(1)
    else:
        print("\n✓ All files contain all required columns!")
        sys.exit(0)


if __name__ == "__main__":
    main()

