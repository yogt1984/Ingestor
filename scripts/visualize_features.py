#!/usr/bin/env python3
"""
Quick explorations for ingestor feature batches.

Usage:
    python scripts/visualize_features.py --input data/features/features_*.parquet --limit 1000

The script will
  * read one or more parquet files into a Pandas DataFrame
  * optionally filter/limit rows
  * produce a few example plots (Matplotlib) and show them interactively

Dependencies:
    pandas, polars, matplotlib
"""
from __future__ import annotations

import argparse
import glob
import sys
from pathlib import Path
from typing import Iterable, List

import matplotlib.pyplot as plt
import pandas as pd
import polars as pl


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Visualize ingestor parquet feature batches.")
    parser.add_argument(
        "--input",
        required=True,
        nargs="+",
        help="Parquet file glob(s) to load, e.g. data/features/features_*.parquet",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional row limit after concatenation.",
    )
    parser.add_argument(
        "--show-columns",
        action="store_true",
        help="Print column names and exit.",
    )
    return parser.parse_args()


def expand_inputs(patterns: Iterable[str]) -> List[Path]:
    files: List[Path] = []
    for pattern in patterns:
        matches = sorted(glob.glob(pattern))
        files.extend(Path(m) for m in matches)
    if not files:
        raise FileNotFoundError(f"No parquet files matched patterns: {patterns}")
    return files


def load_parquet(files: List[Path], limit: int | None) -> pd.DataFrame:
    frames = []
    for file in files:
        table = pl.read_parquet(file)
        frames.append(table.to_pandas())
    df = pd.concat(frames, ignore_index=True)
    if limit is not None:
        df = df.head(limit)
    return df


def main() -> None:
    args = parse_args()

    try:
        files = expand_inputs(args.input)
    except FileNotFoundError as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)

    df = load_parquet(files, args.limit)

    if args.show_columns:
        print("Columns:")
        for col in df.columns:
            print(f"  {col}")
        return

    if df.empty:
        print("No rows loaded; nothing to visualize.")
        return

    # Ensure timestamp is datetime for plotting
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # Example plots: mid_price & spread over time if present.
    if {"timestamp", "mid_price"}.issubset(df.columns):
        plt.figure(figsize=(12, 5))
        plt.plot(df["timestamp"], df["mid_price"], label="Mid Price")
        if "spread" in df.columns:
            plt.plot(df["timestamp"], df["spread"], label="Spread")
        plt.title("Mid Price / Spread Over Time")
        plt.xlabel("Timestamp")
        plt.ylabel("Price")
        plt.legend()
        plt.tight_layout()
    else:
        print("Mid price or timestamp column missing; skipping price plot.")

    # Example histogram: order_flow_pressure if available.
    if "order_flow_pressure" in df.columns:
        plt.figure(figsize=(8, 4))
        df["order_flow_pressure"].dropna().plot(kind="hist", bins=50)
        plt.title("Order Flow Pressure Distribution")
        plt.xlabel("Pressure")
        plt.tight_layout()
    else:
        print("order_flow_pressure column missing; skipping histogram.")

    plt.show()


if __name__ == "__main__":
    main()

