# Data Visualization Guide

## Quick Start

```bash
# 1. Activate environment
source .venv/bin/activate

# 2. Run visualization
python scripts/visualize_entropy.py -f data/features/<your_file>.parquet

# 3. View results
ls output/entropy_viz/
```

---

## Commands

| Command | Description |
|---------|-------------|
| `python scripts/visualize_entropy.py -f <file>` | Analyze single Parquet file |
| `python scripts/visualize_entropy.py --all` | Analyze all files combined |
| `python scripts/visualize_entropy.py -o <dir>` | Custom output directory |
| `python scripts/visualize_entropy.py -d <dir>` | Custom data directory |

### Examples

```bash
# Single file analysis
python scripts/visualize_entropy.py -f data/features/features_20260210_152432_710.parquet

# All data
python scripts/visualize_entropy.py --all

# Custom output
python scripts/visualize_entropy.py --all -o ./analysis/session_001
```

---

## Output Files

Default output: `output/entropy_viz/`

### Start Here (Core Analysis)

| File | Purpose |
|------|---------|
| `price_entropy_connection.png` | **Primary** - Price action vs entropy relationship |
| `entropy_clusters.png` | Regime clustering and state transitions |
| `entropy_dashboard.png` | Single-page comprehensive overview |

### Supporting Analysis

| File | Purpose |
|------|---------|
| `entropy_with_price.png` | Price chart with entropy overlay |
| `entropy_time_series.png` | All timeframes over time |
| `entropy_heatmap.png` | Multi-timeframe regime view |
| `entropy_distribution.png` | Statistical distributions |
| `entropy_correlation.png` | Cross-feature correlations |
| `entropy_vs_volatility.png` | Entropy-volatility relationship |
| `entropy_cross_timeframe.png` | Short vs long timeframe |
| `entropy_regime_analysis.png` | Analysis by detected regime |
| `phase2_features.png` | Effective/realized spread |
| `entropy_statistics.csv` | Numerical summary (CSV) |

---

## Viewing Results

```bash
# List files
ls -la output/entropy_viz/

# Open folder (Linux)
xdg-open output/entropy_viz/

# Open specific image
xdg-open output/entropy_viz/price_entropy_connection.png

# View statistics
cat output/entropy_viz/entropy_statistics.csv
```

---

## Setup (First Time)

```bash
# Create virtual environment (if not exists)
python3 -m venv .venv

# Activate
source .venv/bin/activate

# Install dependencies
pip install -r scripts/requirements-viz.txt
```

---

## Interpretation Quick Reference

### Entropy Values

| Range | State | Meaning |
|-------|-------|---------|
| `< 0.3` | Trending | Consistent price direction |
| `0.3 - 0.6` | Transitional | Mixed signals |
| `> 0.6` | Random | No directional bias |

### Timeframes

| Timeframe | Use Case |
|-----------|----------|
| 1s, 5s, 10s | Microstructure, scalping |
| 15s, 30s | Short-term regime |
| 1m | Primary trading signal |
| 15m | Macro context |

### Color Coding (in visualizations)

- **Red** = Low entropy (trending market)
- **Yellow** = Medium entropy (transitional)
- **Green** = High entropy (random/mean-reverting)

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `ModuleNotFoundError` | Run `source .venv/bin/activate` first |
| Empty plots | Verify Parquet file has entropy columns |
| No files found | Check `--data-dir` path |
| Permission denied | Run `chmod +x scripts/visualize_entropy.py` |

---

## Related Documentation

- [ENTROPY_VISUALIZATION.md](./ENTROPY_VISUALIZATION.md) - Detailed entropy feature interpretation
- [README.md](../README.md) - Project overview
