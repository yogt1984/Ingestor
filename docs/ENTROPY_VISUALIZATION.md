# Entropy Feature Visualization Guide

## Overview

This document describes the entropy visualization toolkit for analyzing market regime features extracted from the Ingestor pipeline. The visualizations help verify feature correctness and build intuition about entropy-based regime detection.

## Quick Start

```bash
# Activate environment
source .venv/bin/activate

# Single file analysis
python scripts/visualize_entropy.py -f data/features/features_20260210_152432_710.parquet

# All files combined
python scripts/visualize_entropy.py --all

# Custom output directory
python scripts/visualize_entropy.py --all -o ./analysis/my_session
```

## Dependencies

```bash
pip install -r scripts/requirements-viz.txt
```

Required: `pandas`, `matplotlib`, `seaborn`, `pyarrow`, `numpy`

---

## Entropy Features Explained

### Tick Entropy
Measures **directional randomness** of price movements within a time window.

| Value | Interpretation | Trading Implication |
|-------|----------------|---------------------|
| `< 0.3` | **Trending** - Consistent direction | Momentum strategies favorable |
| `0.3-0.6` | **Transitional** - Mixed signals | Reduce position sizing |
| `> 0.6` | **Random** - No directional bias | Mean-reversion / market-making favorable |

### Volume-Weighted Tick Entropy
Same concept, but weighted by trade volume. Captures whether **large trades** follow trends.

### Timeframes
- **1s, 5s, 10s**: Scalping signals, microstructure noise
- **15s, 30s**: Short-term regime detection
- **1m**: Primary trading signal timeframe
- **15m**: Macro regime context

---

## Generated Visualizations

### Core Analysis (Start Here)

| File | Purpose |
|------|---------|
| `price_entropy_connection.png` | **Primary insight**: Direct price-entropy relationship |
| `entropy_clusters.png` | Regime clustering and state transitions |
| `entropy_dashboard.png` | Comprehensive single-page overview |

### Supporting Analysis

| File | Purpose |
|------|---------|
| `entropy_with_price.png` | Price chart with entropy overlay |
| `entropy_time_series.png` | All timeframes over time |
| `entropy_heatmap.png` | Multi-timeframe regime view |
| `entropy_distribution.png` | Statistical distributions |
| `entropy_correlation.png` | Cross-feature correlations |
| `entropy_vs_volatility.png` | Entropy-volatility relationship |
| `entropy_cross_timeframe.png` | Short vs long timeframe analysis |
| `phase2_features.png` | Effective/realized spread analysis |
| `entropy_statistics.csv` | Numerical summary |

---

## Key Visualizations Explained

### 1. Price-Entropy Connection (`price_entropy_connection.png`)

**Row 1: Price with Entropy Background**
- Red background = Low entropy (trending)
- Yellow = Medium entropy
- Green = High entropy (random)

**Row 2: Return Distributions**
- Left: Price return histograms by entropy bin
- Center: Scatter of |return| vs entropy with trend line
- Right: Direction change rate by entropy

**Row 3: Sequential Analysis**
- Entropy vs future price moves
- Entropy momentum vs price momentum

**Row 4: Clustering**
- 2D entropy space colored by price movement
- Multi-timeframe regime quadrants

### 2. Entropy Clusters (`entropy_clusters.png`)

**Scatter Matrix**: Pairwise entropy relationships colored by returns
**Hexbin/KDE**: Density of entropy states
**Regime Evolution**: Time series of entropy states
**Duration Histogram**: How long each regime persists

### 3. Dashboard (`entropy_dashboard.png`)

Consolidated view with:
- Price + entropy timeline
- Heatmap across all timeframes
- Key scatter plots
- Summary statistics

---

## Interpretation Guide

### What Low Entropy Means
```
Entropy < 0.3 (Trending)
├── Price moving consistently in one direction
├── Order flow likely imbalanced
├── Higher adverse selection risk for market makers
└── Momentum strategies have edge
```

### What High Entropy Means
```
Entropy > 0.6 (Random)
├── Price direction is unpredictable
├── Mean-reversion more likely
├── Safer for market-making (less adverse selection)
└── Spread-capture strategies favorable
```

### Cross-Timeframe Signals

| Short (5s) | Long (1m) | Interpretation |
|------------|-----------|----------------|
| Low | Low | Strong trend, momentum play |
| Low | High | Microstructure trend in random macro |
| High | Low | Noise in trending macro |
| High | High | Pure randomness, market-make |

---

## Practical Usage

### Verifying Feature Extraction

1. Check `entropy_statistics.csv`:
   - Mean should be 0.4-0.7 (not stuck at 0 or 1)
   - Std should be > 0.05 (has variance)
   - Null % should match warmup expectations

2. Check `entropy_distribution.png`:
   - Should not be bimodal at 0 and 1
   - Longer timeframes should be smoother

3. Check `entropy_heatmap.png`:
   - Should show temporal structure
   - Vertical bands = regime changes

### Building Trading Intuition

1. **Start with** `price_entropy_connection.png`:
   - Does low entropy correlate with larger moves?
   - Does high entropy show smaller, more random returns?

2. **Check regime persistence** in `entropy_clusters.png`:
   - How long do regimes last?
   - Are transitions gradual or sudden?

3. **Validate with price** in `entropy_with_price.png`:
   - Do entropy drops precede trends?
   - Do entropy spikes mark reversals?

---

## Entropy Gating Parameters

The market maker uses entropy for quote control:

```json
{
  "min_entropy_for_quoting": 0.3,
  "flow_skew_weight": 0.5
}
```

- `min_entropy_for_quoting`: Stop quoting below this threshold (avoid adverse selection)
- `flow_skew_weight`: How much to lean with order flow

Use visualizations to calibrate these thresholds based on your data.

---

## Command Reference

```bash
# Basic usage
python scripts/visualize_entropy.py --file <path>

# Options
--file, -f     Specific Parquet file
--all, -a      Load all files in data directory
--output, -o   Output directory (default: ./output/entropy_viz)
--data-dir, -d Data directory (default: ./data/features)
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Empty plots | Check Parquet has entropy columns |
| All entropy = 0 | Warmup period not complete |
| All entropy = 0.693 | Max entropy, need more tick variation |
| Missing price overlay | Verify `mid_price` column exists |
| KDE fails | Insufficient data points (need >100) |
