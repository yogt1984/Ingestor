#!/usr/bin/env python3
"""
Entropy Feature Visualization Script

This script reads Parquet feature files and visualizes entropy metrics
to help verify feature extraction correctness and develop intuition
about entropy-based regime detection.

Usage:
    python scripts/visualize_entropy.py [--file PATH] [--all] [--output DIR]

Examples:
    python scripts/visualize_entropy.py --all
    python scripts/visualize_entropy.py --file data/features/features_20260210_152432_710.parquet
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.gridspec import GridSpec
import seaborn as sns

# Set style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")


def load_parquet_files(data_dir: Path, file_path: str = None) -> pd.DataFrame:
    """Load Parquet files into a combined DataFrame."""
    if file_path:
        files = [Path(file_path)]
    else:
        files = sorted(data_dir.glob("*.parquet"))

    if not files:
        print(f"No Parquet files found in {data_dir}")
        sys.exit(1)

    print(f"Loading {len(files)} Parquet file(s)...")

    dfs = []
    for f in files:
        try:
            df = pd.read_parquet(f)
            df['source_file'] = f.name
            dfs.append(df)
            print(f"  Loaded {f.name}: {len(df)} rows")
        except Exception as e:
            print(f"  Error loading {f.name}: {e}")

    if not dfs:
        print("No data loaded successfully")
        sys.exit(1)

    combined = pd.concat(dfs, ignore_index=True)

    # Parse timestamp
    if 'timestamp' in combined.columns:
        combined['timestamp'] = pd.to_datetime(combined['timestamp'])
        combined = combined.sort_values('timestamp').reset_index(drop=True)

    print(f"\nTotal rows: {len(combined)}")
    print(f"Time range: {combined['timestamp'].min()} to {combined['timestamp'].max()}")

    return combined


def get_entropy_columns(df: pd.DataFrame) -> dict:
    """Get entropy column groups."""
    tick_entropy_cols = [c for c in df.columns if c.startswith('tick_entropy_')]
    volume_entropy_cols = [c for c in df.columns if c.startswith('volume_tick_entropy_')]

    # Order by timeframe
    timeframe_order = ['1s', '5s', '10s', '15s', '30s', '1m', '15m']

    def sort_key(col):
        for i, tf in enumerate(timeframe_order):
            if col.endswith(f'_{tf}'):
                return i
        return 999

    tick_entropy_cols = sorted(tick_entropy_cols, key=sort_key)
    volume_entropy_cols = sorted(volume_entropy_cols, key=sort_key)

    return {
        'tick_entropy': tick_entropy_cols,
        'volume_tick_entropy': volume_entropy_cols
    }


def plot_entropy_time_series(df: pd.DataFrame, output_dir: Path):
    """Plot entropy values over time for all timeframes."""
    entropy_cols = get_entropy_columns(df)

    fig, axes = plt.subplots(2, 1, figsize=(16, 10), sharex=True)

    # Plot tick entropy
    ax1 = axes[0]
    for col in entropy_cols['tick_entropy']:
        timeframe = col.replace('tick_entropy_', '')
        valid_data = df[['timestamp', col]].dropna()
        if len(valid_data) > 0:
            ax1.plot(valid_data['timestamp'], valid_data[col],
                    label=timeframe, alpha=0.7, linewidth=0.8)

    ax1.set_ylabel('Tick Entropy')
    ax1.set_title('Tick Entropy Across Timeframes')
    ax1.legend(loc='upper right', ncol=7, fontsize=8)
    ax1.set_ylim(0, 1.1)
    ax1.axhline(y=0.5, color='red', linestyle='--', alpha=0.5, label='Mid entropy')

    # Plot volume tick entropy
    ax2 = axes[1]
    for col in entropy_cols['volume_tick_entropy']:
        timeframe = col.replace('volume_tick_entropy_', '')
        valid_data = df[['timestamp', col]].dropna()
        if len(valid_data) > 0:
            ax2.plot(valid_data['timestamp'], valid_data[col],
                    label=timeframe, alpha=0.7, linewidth=0.8)

    ax2.set_ylabel('Volume Tick Entropy')
    ax2.set_xlabel('Time')
    ax2.set_title('Volume-Weighted Tick Entropy Across Timeframes')
    ax2.legend(loc='upper right', ncol=7, fontsize=8)
    ax2.set_ylim(0, 1.1)
    ax2.axhline(y=0.5, color='red', linestyle='--', alpha=0.5)

    # Format x-axis
    for ax in axes:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())

    plt.tight_layout()
    plt.savefig(output_dir / 'entropy_time_series.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("  Saved: entropy_time_series.png")


def plot_entropy_with_price(df: pd.DataFrame, output_dir: Path):
    """Plot entropy overlaid with price to see relationship."""
    fig, axes = plt.subplots(3, 1, figsize=(16, 12), sharex=True)

    # Price subplot
    ax1 = axes[0]
    valid_price = df[['timestamp', 'mid_price']].dropna()
    if len(valid_price) > 0:
        ax1.plot(valid_price['timestamp'], valid_price['mid_price'],
                color='blue', linewidth=0.8, label='Mid Price')
    ax1.set_ylabel('Price')
    ax1.set_title('Mid Price')
    ax1.legend(loc='upper right')

    # Short timeframe entropy (1s, 5s, 10s) - for scalping signals
    ax2 = axes[1]
    short_tf_cols = ['tick_entropy_1s', 'tick_entropy_5s', 'tick_entropy_10s']
    colors = ['#e41a1c', '#377eb8', '#4daf4a']
    for col, color in zip(short_tf_cols, colors):
        if col in df.columns:
            valid_data = df[['timestamp', col]].dropna()
            if len(valid_data) > 0:
                ax2.plot(valid_data['timestamp'], valid_data[col],
                        label=col.replace('tick_entropy_', ''),
                        alpha=0.7, linewidth=0.8, color=color)

    ax2.set_ylabel('Entropy')
    ax2.set_title('Short Timeframe Entropy (Scalping Signals)')
    ax2.legend(loc='upper right', ncol=3)
    ax2.set_ylim(0, 1.1)
    ax2.axhline(y=0.3, color='red', linestyle='--', alpha=0.5, linewidth=0.5)
    ax2.axhline(y=0.7, color='green', linestyle='--', alpha=0.5, linewidth=0.5)
    ax2.fill_between(df['timestamp'], 0, 0.3, alpha=0.1, color='red', label='Low entropy zone')
    ax2.fill_between(df['timestamp'], 0.7, 1, alpha=0.1, color='green', label='High entropy zone')

    # Long timeframe entropy (1m, 15m) - for regime detection
    ax3 = axes[2]
    long_tf_cols = ['tick_entropy_30s', 'tick_entropy_1m', 'tick_entropy_15m']
    colors = ['#984ea3', '#ff7f00', '#a65628']
    for col, color in zip(long_tf_cols, colors):
        if col in df.columns:
            valid_data = df[['timestamp', col]].dropna()
            if len(valid_data) > 0:
                ax3.plot(valid_data['timestamp'], valid_data[col],
                        label=col.replace('tick_entropy_', ''),
                        alpha=0.7, linewidth=0.8, color=color)

    ax3.set_ylabel('Entropy')
    ax3.set_xlabel('Time')
    ax3.set_title('Long Timeframe Entropy (Regime Detection)')
    ax3.legend(loc='upper right', ncol=3)
    ax3.set_ylim(0, 1.1)
    ax3.axhline(y=0.3, color='red', linestyle='--', alpha=0.5, linewidth=0.5)
    ax3.axhline(y=0.7, color='green', linestyle='--', alpha=0.5, linewidth=0.5)

    for ax in axes:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

    plt.tight_layout()
    plt.savefig(output_dir / 'entropy_with_price.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("  Saved: entropy_with_price.png")


def plot_entropy_heatmap(df: pd.DataFrame, output_dir: Path):
    """Create a heatmap showing entropy values across timeframes."""
    entropy_cols = get_entropy_columns(df)
    all_cols = entropy_cols['tick_entropy'] + entropy_cols['volume_tick_entropy']

    # Sample data if too large for heatmap
    sample_size = min(1000, len(df))
    if len(df) > sample_size:
        indices = np.linspace(0, len(df)-1, sample_size, dtype=int)
        df_sample = df.iloc[indices].copy()
    else:
        df_sample = df.copy()

    # Create entropy matrix
    entropy_data = df_sample[all_cols].values.T

    # Create labels
    y_labels = [c.replace('tick_entropy_', 'TE ').replace('volume_tick_entropy_', 'VTE ')
                for c in all_cols]

    # Create time labels (every 50th point)
    x_tick_indices = np.linspace(0, len(df_sample)-1, 20, dtype=int)
    x_labels = [df_sample.iloc[i]['timestamp'].strftime('%H:%M:%S')
                for i in x_tick_indices]

    fig, ax = plt.subplots(figsize=(16, 8))

    # Handle NaN values
    entropy_data = np.nan_to_num(entropy_data, nan=0.5)

    im = ax.imshow(entropy_data, aspect='auto', cmap='RdYlGn', vmin=0, vmax=1)

    ax.set_yticks(range(len(y_labels)))
    ax.set_yticklabels(y_labels)
    ax.set_xticks(x_tick_indices)
    ax.set_xticklabels(x_labels, rotation=45, ha='right')

    ax.set_title('Entropy Heatmap Across All Timeframes\n(Red=Low/Trending, Green=High/Random)')
    ax.set_xlabel('Time')
    ax.set_ylabel('Entropy Metric')

    # Add colorbar
    cbar = plt.colorbar(im, ax=ax, label='Entropy Value')
    cbar.ax.axhline(y=0.5, color='black', linewidth=2)

    # Add separator between tick entropy and volume tick entropy
    ax.axhline(y=len(entropy_cols['tick_entropy'])-0.5, color='white', linewidth=2)

    plt.tight_layout()
    plt.savefig(output_dir / 'entropy_heatmap.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("  Saved: entropy_heatmap.png")


def plot_entropy_distribution(df: pd.DataFrame, output_dir: Path):
    """Plot distribution of entropy values."""
    entropy_cols = get_entropy_columns(df)

    fig, axes = plt.subplots(2, 1, figsize=(14, 10))

    # Tick entropy distributions
    ax1 = axes[0]
    for col in entropy_cols['tick_entropy']:
        valid = df[col].dropna()
        if len(valid) > 0:
            timeframe = col.replace('tick_entropy_', '')
            ax1.hist(valid, bins=50, alpha=0.5, label=timeframe, density=True)

    ax1.set_xlabel('Entropy Value')
    ax1.set_ylabel('Density')
    ax1.set_title('Tick Entropy Distribution by Timeframe')
    ax1.legend(loc='upper right')
    ax1.axvline(x=0.5, color='red', linestyle='--', alpha=0.7, label='Mid entropy')
    ax1.set_xlim(0, 1)

    # Volume tick entropy distributions
    ax2 = axes[1]
    for col in entropy_cols['volume_tick_entropy']:
        valid = df[col].dropna()
        if len(valid) > 0:
            timeframe = col.replace('volume_tick_entropy_', '')
            ax2.hist(valid, bins=50, alpha=0.5, label=timeframe, density=True)

    ax2.set_xlabel('Entropy Value')
    ax2.set_ylabel('Density')
    ax2.set_title('Volume Tick Entropy Distribution by Timeframe')
    ax2.legend(loc='upper right')
    ax2.axvline(x=0.5, color='red', linestyle='--', alpha=0.7)
    ax2.set_xlim(0, 1)

    plt.tight_layout()
    plt.savefig(output_dir / 'entropy_distribution.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("  Saved: entropy_distribution.png")


def plot_entropy_correlation(df: pd.DataFrame, output_dir: Path):
    """Plot correlation matrix between entropy metrics."""
    entropy_cols = get_entropy_columns(df)
    all_cols = entropy_cols['tick_entropy'] + entropy_cols['volume_tick_entropy']

    # Add other relevant metrics for correlation
    extra_cols = ['spread', 'imbalance', 'order_flow_imbalance', 'vpin',
                  'realized_volatility_100', 'regime_confidence']
    extra_cols = [c for c in extra_cols if c in df.columns]

    corr_cols = all_cols + extra_cols

    # Compute correlation
    corr_data = df[corr_cols].corr()

    # Create labels
    labels = [c.replace('tick_entropy_', 'TE_').replace('volume_tick_entropy_', 'VTE_')
              .replace('_', '\n') for c in corr_cols]

    fig, ax = plt.subplots(figsize=(16, 14))

    mask = np.triu(np.ones_like(corr_data, dtype=bool), k=1)

    sns.heatmap(corr_data, mask=mask, annot=True, fmt='.2f',
                cmap='RdBu_r', center=0, square=True, ax=ax,
                xticklabels=labels, yticklabels=labels,
                annot_kws={'size': 7})

    ax.set_title('Entropy Correlation Matrix\n(with other market metrics)')

    plt.tight_layout()
    plt.savefig(output_dir / 'entropy_correlation.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("  Saved: entropy_correlation.png")


def plot_entropy_vs_volatility(df: pd.DataFrame, output_dir: Path):
    """Scatter plot of entropy vs realized volatility."""
    if 'realized_volatility_100' not in df.columns:
        print("  Skipping entropy_vs_volatility: missing volatility column")
        return

    fig, axes = plt.subplots(2, 3, figsize=(15, 10))
    axes = axes.flatten()

    timeframes = ['1s', '5s', '10s', '30s', '1m', '15m']

    for idx, tf in enumerate(timeframes):
        ax = axes[idx]
        col = f'tick_entropy_{tf}'

        if col not in df.columns:
            continue

        valid = df[['realized_volatility_100', col]].dropna()
        if len(valid) > 100:
            # Sample for scatter plot
            sample = valid.sample(min(2000, len(valid)))

            ax.scatter(sample[col], sample['realized_volatility_100'],
                      alpha=0.3, s=5, c='blue')

            # Add trend line
            z = np.polyfit(sample[col], sample['realized_volatility_100'], 1)
            p = np.poly1d(z)
            x_line = np.linspace(sample[col].min(), sample[col].max(), 100)
            ax.plot(x_line, p(x_line), 'r-', linewidth=2, label='Trend')

            # Calculate correlation
            corr = sample[col].corr(sample['realized_volatility_100'])
            ax.set_title(f'{tf} Entropy (r={corr:.3f})')

        ax.set_xlabel('Tick Entropy')
        ax.set_ylabel('Realized Volatility')
        ax.set_xlim(0, 1)

    plt.suptitle('Tick Entropy vs Realized Volatility', fontsize=14)
    plt.tight_layout()
    plt.savefig(output_dir / 'entropy_vs_volatility.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("  Saved: entropy_vs_volatility.png")


def plot_entropy_regime_analysis(df: pd.DataFrame, output_dir: Path):
    """Analyze entropy in context of detected regimes."""
    if 'regime' not in df.columns:
        print("  Skipping regime analysis: missing regime column")
        return

    entropy_cols = get_entropy_columns(df)

    # Get unique regimes
    regimes = df['regime'].dropna().unique()
    if len(regimes) == 0:
        print("  Skipping regime analysis: no regime data")
        return

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    # Box plot of entropy by regime
    ax1 = axes[0]

    entropy_by_regime = []
    labels = []

    for regime in sorted(regimes):
        regime_data = df[df['regime'] == regime]
        if 'tick_entropy_1m' in df.columns:
            valid = regime_data['tick_entropy_1m'].dropna()
            if len(valid) > 0:
                entropy_by_regime.append(valid.values)
                labels.append(f"{regime}\n(n={len(valid)})")

    if entropy_by_regime:
        bp = ax1.boxplot(entropy_by_regime, tick_labels=labels, patch_artist=True)
        colors = plt.cm.Set2(np.linspace(0, 1, len(entropy_by_regime)))
        for patch, color in zip(bp['boxes'], colors):
            patch.set_facecolor(color)

        ax1.set_ylabel('Tick Entropy (1m)')
        ax1.set_title('Entropy Distribution by Regime')
        ax1.axhline(y=0.5, color='red', linestyle='--', alpha=0.5)

    # Time series with regime coloring
    ax2 = axes[1]

    if 'tick_entropy_1m' in df.columns:
        valid_data = df[['timestamp', 'tick_entropy_1m', 'regime']].dropna()

        # Color by regime
        regime_colors = {'trending': 'red', 'ranging': 'blue', 'volatile': 'orange',
                        'calm': 'green', 'unknown': 'gray'}

        for regime in valid_data['regime'].unique():
            regime_data = valid_data[valid_data['regime'] == regime]
            color = regime_colors.get(regime, 'gray')
            ax2.scatter(regime_data['timestamp'], regime_data['tick_entropy_1m'],
                       c=color, label=regime, alpha=0.5, s=3)

        ax2.set_xlabel('Time')
        ax2.set_ylabel('Tick Entropy (1m)')
        ax2.set_title('Entropy Colored by Regime')
        ax2.legend(loc='upper right', markerscale=3)
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

    plt.tight_layout()
    plt.savefig(output_dir / 'entropy_regime_analysis.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("  Saved: entropy_regime_analysis.png")


def plot_entropy_cross_timeframe(df: pd.DataFrame, output_dir: Path):
    """Analyze relationship between short and long timeframe entropy."""
    short_col = 'tick_entropy_5s'
    long_col = 'tick_entropy_1m'

    if short_col not in df.columns or long_col not in df.columns:
        print("  Skipping cross-timeframe: missing columns")
        return

    fig, axes = plt.subplots(1, 3, figsize=(15, 5))

    valid = df[[short_col, long_col, 'timestamp']].dropna()
    if len(valid) < 100:
        print("  Skipping cross-timeframe: insufficient data")
        return

    # Scatter plot
    ax1 = axes[0]
    sample = valid.sample(min(3000, len(valid)))
    ax1.scatter(sample[short_col], sample[long_col], alpha=0.3, s=5)
    ax1.plot([0, 1], [0, 1], 'r--', linewidth=1, label='y=x')
    ax1.set_xlabel('Short-term Entropy (5s)')
    ax1.set_ylabel('Long-term Entropy (1m)')
    ax1.set_title('Short vs Long Timeframe Entropy')
    ax1.set_xlim(0, 1)
    ax1.set_ylim(0, 1)
    ax1.legend()

    # Difference (momentum) over time
    ax2 = axes[1]
    valid['entropy_diff'] = valid[short_col] - valid[long_col]
    ax2.plot(valid['timestamp'], valid['entropy_diff'], linewidth=0.5, alpha=0.7)
    ax2.axhline(y=0, color='red', linestyle='--', alpha=0.5)
    ax2.fill_between(valid['timestamp'], 0, valid['entropy_diff'],
                    where=valid['entropy_diff'] > 0, alpha=0.3, color='green',
                    label='Short > Long')
    ax2.fill_between(valid['timestamp'], 0, valid['entropy_diff'],
                    where=valid['entropy_diff'] < 0, alpha=0.3, color='red',
                    label='Short < Long')
    ax2.set_xlabel('Time')
    ax2.set_ylabel('Entropy Difference (5s - 1m)')
    ax2.set_title('Entropy Momentum (Short - Long)')
    ax2.legend(loc='upper right')
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

    # 2D histogram
    ax3 = axes[2]
    h = ax3.hist2d(valid[short_col], valid[long_col], bins=30, cmap='YlOrRd')
    ax3.plot([0, 1], [0, 1], 'b--', linewidth=1)
    ax3.set_xlabel('Short-term Entropy (5s)')
    ax3.set_ylabel('Long-term Entropy (1m)')
    ax3.set_title('Entropy Joint Distribution')
    plt.colorbar(h[3], ax=ax3, label='Count')

    plt.tight_layout()
    plt.savefig(output_dir / 'entropy_cross_timeframe.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("  Saved: entropy_cross_timeframe.png")


def plot_entropy_statistics(df: pd.DataFrame, output_dir: Path):
    """Print and save summary statistics for entropy features."""
    entropy_cols = get_entropy_columns(df)
    all_cols = entropy_cols['tick_entropy'] + entropy_cols['volume_tick_entropy']

    stats = []
    for col in all_cols:
        valid = df[col].dropna()
        if len(valid) > 0:
            stats.append({
                'metric': col,
                'count': len(valid),
                'mean': valid.mean(),
                'std': valid.std(),
                'min': valid.min(),
                '25%': valid.quantile(0.25),
                '50%': valid.quantile(0.50),
                '75%': valid.quantile(0.75),
                'max': valid.max(),
                'null_pct': (len(df) - len(valid)) / len(df) * 100
            })

    stats_df = pd.DataFrame(stats)

    # Save to file
    stats_path = output_dir / 'entropy_statistics.csv'
    stats_df.to_csv(stats_path, index=False)

    # Print summary
    print("\n" + "="*80)
    print("ENTROPY FEATURE STATISTICS")
    print("="*80)
    print(stats_df.to_string(index=False))
    print("="*80)
    print(f"\nSaved to: {stats_path}")


def plot_phase2_features(df: pd.DataFrame, output_dir: Path):
    """Visualize Phase 2 features (effective spread, realized spread, inter-trade duration)."""
    has_phase2 = all(c in df.columns for c in ['effective_spread', 'realized_spread'])

    if not has_phase2:
        print("  Skipping Phase 2 features: columns not present")
        return

    fig = plt.figure(figsize=(16, 12))
    gs = GridSpec(3, 2, figure=fig)

    # Effective spread over time
    ax1 = fig.add_subplot(gs[0, 0])
    valid = df[['timestamp', 'effective_spread']].dropna()
    if len(valid) > 0:
        ax1.plot(valid['timestamp'], valid['effective_spread'], linewidth=0.5, alpha=0.7)
        ax1.set_ylabel('Effective Spread')
        ax1.set_title('Effective Spread Over Time')
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

    # Realized spread over time
    ax2 = fig.add_subplot(gs[0, 1])
    valid = df[['timestamp', 'realized_spread']].dropna()
    if len(valid) > 0:
        ax2.plot(valid['timestamp'], valid['realized_spread'], linewidth=0.5, alpha=0.7, color='orange')
        ax2.axhline(y=0, color='red', linestyle='--', alpha=0.5)
        ax2.set_ylabel('Realized Spread')
        ax2.set_title('Realized Spread Over Time (Maker P&L Proxy)')
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

    # Inter-trade duration
    ax3 = fig.add_subplot(gs[1, 0])
    if 'inter_trade_duration_mean_ms' in df.columns:
        valid = df[['timestamp', 'inter_trade_duration_mean_ms']].dropna()
        if len(valid) > 0:
            ax3.plot(valid['timestamp'], valid['inter_trade_duration_mean_ms'],
                    linewidth=0.5, alpha=0.7, color='green')
            ax3.set_ylabel('Duration (ms)')
            ax3.set_title('Mean Inter-Trade Duration')
            ax3.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

    # Inter-trade duration std
    ax4 = fig.add_subplot(gs[1, 1])
    if 'inter_trade_duration_std_ms' in df.columns:
        valid = df[['timestamp', 'inter_trade_duration_std_ms']].dropna()
        if len(valid) > 0:
            ax4.plot(valid['timestamp'], valid['inter_trade_duration_std_ms'],
                    linewidth=0.5, alpha=0.7, color='purple')
            ax4.set_ylabel('Duration Std (ms)')
            ax4.set_title('Inter-Trade Duration Variability')
            ax4.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

    # Effective vs Realized spread scatter
    ax5 = fig.add_subplot(gs[2, 0])
    valid = df[['effective_spread', 'realized_spread']].dropna()
    if len(valid) > 100:
        sample = valid.sample(min(2000, len(valid)))
        ax5.scatter(sample['effective_spread'], sample['realized_spread'], alpha=0.3, s=5)
        ax5.axhline(y=0, color='red', linestyle='--', alpha=0.5)
        ax5.set_xlabel('Effective Spread')
        ax5.set_ylabel('Realized Spread')
        ax5.set_title('Effective vs Realized Spread')

    # Correlation with entropy
    ax6 = fig.add_subplot(gs[2, 1])
    if 'tick_entropy_1m' in df.columns:
        valid = df[['tick_entropy_1m', 'realized_spread']].dropna()
        if len(valid) > 100:
            sample = valid.sample(min(2000, len(valid)))
            ax6.scatter(sample['tick_entropy_1m'], sample['realized_spread'], alpha=0.3, s=5, c='orange')
            ax6.axhline(y=0, color='red', linestyle='--', alpha=0.5)
            ax6.set_xlabel('Tick Entropy (1m)')
            ax6.set_ylabel('Realized Spread')
            ax6.set_title('Entropy vs Realized Spread\n(Higher entropy = less adverse selection?)')

    plt.tight_layout()
    plt.savefig(output_dir / 'phase2_features.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("  Saved: phase2_features.png")


def create_dashboard(df: pd.DataFrame, output_dir: Path):
    """Create a comprehensive dashboard view."""
    fig = plt.figure(figsize=(20, 16))
    gs = GridSpec(4, 3, figure=fig, hspace=0.3, wspace=0.25)

    # 1. Price with entropy overlay
    ax1 = fig.add_subplot(gs[0, :])
    if 'mid_price' in df.columns and 'tick_entropy_1m' in df.columns:
        ax1_twin = ax1.twinx()

        valid_price = df[['timestamp', 'mid_price']].dropna()
        valid_entropy = df[['timestamp', 'tick_entropy_1m']].dropna()

        if len(valid_price) > 0:
            ax1.plot(valid_price['timestamp'], valid_price['mid_price'],
                    'b-', linewidth=0.8, label='Mid Price')
        if len(valid_entropy) > 0:
            ax1_twin.plot(valid_entropy['timestamp'], valid_entropy['tick_entropy_1m'],
                         'r-', linewidth=0.8, alpha=0.7, label='Entropy (1m)')
            ax1_twin.axhline(y=0.5, color='red', linestyle='--', alpha=0.3)

        ax1.set_ylabel('Price', color='blue')
        ax1_twin.set_ylabel('Entropy', color='red')
        ax1_twin.set_ylim(0, 1)
        ax1.set_title('Price and Entropy (1m) Timeline')
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

        # Combine legends
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax1_twin.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right')

    # 2. Entropy heatmap (mini version)
    ax2 = fig.add_subplot(gs[1, :2])
    entropy_cols = get_entropy_columns(df)
    all_entropy = entropy_cols['tick_entropy'] + entropy_cols['volume_tick_entropy']

    sample_size = min(500, len(df))
    indices = np.linspace(0, len(df)-1, sample_size, dtype=int)
    df_sample = df.iloc[indices]

    entropy_data = df_sample[all_entropy].values.T
    entropy_data = np.nan_to_num(entropy_data, nan=0.5)

    im = ax2.imshow(entropy_data, aspect='auto', cmap='RdYlGn', vmin=0, vmax=1)
    ax2.set_yticks(range(len(all_entropy)))
    ax2.set_yticklabels([c.split('_')[-1] for c in all_entropy], fontsize=7)
    ax2.set_title('Entropy Heatmap')
    ax2.axhline(y=len(entropy_cols['tick_entropy'])-0.5, color='white', linewidth=2)
    plt.colorbar(im, ax=ax2, shrink=0.8)

    # 3. Entropy distribution
    ax3 = fig.add_subplot(gs[1, 2])
    if 'tick_entropy_1m' in df.columns:
        valid = df['tick_entropy_1m'].dropna()
        ax3.hist(valid, bins=40, edgecolor='black', alpha=0.7)
        ax3.axvline(x=0.5, color='red', linestyle='--', linewidth=2)
        ax3.axvline(x=valid.mean(), color='green', linestyle='-', linewidth=2, label=f'Mean: {valid.mean():.3f}')
        ax3.set_xlabel('Entropy (1m)')
        ax3.set_ylabel('Count')
        ax3.set_title('Entropy Distribution')
        ax3.legend()

    # 4. Short vs Long entropy
    ax4 = fig.add_subplot(gs[2, 0])
    if 'tick_entropy_5s' in df.columns and 'tick_entropy_1m' in df.columns:
        valid = df[['tick_entropy_5s', 'tick_entropy_1m']].dropna()
        if len(valid) > 100:
            sample = valid.sample(min(1500, len(valid)))
            ax4.scatter(sample['tick_entropy_5s'], sample['tick_entropy_1m'], alpha=0.2, s=3)
            ax4.plot([0, 1], [0, 1], 'r--')
            ax4.set_xlabel('Short (5s)')
            ax4.set_ylabel('Long (1m)')
            ax4.set_title('Cross-Timeframe')
            ax4.set_xlim(0, 1)
            ax4.set_ylim(0, 1)

    # 5. Tick vs Volume entropy
    ax5 = fig.add_subplot(gs[2, 1])
    if 'tick_entropy_1m' in df.columns and 'volume_tick_entropy_1m' in df.columns:
        valid = df[['tick_entropy_1m', 'volume_tick_entropy_1m']].dropna()
        if len(valid) > 100:
            sample = valid.sample(min(1500, len(valid)))
            ax5.scatter(sample['tick_entropy_1m'], sample['volume_tick_entropy_1m'], alpha=0.2, s=3)
            ax5.plot([0, 1], [0, 1], 'r--')
            ax5.set_xlabel('Tick Entropy (1m)')
            ax5.set_ylabel('Volume Tick Entropy (1m)')
            ax5.set_title('Tick vs Volume-Weighted')
            ax5.set_xlim(0, 1)
            ax5.set_ylim(0, 1)

    # 6. Entropy vs Volatility
    ax6 = fig.add_subplot(gs[2, 2])
    if 'tick_entropy_1m' in df.columns and 'realized_volatility_100' in df.columns:
        valid = df[['tick_entropy_1m', 'realized_volatility_100']].dropna()
        if len(valid) > 100:
            sample = valid.sample(min(1500, len(valid)))
            ax6.scatter(sample['tick_entropy_1m'], sample['realized_volatility_100'], alpha=0.2, s=3)
            ax6.set_xlabel('Tick Entropy (1m)')
            ax6.set_ylabel('Realized Volatility')
            ax6.set_title('Entropy vs Volatility')

    # 7. Key statistics text
    ax7 = fig.add_subplot(gs[3, :])
    ax7.axis('off')

    # Compute statistics
    stats_text = []
    stats_text.append(f"DATA SUMMARY")
    stats_text.append(f"{'─'*60}")
    stats_text.append(f"Total rows: {len(df):,}")
    stats_text.append(f"Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")
    stats_text.append(f"")

    stats_text.append(f"ENTROPY METRICS (1m timeframe)")
    stats_text.append(f"{'─'*60}")

    for col in ['tick_entropy_1m', 'volume_tick_entropy_1m']:
        if col in df.columns:
            valid = df[col].dropna()
            if len(valid) > 0:
                stats_text.append(f"{col}:")
                stats_text.append(f"  Mean: {valid.mean():.4f}  Std: {valid.std():.4f}  "
                                f"Min: {valid.min():.4f}  Max: {valid.max():.4f}")
                stats_text.append(f"  <0.3 (trending): {(valid < 0.3).sum()/len(valid)*100:.1f}%  "
                                f">0.7 (random): {(valid > 0.7).sum()/len(valid)*100:.1f}%")

    ax7.text(0.02, 0.95, '\n'.join(stats_text),
             transform=ax7.transAxes, fontsize=10, fontfamily='monospace',
             verticalalignment='top')

    plt.suptitle('Entropy Features Dashboard', fontsize=16, fontweight='bold', y=0.98)
    plt.savefig(output_dir / 'entropy_dashboard.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("  Saved: entropy_dashboard.png")


def plot_price_entropy_connection(df: pd.DataFrame, output_dir: Path):
    """
    Core visualization showing direct connection between price action and entropy.
    This helps develop intuition about what entropy measures actually capture.
    """
    if 'mid_price' not in df.columns:
        print("  Skipping price-entropy connection: missing mid_price")
        return

    # Calculate price returns
    df = df.copy()
    df['price_return'] = df['mid_price'].pct_change() * 10000  # basis points
    df['price_return_abs'] = df['price_return'].abs()
    df['price_direction'] = np.sign(df['price_return'])

    # Rolling price movement
    df['price_move_5'] = df['mid_price'].diff(5)
    df['price_move_20'] = df['mid_price'].diff(20)

    fig = plt.figure(figsize=(18, 16))
    gs = GridSpec(4, 3, figure=fig, hspace=0.35, wspace=0.3)

    # ===== ROW 1: Price chart with entropy-colored background =====
    ax1 = fig.add_subplot(gs[0, :])

    if 'tick_entropy_10s' in df.columns:
        valid = df[['timestamp', 'mid_price', 'tick_entropy_10s']].dropna()
        if len(valid) > 10:
            # Create entropy bins for background coloring
            entropy = valid['tick_entropy_10s'].values
            timestamps = mdates.date2num(valid['timestamp'])

            # Color background by entropy level
            for i in range(len(valid) - 1):
                e = entropy[i]
                if e < 0.3:
                    color = 'red'
                    alpha = 0.3
                elif e > 0.6:
                    color = 'green'
                    alpha = 0.2
                else:
                    color = 'yellow'
                    alpha = 0.1
                ax1.axvspan(timestamps[i], timestamps[i+1], alpha=alpha, color=color, linewidth=0)

            # Plot price line
            ax1.plot(valid['timestamp'], valid['mid_price'], 'k-', linewidth=1.2, label='Mid Price')

            ax1.set_ylabel('Price')
            ax1.set_title('Price Action with Entropy Background\n(Red=Low/Trending, Yellow=Medium, Green=High/Random)')
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            ax1.legend(loc='upper right')

    # ===== ROW 2: Price returns conditioned on entropy =====

    # 2a: Return distribution by entropy bin
    ax2a = fig.add_subplot(gs[1, 0])
    if 'tick_entropy_10s' in df.columns:
        valid = df[['price_return', 'tick_entropy_10s']].dropna()
        if len(valid) > 100:
            # Create entropy bins
            valid['entropy_bin'] = pd.cut(valid['tick_entropy_10s'],
                                          bins=[0, 0.3, 0.5, 0.7, 1.0],
                                          labels=['<0.3\n(trend)', '0.3-0.5', '0.5-0.7', '>0.7\n(random)'])

            colors = ['#d62728', '#ff7f0e', '#2ca02c', '#1f77b4']
            for i, (label, group) in enumerate(valid.groupby('entropy_bin', observed=True)):
                if len(group) > 10:
                    ax2a.hist(group['price_return'].clip(-50, 50), bins=40, alpha=0.5,
                             label=f'{label} (n={len(group)})', color=colors[i], density=True)

            ax2a.set_xlabel('Price Return (bps)')
            ax2a.set_ylabel('Density')
            ax2a.set_title('Return Distribution by Entropy')
            ax2a.legend(fontsize=7)
            ax2a.axvline(x=0, color='black', linestyle='--', alpha=0.5)

    # 2b: Absolute return vs entropy scatter
    ax2b = fig.add_subplot(gs[1, 1])
    if 'tick_entropy_10s' in df.columns:
        valid = df[['price_return_abs', 'tick_entropy_10s']].dropna()
        if len(valid) > 100:
            sample = valid.sample(min(2000, len(valid)))
            scatter = ax2b.scatter(sample['tick_entropy_10s'], sample['price_return_abs'].clip(0, 30),
                                  alpha=0.3, s=5, c=sample['tick_entropy_10s'], cmap='RdYlGn')
            ax2b.set_xlabel('Tick Entropy (10s)')
            ax2b.set_ylabel('|Price Return| (bps)')
            ax2b.set_title('Price Movement Magnitude vs Entropy')

            # Add binned mean line
            bins = np.linspace(0, 1, 20)
            valid['entropy_binned'] = pd.cut(valid['tick_entropy_10s'], bins=bins)
            means = valid.groupby('entropy_binned', observed=True)['price_return_abs'].mean()
            bin_centers = [(b.left + b.right) / 2 for b in means.index]
            ax2b.plot(bin_centers, means.values, 'r-', linewidth=2, label='Binned Mean')
            ax2b.legend()

    # 2c: Directional consistency by entropy
    ax2c = fig.add_subplot(gs[1, 2])
    if 'tick_entropy_10s' in df.columns:
        valid = df[['price_direction', 'tick_entropy_10s']].dropna()
        if len(valid) > 100:
            # Calculate rolling direction consistency (how often direction repeats)
            valid['dir_change'] = (valid['price_direction'] != valid['price_direction'].shift(1)).astype(int)

            bins = np.linspace(0, 1, 10)
            valid['entropy_bin'] = pd.cut(valid['tick_entropy_10s'], bins=bins)
            direction_change_rate = valid.groupby('entropy_bin', observed=True)['dir_change'].mean()

            bin_centers = [(b.left + b.right) / 2 for b in direction_change_rate.index]
            ax2c.bar(bin_centers, direction_change_rate.values, width=0.08, alpha=0.7, color='steelblue')
            ax2c.set_xlabel('Tick Entropy (10s)')
            ax2c.set_ylabel('Direction Change Rate')
            ax2c.set_title('Price Direction Randomness vs Entropy\n(Higher = more random direction)')
            ax2c.axhline(y=0.5, color='red', linestyle='--', alpha=0.5, label='Random (50%)')
            ax2c.legend()

    # ===== ROW 3: Sequential price movements =====

    # 3a: Entropy vs N-step price move
    ax3a = fig.add_subplot(gs[2, 0])
    if 'tick_entropy_30s' in df.columns and 'price_move_20' in df.columns:
        valid = df[['tick_entropy_30s', 'price_move_20']].dropna()
        if len(valid) > 100:
            sample = valid.sample(min(2000, len(valid)))
            colors = np.where(sample['price_move_20'] > 0, 'green', 'red')
            ax3a.scatter(sample['tick_entropy_30s'], sample['price_move_20'],
                        alpha=0.3, s=5, c=colors)
            ax3a.axhline(y=0, color='black', linestyle='--', alpha=0.5)
            ax3a.set_xlabel('Tick Entropy (30s)')
            ax3a.set_ylabel('20-tick Price Move')
            ax3a.set_title('Entropy vs Future Price Movement')

    # 3b: Entropy momentum vs price momentum
    ax3b = fig.add_subplot(gs[2, 1])
    if 'tick_entropy_5s' in df.columns and 'tick_entropy_30s' in df.columns:
        valid = df.copy()
        valid['entropy_momentum'] = valid['tick_entropy_5s'] - valid['tick_entropy_30s']
        valid['price_momentum'] = valid['mid_price'].pct_change(10) * 10000

        valid = valid[['entropy_momentum', 'price_momentum']].dropna()
        if len(valid) > 100:
            sample = valid.sample(min(2000, len(valid)))
            ax3b.scatter(sample['entropy_momentum'], sample['price_momentum'].clip(-50, 50),
                        alpha=0.3, s=5)
            ax3b.axhline(y=0, color='red', linestyle='--', alpha=0.5)
            ax3b.axvline(x=0, color='red', linestyle='--', alpha=0.5)
            ax3b.set_xlabel('Entropy Momentum (5s - 30s)')
            ax3b.set_ylabel('Price Momentum (bps)')
            ax3b.set_title('Entropy Momentum vs Price Momentum')

    # 3c: Entropy state transitions
    ax3c = fig.add_subplot(gs[2, 2])
    if 'tick_entropy_10s' in df.columns:
        valid = df[['tick_entropy_10s', 'price_return_abs']].dropna()
        if len(valid) > 50:
            # Classify entropy states
            valid['entropy_state'] = pd.cut(valid['tick_entropy_10s'],
                                           bins=[0, 0.4, 0.6, 1.0],
                                           labels=['Low', 'Medium', 'High'])
            valid['next_state'] = valid['entropy_state'].shift(-10)

            # Transition matrix
            transitions = pd.crosstab(valid['entropy_state'], valid['next_state'], normalize='index')

            if not transitions.empty:
                sns.heatmap(transitions, annot=True, fmt='.2f', cmap='Blues', ax=ax3c)
                ax3c.set_title('Entropy State Transition Matrix\n(10-tick lookahead)')
                ax3c.set_xlabel('Next State')
                ax3c.set_ylabel('Current State')

    # ===== ROW 4: Clustering and regime detection =====

    # 4a: 2D scatter for clustering visualization
    ax4a = fig.add_subplot(gs[3, 0])
    if 'tick_entropy_10s' in df.columns and 'volume_tick_entropy_10s' in df.columns:
        valid = df[['tick_entropy_10s', 'volume_tick_entropy_10s', 'price_return_abs']].dropna()
        if len(valid) > 100:
            sample = valid.sample(min(3000, len(valid)))
            scatter = ax4a.scatter(sample['tick_entropy_10s'], sample['volume_tick_entropy_10s'],
                                  c=sample['price_return_abs'].clip(0, 20), cmap='hot',
                                  alpha=0.5, s=10)
            plt.colorbar(scatter, ax=ax4a, label='|Return| (bps)')
            ax4a.plot([0, 1], [0, 1], 'b--', alpha=0.5)
            ax4a.set_xlabel('Tick Entropy (10s)')
            ax4a.set_ylabel('Volume Tick Entropy (10s)')
            ax4a.set_title('Entropy Space Colored by Price Movement')
            ax4a.set_xlim(0, 1)
            ax4a.set_ylim(0, 1)

    # 4b: Multi-timeframe entropy clustering
    ax4b = fig.add_subplot(gs[3, 1])
    if 'tick_entropy_5s' in df.columns and 'tick_entropy_1m' in df.columns:
        valid = df[['tick_entropy_5s', 'tick_entropy_1m', 'price_return_abs']].dropna()
        if len(valid) > 100:
            sample = valid.sample(min(3000, len(valid)))

            # Define quadrants
            sample['quadrant'] = 'Mixed'
            sample.loc[(sample['tick_entropy_5s'] < 0.4) & (sample['tick_entropy_1m'] < 0.5), 'quadrant'] = 'Short+Long Trend'
            sample.loc[(sample['tick_entropy_5s'] > 0.6) & (sample['tick_entropy_1m'] > 0.6), 'quadrant'] = 'Short+Long Random'
            sample.loc[(sample['tick_entropy_5s'] < 0.4) & (sample['tick_entropy_1m'] > 0.6), 'quadrant'] = 'Short Trend, Long Random'
            sample.loc[(sample['tick_entropy_5s'] > 0.6) & (sample['tick_entropy_1m'] < 0.5), 'quadrant'] = 'Short Random, Long Trend'

            colors_map = {'Short+Long Trend': 'red', 'Short+Long Random': 'green',
                         'Short Trend, Long Random': 'orange', 'Short Random, Long Trend': 'blue', 'Mixed': 'gray'}

            for quadrant, color in colors_map.items():
                subset = sample[sample['quadrant'] == quadrant]
                if len(subset) > 0:
                    ax4b.scatter(subset['tick_entropy_5s'], subset['tick_entropy_1m'],
                               c=color, alpha=0.4, s=10, label=f'{quadrant} ({len(subset)})')

            ax4b.axhline(y=0.5, color='black', linestyle='--', alpha=0.3)
            ax4b.axvline(x=0.5, color='black', linestyle='--', alpha=0.3)
            ax4b.set_xlabel('Short-term Entropy (5s)')
            ax4b.set_ylabel('Long-term Entropy (1m)')
            ax4b.set_title('Multi-Timeframe Regime Clusters')
            ax4b.legend(fontsize=6, loc='upper left')
            ax4b.set_xlim(0, 1)
            ax4b.set_ylim(0, 1)

    # 4c: Summary statistics by cluster
    ax4c = fig.add_subplot(gs[3, 2])
    if 'tick_entropy_10s' in df.columns:
        valid = df[['tick_entropy_10s', 'price_return_abs', 'price_return']].dropna()
        if len(valid) > 100:
            valid['regime'] = pd.cut(valid['tick_entropy_10s'],
                                    bins=[0, 0.3, 0.5, 0.7, 1.0],
                                    labels=['Trending\n(<0.3)', 'Moderate\n(0.3-0.5)',
                                           'Transitional\n(0.5-0.7)', 'Random\n(>0.7)'])

            stats = valid.groupby('regime', observed=True).agg({
                'price_return_abs': ['mean', 'std'],
                'price_return': 'mean'
            }).round(3)

            # Create text summary
            summary_lines = ['REGIME STATISTICS', '─' * 40]
            for regime in stats.index:
                mean_abs = stats.loc[regime, ('price_return_abs', 'mean')]
                std_abs = stats.loc[regime, ('price_return_abs', 'std')]
                mean_ret = stats.loc[regime, ('price_return', 'mean')]
                n = len(valid[valid['regime'] == regime])
                summary_lines.append(f'{regime}:')
                summary_lines.append(f'  |Return|: {mean_abs:.2f}±{std_abs:.2f} bps')
                summary_lines.append(f'  Bias: {mean_ret:+.3f} bps  (n={n})')
                summary_lines.append('')

            ax4c.axis('off')
            ax4c.text(0.1, 0.95, '\n'.join(summary_lines),
                     transform=ax4c.transAxes, fontsize=9, fontfamily='monospace',
                     verticalalignment='top')

    plt.suptitle('Price-Entropy Connection Analysis', fontsize=14, fontweight='bold', y=0.98)
    plt.savefig(output_dir / 'price_entropy_connection.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("  Saved: price_entropy_connection.png")


def plot_entropy_clusters(df: pd.DataFrame, output_dir: Path):
    """
    Dedicated clustering visualization to identify market regimes
    based on entropy features.
    """
    entropy_cols = get_entropy_columns(df)

    if len(entropy_cols['tick_entropy']) < 3:
        print("  Skipping entropy clusters: insufficient entropy columns")
        return

    fig = plt.figure(figsize=(16, 12))
    gs = GridSpec(3, 3, figure=fig, hspace=0.3, wspace=0.3)

    # Prepare feature matrix for clustering visualization
    feature_cols = ['tick_entropy_5s', 'tick_entropy_10s', 'tick_entropy_30s',
                   'volume_tick_entropy_5s', 'volume_tick_entropy_10s']
    feature_cols = [c for c in feature_cols if c in df.columns]

    if len(feature_cols) < 2:
        print("  Skipping entropy clusters: insufficient features")
        return

    valid = df[feature_cols + ['mid_price']].dropna().copy()
    if len(valid) < 100:
        print("  Skipping entropy clusters: insufficient data")
        return

    valid['price_return'] = valid['mid_price'].pct_change() * 10000

    # ===== Pairplot-style scatter matrix for key entropy pairs =====

    pairs = [
        ('tick_entropy_5s', 'tick_entropy_30s'),
        ('tick_entropy_5s', 'volume_tick_entropy_5s'),
        ('tick_entropy_10s', 'tick_entropy_30s'),
    ]

    for idx, (x_col, y_col) in enumerate(pairs):
        if x_col not in df.columns or y_col not in df.columns:
            continue

        ax = fig.add_subplot(gs[0, idx])
        sample = valid.sample(min(2000, len(valid)))

        # Color by price return
        colors = np.clip(sample['price_return'], -20, 20)
        scatter = ax.scatter(sample[x_col], sample[y_col], c=colors, cmap='RdYlGn',
                           alpha=0.5, s=8, vmin=-20, vmax=20)

        ax.set_xlabel(x_col.replace('_', ' ').title())
        ax.set_ylabel(y_col.replace('_', ' ').title())
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.plot([0, 1], [0, 1], 'k--', alpha=0.3)

        if idx == 2:
            plt.colorbar(scatter, ax=ax, label='Return (bps)')

    # ===== Hexbin density plots =====
    ax_hex1 = fig.add_subplot(gs[1, 0])
    if 'tick_entropy_5s' in df.columns and 'tick_entropy_30s' in df.columns:
        sample = valid.sample(min(5000, len(valid)))
        hb = ax_hex1.hexbin(sample['tick_entropy_5s'], sample['tick_entropy_30s'],
                           gridsize=25, cmap='YlOrRd', mincnt=1)
        ax_hex1.set_xlabel('Short-term (5s)')
        ax_hex1.set_ylabel('Long-term (30s)')
        ax_hex1.set_title('Entropy Density (Hexbin)')
        ax_hex1.set_xlim(0, 1)
        ax_hex1.set_ylim(0, 1)
        plt.colorbar(hb, ax=ax_hex1, label='Count')

    # ===== KDE contour plot =====
    ax_kde = fig.add_subplot(gs[1, 1])
    if 'tick_entropy_5s' in df.columns and 'tick_entropy_30s' in df.columns:
        sample = valid.sample(min(3000, len(valid)))
        try:
            sns.kdeplot(data=sample, x='tick_entropy_5s', y='tick_entropy_30s',
                       levels=10, cmap='Blues', fill=True, ax=ax_kde, thresh=0.05)
            ax_kde.set_xlabel('Short-term (5s)')
            ax_kde.set_ylabel('Long-term (30s)')
            ax_kde.set_title('Entropy KDE Contours')
            ax_kde.set_xlim(0, 1)
            ax_kde.set_ylim(0, 1)
        except Exception:
            ax_kde.text(0.5, 0.5, 'KDE failed', ha='center', va='center')

    # ===== Marginal distributions =====
    ax_marg = fig.add_subplot(gs[1, 2])
    for col in ['tick_entropy_5s', 'tick_entropy_30s', 'tick_entropy_1m']:
        if col in df.columns:
            data = df[col].dropna()
            label = col.replace('tick_entropy_', '')
            ax_marg.hist(data, bins=50, alpha=0.5, label=label, density=True)

    ax_marg.set_xlabel('Entropy Value')
    ax_marg.set_ylabel('Density')
    ax_marg.set_title('Marginal Distributions')
    ax_marg.legend()
    ax_marg.set_xlim(0, 1)

    # ===== Time evolution of entropy state =====
    ax_time = fig.add_subplot(gs[2, :2])
    if 'tick_entropy_10s' in df.columns:
        sample_size = min(1000, len(valid))
        indices = np.linspace(0, len(valid)-1, sample_size, dtype=int)
        plot_data = valid.iloc[indices].copy()

        # Define regime
        plot_data['regime'] = 'medium'
        plot_data.loc[plot_data['tick_entropy_10s'] < 0.35, 'regime'] = 'trending'
        plot_data.loc[plot_data['tick_entropy_10s'] > 0.65, 'regime'] = 'random'

        colors_map = {'trending': 'red', 'medium': 'yellow', 'random': 'green'}
        x_indices = np.arange(len(plot_data))
        for regime, color in colors_map.items():
            mask = (plot_data['regime'] == regime).values
            ax_time.scatter(x_indices[mask],
                          plot_data.loc[plot_data['regime'] == regime, 'tick_entropy_10s'],
                          c=color, s=3, alpha=0.6, label=regime)

        ax_time.set_xlabel('Sample Index (time)')
        ax_time.set_ylabel('Tick Entropy (10s)')
        ax_time.set_title('Entropy Regime Evolution Over Time')
        ax_time.axhline(y=0.35, color='red', linestyle='--', alpha=0.5)
        ax_time.axhline(y=0.65, color='green', linestyle='--', alpha=0.5)
        ax_time.legend()
        ax_time.set_ylim(0, 1)

    # ===== Regime duration histogram =====
    ax_dur = fig.add_subplot(gs[2, 2])
    if 'tick_entropy_10s' in df.columns:
        entropy = df['tick_entropy_10s'].dropna().values
        regime = np.where(entropy < 0.35, 'trend', np.where(entropy > 0.65, 'random', 'medium'))

        # Calculate regime durations
        durations = {'trend': [], 'medium': [], 'random': []}
        current_regime = regime[0]
        duration = 1

        for i in range(1, len(regime)):
            if regime[i] == current_regime:
                duration += 1
            else:
                durations[current_regime].append(duration)
                current_regime = regime[i]
                duration = 1
        durations[current_regime].append(duration)

        # Plot
        colors = {'trend': 'red', 'medium': 'yellow', 'random': 'green'}
        for r, durs in durations.items():
            if durs:
                ax_dur.hist(durs, bins=30, alpha=0.5, label=f'{r} (μ={np.mean(durs):.1f})',
                           color=colors[r])

        ax_dur.set_xlabel('Regime Duration (ticks)')
        ax_dur.set_ylabel('Frequency')
        ax_dur.set_title('Regime Persistence')
        ax_dur.legend()

    plt.suptitle('Entropy Clustering Analysis', fontsize=14, fontweight='bold', y=0.98)
    plt.savefig(output_dir / 'entropy_clusters.png', dpi=150, bbox_inches='tight')
    plt.close()
    print("  Saved: entropy_clusters.png")


def main():
    parser = argparse.ArgumentParser(description='Visualize entropy features from Parquet files')
    parser.add_argument('--file', '-f', type=str, help='Specific Parquet file to visualize')
    parser.add_argument('--all', '-a', action='store_true', help='Load all Parquet files')
    parser.add_argument('--output', '-o', type=str, default='./output/entropy_viz',
                       help='Output directory for visualizations')
    parser.add_argument('--data-dir', '-d', type=str, default='./data/features',
                       help='Directory containing Parquet files')

    args = parser.parse_args()

    # Set up paths
    data_dir = Path(args.data_dir)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nEntropy Feature Visualization")
    print("="*50)

    # Load data
    df = load_parquet_files(data_dir, args.file)

    # Check for entropy columns
    entropy_cols = get_entropy_columns(df)
    print(f"\nFound {len(entropy_cols['tick_entropy'])} tick entropy columns")
    print(f"Found {len(entropy_cols['volume_tick_entropy'])} volume tick entropy columns")

    if not entropy_cols['tick_entropy'] and not entropy_cols['volume_tick_entropy']:
        print("\nNo entropy columns found in data!")
        sys.exit(1)

    # Generate visualizations
    print(f"\nGenerating visualizations to {output_dir}/...")

    plot_entropy_time_series(df, output_dir)
    plot_entropy_with_price(df, output_dir)
    plot_entropy_heatmap(df, output_dir)
    plot_entropy_distribution(df, output_dir)
    plot_entropy_correlation(df, output_dir)
    plot_entropy_vs_volatility(df, output_dir)
    plot_entropy_regime_analysis(df, output_dir)
    plot_entropy_cross_timeframe(df, output_dir)
    plot_phase2_features(df, output_dir)
    plot_price_entropy_connection(df, output_dir)
    plot_entropy_clusters(df, output_dir)
    create_dashboard(df, output_dir)

    # Print statistics
    plot_entropy_statistics(df, output_dir)

    print(f"\nVisualization complete! Output saved to: {output_dir}/")
    print("\nKey files:")
    print("  - entropy_dashboard.png          (comprehensive overview)")
    print("  - price_entropy_connection.png   (CORE: price-entropy relationship)")
    print("  - entropy_clusters.png           (regime clustering analysis)")
    print("  - entropy_time_series.png        (entropy over time)")
    print("  - entropy_with_price.png         (entropy + price overlay)")
    print("  - entropy_heatmap.png            (multi-timeframe view)")
    print("  - entropy_distribution.png       (histogram analysis)")
    print("  - entropy_correlation.png        (feature correlations)")
    print("  - entropy_statistics.csv         (numerical summary)")


if __name__ == '__main__':
    main()
