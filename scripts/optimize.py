#!/usr/bin/env python3
"""
Bayesian Optimization for Market Making Parameters using Optuna.

This script uses Optuna's TPE (Tree-structured Parzen Estimator) sampler
to efficiently search the parameter space, learning from previous trials
to focus on promising regions.

Usage:
    python3 scripts/optimize.py --trials 100 --metric sharpe
    python3 scripts/optimize.py --trials 50 --metric return --timeout 300

Requirements:
    pip install optuna
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

try:
    import optuna
    from optuna.samplers import TPESampler
except ImportError:
    print("Error: Optuna not installed. Run: pip3 install optuna")
    sys.exit(1)


def run_backtest(spread: float, skew: float, fill_prob: float,
                 high_entropy: float, entropy_gate: bool) -> dict:
    """Run a single backtest with given parameters and return metrics."""

    cmd = [
        "cargo", "run", "--release", "--bin", "backtest", "--",
        "--spread", str(spread),
        "--skew", str(skew),
        "--fill-prob", str(fill_prob),
        "--high-entropy", str(high_entropy),
        "--json"  # Output JSON for parsing
    ]

    if entropy_gate:
        cmd.append("--entropy-gate")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
            cwd=Path(__file__).parent.parent
        )

        # Parse JSON output from backtest
        # Look for JSON in output
        for line in result.stdout.split('\n'):
            line = line.strip()
            if line.startswith('{') and line.endswith('}'):
                try:
                    return json.loads(line)
                except json.JSONDecodeError:
                    continue

        # If no JSON found, try to parse metrics from text output
        metrics = {
            'sharpe': float('-inf'),
            'total_return': 0.0,
            'max_drawdown': 1.0,
            'num_trades': 0,
            'win_rate': 0.0
        }

        for line in result.stdout.split('\n'):
            if 'Sharpe' in line:
                try:
                    # Parse "Sharpe: +0.12" or "Sharpe=-1.20"
                    val = line.split('Sharpe')[1].strip().lstrip(':=').split()[0]
                    metrics['sharpe'] = float(val.replace('+', ''))
                except:
                    pass
            if 'Return' in line or 'Ret=' in line:
                try:
                    if 'Ret=' in line:
                        val = line.split('Ret=')[1].split('%')[0]
                    else:
                        val = line.split('Return')[1].strip().lstrip(':=').split('%')[0]
                    metrics['total_return'] = float(val.replace('+', '')) / 100
                except:
                    pass
            if 'Trades' in line or 'Tr=' in line:
                try:
                    if 'Tr=' in line:
                        val = line.split('Tr=')[1].split()[0]
                    else:
                        val = line.split('Trades')[1].strip().lstrip(':=').split()[0]
                    metrics['num_trades'] = int(val)
                except:
                    pass

        return metrics

    except subprocess.TimeoutExpired:
        return {'sharpe': float('-inf'), 'total_return': 0, 'num_trades': 0}
    except Exception as e:
        print(f"Error running backtest: {e}")
        return {'sharpe': float('-inf'), 'total_return': 0, 'num_trades': 0}


def objective(trial: optuna.Trial, metric: str = 'sharpe') -> float:
    """Optuna objective function - maximize the chosen metric."""

    # Sample parameters using Optuna's smart sampling
    spread = trial.suggest_float('spread', 0.5, 5.0, step=0.5)
    skew = trial.suggest_float('skew', 0.1, 1.5, step=0.1)
    fill_prob = trial.suggest_float('fill_prob', 0.03, 0.20, step=0.01)
    high_entropy = trial.suggest_float('high_entropy', 0.5, 0.9, step=0.05)
    entropy_gate = trial.suggest_categorical('entropy_gate', [False, True])

    # Run backtest
    results = run_backtest(spread, skew, fill_prob, high_entropy, entropy_gate)

    # Store all metrics as user attributes for later analysis
    trial.set_user_attr('total_return', results.get('total_return', 0))
    trial.set_user_attr('sharpe', results.get('sharpe', float('-inf')))
    trial.set_user_attr('num_trades', results.get('num_trades', 0))
    trial.set_user_attr('max_drawdown', results.get('max_drawdown', 0))
    trial.set_user_attr('win_rate', results.get('win_rate', 0))

    # Return objective value based on metric choice
    if metric == 'sharpe':
        value = results.get('sharpe', float('-inf'))
        # Handle edge cases
        if value == float('-inf') or value < -100:
            return -100  # Cap at -100 for Optuna
        return value
    elif metric == 'return':
        return results.get('total_return', 0) * 100  # Convert to percentage
    elif metric == 'risk_adjusted':
        # Custom: return / max_drawdown ratio
        ret = results.get('total_return', 0)
        dd = results.get('max_drawdown', 1.0)
        if dd <= 0:
            dd = 0.001
        return ret / dd
    else:
        return results.get('sharpe', float('-inf'))


def print_banner():
    print("""
╔═══════════════════════════════════════════════════════════════════╗
║          BAYESIAN OPTIMIZATION FOR MARKET MAKING                  ║
║                     Powered by Optuna TPE                          ║
╚═══════════════════════════════════════════════════════════════════╝
""")


def main():
    parser = argparse.ArgumentParser(
        description='Bayesian optimization for market making parameters'
    )
    parser.add_argument(
        '--trials', '-n', type=int, default=50,
        help='Number of optimization trials (default: 50)'
    )
    parser.add_argument(
        '--metric', '-m', type=str, default='return',
        choices=['sharpe', 'return', 'risk_adjusted'],
        help='Metric to optimize (default: return)'
    )
    parser.add_argument(
        '--timeout', '-t', type=int, default=None,
        help='Timeout in seconds (optional)'
    )
    parser.add_argument(
        '--output', '-o', type=str, default='optuna_results.json',
        help='Output file for results (default: optuna_results.json)'
    )
    parser.add_argument(
        '--study-name', type=str, default=None,
        help='Optuna study name (for persistence)'
    )
    parser.add_argument(
        '--db', type=str, default=None,
        help='SQLite database for study persistence (e.g., sqlite:///optuna.db)'
    )

    args = parser.parse_args()

    print_banner()
    print(f"Configuration:")
    print(f"  Trials: {args.trials}")
    print(f"  Metric: {args.metric}")
    print(f"  Timeout: {args.timeout}s" if args.timeout else "  Timeout: None")
    print(f"  Output: {args.output}")
    print()

    # Create study with TPE sampler (Bayesian optimization)
    sampler = TPESampler(
        n_startup_trials=10,  # Random trials before TPE kicks in
        seed=42  # For reproducibility
    )

    study_name = args.study_name or f"mm_optimization_{datetime.now():%Y%m%d_%H%M%S}"
    storage = args.db if args.db else None

    study = optuna.create_study(
        study_name=study_name,
        storage=storage,
        direction='maximize',
        sampler=sampler,
        load_if_exists=True
    )

    # Optimize
    print("Starting optimization...\n")
    print("=" * 70)

    def callback(study, trial):
        """Print progress after each trial."""
        ret = trial.user_attrs.get('total_return', 0) * 100
        sharpe = trial.user_attrs.get('sharpe', 0)
        trades = trial.user_attrs.get('num_trades', 0)

        print(f"[{trial.number + 1:3d}/{args.trials}] "
              f"spread={trial.params['spread']:.1f} "
              f"skew={trial.params['skew']:.1f} "
              f"fp={trial.params['fill_prob']:.2f} "
              f"{'GATE' if trial.params['entropy_gate'] else 'WIDE'} "
              f"=> Ret={ret:+.2f}% Sharpe={sharpe:+.2f} Trades={trades}")

    study.optimize(
        lambda trial: objective(trial, args.metric),
        n_trials=args.trials,
        timeout=args.timeout,
        callbacks=[callback],
        show_progress_bar=False
    )

    print("=" * 70)
    print()

    # Results
    print("╔═══════════════════════════════════════════════════════════════════╗")
    print("║                      OPTIMIZATION RESULTS                          ║")
    print("╚═══════════════════════════════════════════════════════════════════╝")
    print()

    best = study.best_trial
    print("BEST PARAMETERS:")
    print(f"  Spread:        {best.params['spread']:.1f} bps")
    print(f"  Skew:          {best.params['skew']:.2f}")
    print(f"  Fill Prob:     {best.params['fill_prob']:.2f}")
    print(f"  High Entropy:  {best.params['high_entropy']:.2f}")
    print(f"  Entropy Gate:  {'GATE' if best.params['entropy_gate'] else 'WIDE'}")
    print()
    print("BEST METRICS:")
    print(f"  Return:        {best.user_attrs.get('total_return', 0) * 100:+.2f}%")
    print(f"  Sharpe:        {best.user_attrs.get('sharpe', 0):+.2f}")
    print(f"  Trades:        {best.user_attrs.get('num_trades', 0)}")
    print(f"  Win Rate:      {best.user_attrs.get('win_rate', 0) * 100:.1f}%")
    print(f"  Max Drawdown:  {best.user_attrs.get('max_drawdown', 0) * 100:.2f}%")
    print()

    # Top 10 trials
    print("TOP 10 TRIALS (by objective):")
    print("-" * 70)

    sorted_trials = sorted(study.trials, key=lambda t: t.value if t.value else float('-inf'), reverse=True)
    for i, trial in enumerate(sorted_trials[:10], 1):
        ret = trial.user_attrs.get('total_return', 0) * 100
        sharpe = trial.user_attrs.get('sharpe', 0)
        trades = trial.user_attrs.get('num_trades', 0)
        gate = 'GATE' if trial.params.get('entropy_gate', False) else 'WIDE'
        print(f"{i:2d}. s={trial.params['spread']:.1f} k={trial.params['skew']:.1f} "
              f"fp={trial.params['fill_prob']:.2f} {gate} => "
              f"Ret={ret:+.2f}% Sharpe={sharpe:+.2f} Trades={trades}")

    print()

    # Parameter importance
    try:
        importance = optuna.importance.get_param_importances(study)
        print("PARAMETER IMPORTANCE:")
        print("-" * 40)
        for param, imp in sorted(importance.items(), key=lambda x: x[1], reverse=True):
            bar = '█' * int(imp * 30)
            print(f"  {param:15s} {bar} {imp:.1%}")
        print()
    except:
        pass  # Importance might fail with few trials

    # Save results
    results = {
        'study_name': study_name,
        'metric': args.metric,
        'n_trials': len(study.trials),
        'best_params': best.params,
        'best_value': best.value,
        'best_metrics': {
            'total_return': best.user_attrs.get('total_return', 0),
            'sharpe': best.user_attrs.get('sharpe', 0),
            'num_trades': best.user_attrs.get('num_trades', 0),
            'win_rate': best.user_attrs.get('win_rate', 0),
            'max_drawdown': best.user_attrs.get('max_drawdown', 0),
        },
        'all_trials': [
            {
                'number': t.number,
                'params': t.params,
                'value': t.value,
                'metrics': {
                    'total_return': t.user_attrs.get('total_return', 0),
                    'sharpe': t.user_attrs.get('sharpe', 0),
                    'num_trades': t.user_attrs.get('num_trades', 0),
                }
            }
            for t in study.trials
        ]
    }

    output_path = Path(__file__).parent.parent / args.output
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"Results saved to: {output_path}")

    # Command to run best params
    print()
    print("RUN BEST PARAMETERS:")
    print("-" * 70)
    gate_flag = "--entropy-gate" if best.params['entropy_gate'] else ""
    print(f"cargo run --release --bin backtest -- \\")
    print(f"    --spread {best.params['spread']:.1f} \\")
    print(f"    --skew {best.params['skew']:.2f} \\")
    print(f"    --fill-prob {best.params['fill_prob']:.2f} \\")
    print(f"    --high-entropy {best.params['high_entropy']:.2f} {gate_flag}")
    print()


if __name__ == '__main__':
    main()
