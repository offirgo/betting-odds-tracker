#!/usr/bin/env python3
"""
Threshold Adjustment Testing

Tests different threshold values for timing models to find the optimal balance
between coverage (firing signals) and precision (good timing).

Current thresholds:
- Home: 0.550
- Draw: 0.575
- Away: 0.550

We'll test: 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65
"""

import sqlite3
import pandas as pd
import numpy as np
import pickle
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath('../..'))

db_path = "../../data/raw/epl_arbitrage.db"

# Load models
print("Loading models...")
models = {}
model_files = {
    'home': '../../models/winners/model_home_timing_strong_precision.pkl',
    'draw': '../../models/winners/model_draw_timing_strong_lower_false_alarms.pkl',
    'away': '../../models/winners/model_away_timing_strong_precision.pkl'
}

for outcome, path in model_files.items():
    try:
        with open(path, 'rb') as f:
            models[outcome] = pickle.load(f)
        print(f"  ✓ Loaded {outcome} model")
    except Exception as e:
        print(f"  ✗ Failed to load {outcome} model: {e}")
        sys.exit(1)

print()

# Load data
print("Loading data...")
conn = sqlite3.connect(db_path)
df = pd.read_sql("SELECT * FROM ml_features WHERE season = '21/22'", conn)
conn.close()
print(f"Loaded {len(df):,} snapshots for {df['match_id'].nunique()} matches\n")

# Get feature columns (excluding labels and metadata)
label_cols = ['will_have_future_arbitrage', 'should_bet_home_now', 'should_bet_draw_now', 'should_bet_away_now']
meta_cols = ['match_id', 'season', 'home_team', 'away_team', 'snapshot_time']
feature_cols = [col for col in df.columns if col not in label_cols + meta_cols]

print(f"Using {len(feature_cols)} features for predictions\n")

# Test different thresholds
thresholds_to_test = [0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65]

print("="*100)
print("THRESHOLD TESTING RESULTS")
print("="*100 + "\n")

# Current thresholds for comparison
current_thresholds = {
    'home': 0.550,
    'draw': 0.575,
    'away': 0.550
}

for outcome in ['home', 'draw', 'away']:
    print(f"\n{'='*100}")
    print(f"{outcome.upper()} TIMING MODEL")
    print(f"{'='*100}\n")
    print(f"Current threshold: {current_thresholds[outcome]:.3f}\n")

    model = models[outcome]

    # Get matches with future arbitrage
    matches = df[df['will_have_future_arbitrage'] == 1].groupby('match_id')

    print(f"{'Threshold':<12} {'Coverage':<12} {'Fired':<8} {'Improved':<10} {'Avg Improvement':<18} {'Worsened':<10} {'Recommendation':<15}")
    print("-" * 100)

    results = []

    for threshold in thresholds_to_test:
        stats = {
            'threshold': threshold,
            'total_matches': 0,
            'signals_fired': 0,
            'odds_improved': 0,
            'odds_worsened': 0,
            'improvements': [],
            'worsenings': []
        }

        for match_id, match_df in matches:
            match_df = match_df.sort_values('snapshot_time')
            stats['total_matches'] += 1

            first_row = match_df.iloc[0]
            odds_col = f'{outcome}_odds_current'
            initial_odds = first_row[odds_col]

            if pd.isna(initial_odds):
                continue

            # Make predictions for all snapshots
            signal_fired = False

            for idx, row in match_df.iterrows():
                if row['snapshot_time'] == first_row['snapshot_time']:
                    continue  # Skip first snapshot

                # Prepare features
                try:
                    features = row[feature_cols].values.reshape(1, -1)

                    # Handle any NaN values
                    if pd.isna(features).any():
                        continue

                    # Get prediction probability
                    proba = model.predict_proba(features)[0][1]  # Probability of class 1

                    # Check if above threshold
                    if proba >= threshold and not signal_fired:
                        signal_fired = True
                        stats['signals_fired'] += 1

                        # Check odds improvement
                        signal_odds = row[odds_col]

                        if pd.notna(signal_odds):
                            change_pct = ((signal_odds - initial_odds) / initial_odds) * 100

                            if change_pct > 0:
                                stats['odds_improved'] += 1
                                stats['improvements'].append(change_pct)
                            else:
                                stats['odds_worsened'] += 1
                                stats['worsenings'].append(change_pct)

                        break  # First signal only

                except Exception as e:
                    continue

        # Calculate metrics
        coverage = (stats['signals_fired'] / stats['total_matches'] * 100) if stats['total_matches'] > 0 else 0
        improved_pct = (stats['odds_improved'] / stats['signals_fired'] * 100) if stats['signals_fired'] > 0 else 0
        worsened_pct = (stats['odds_worsened'] / stats['signals_fired'] * 100) if stats['signals_fired'] > 0 else 0
        avg_improvement = np.mean(stats['improvements']) if stats['improvements'] else 0

        # Recommendation logic
        if coverage < 75:
            recommendation = "Too conservative"
        elif coverage > 95:
            recommendation = "Too aggressive"
        elif improved_pct < 75:
            recommendation = "Poor quality"
        else:
            recommendation = "✓ Good balance"

        # Special marker for current threshold
        threshold_str = f"{threshold:.2f}"
        if abs(threshold - current_thresholds[outcome]) < 0.001:
            threshold_str += " *"

        print(f"{threshold_str:<12} {coverage:>6.1f}%      {stats['signals_fired']:>4}    "
              f"{improved_pct:>6.1f}%    {avg_improvement:>10.2f}%        "
              f"{worsened_pct:>6.1f}%    {recommendation:<15}")

        results.append({
            'threshold': threshold,
            'coverage': coverage,
            'signals_fired': stats['signals_fired'],
            'improved_pct': improved_pct,
            'avg_improvement': avg_improvement,
            'recommendation': recommendation
        })

    # Find best threshold
    print()
    print("ANALYSIS:")

    # Best for coverage
    best_coverage = max(results, key=lambda x: x['coverage'])
    print(f"  Best coverage: {best_coverage['threshold']:.2f} ({best_coverage['coverage']:.1f}%)")

    # Best for quality (improvement %)
    quality_results = [r for r in results if r['signals_fired'] > 0]
    if quality_results:
        best_quality = max(quality_results, key=lambda x: x['improved_pct'])
        print(f"  Best quality: {best_quality['threshold']:.2f} ({best_quality['improved_pct']:.1f}% improved)")

    # Best balance (coverage * quality)
    balance_results = [r for r in results if r['signals_fired'] > 0]
    if balance_results:
        for r in balance_results:
            r['balance_score'] = (r['coverage'] / 100) * (r['improved_pct'] / 100) * 100
        best_balance = max(balance_results, key=lambda x: x['balance_score'])
        print(f"  Best balance: {best_balance['threshold']:.2f} (score: {best_balance['balance_score']:.1f})")

    # Recommendation
    print()
    print("RECOMMENDATION:")
    current = current_thresholds[outcome]
    current_result = next((r for r in results if abs(r['threshold'] - current) < 0.01), None)

    if current_result:
        print(f"  Current ({current:.3f}): {current_result['coverage']:.1f}% coverage, {current_result['improved_pct']:.1f}% improved")

    if best_balance['threshold'] < current - 0.05:
        print(f"  ⚠️  Consider LOWERING to {best_balance['threshold']:.2f} for better coverage")
    elif best_balance['threshold'] > current + 0.05:
        print(f"  ⚠️  Consider RAISING to {best_balance['threshold']:.2f} for better quality")
    else:
        print(f"  ✓ Current threshold is near optimal")

    print()

# Summary recommendations
print("\n" + "="*100)
print("SUMMARY RECOMMENDATIONS")
print("="*100 + "\n")

print("Current vs. Optimal Thresholds:\n")
print(f"{'Outcome':<10} {'Current':<12} {'Recommended':<12} {'Change':<12} {'Reason':<40}")
print("-" * 100)

# This would be filled in based on the analysis above
print(f"{'Home':<10} {current_thresholds['home']:<12.3f} {'TBD':<12} {'TBD':<12} {'See analysis above':<40}")
print(f"{'Draw':<10} {current_thresholds['draw']:<12.3f} {'TBD':<12} {'TBD':<12} {'See analysis above':<40}")
print(f"{'Away':<10} {current_thresholds['away']:<12.3f} {'TBD':<12} {'TBD':<12} {'See analysis above':<40}")

print("\n" + "="*100)
print("NEXT STEPS")
print("="*100)
print()
print("1. Review threshold recommendations above")
print("2. Update threshold files in models/winners/")
print("3. Re-run simulation to measure profit impact")
print("4. If improvement confirmed, commit new thresholds")
print()
print("="*100 + "\n")
