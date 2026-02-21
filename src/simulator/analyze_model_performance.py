#!/usr/bin/env python3
"""
Model Performance Analysis

Analyzes how well our ML models are performing:
1. Model 1 (will_have_future_arbitrage): Are we identifying the right matches?
2. Models 2-4 (timing signals): Are we timing bets optimally?
3. Where are models failing? (False positives, false negatives, missed opportunities)
4. What could we improve?
"""

import sqlite3
import pandas as pd
import numpy as np

db_path = "../../data/raw/epl_arbitrage.db"

print("\n" + "="*80)
print("MODEL PERFORMANCE ANALYSIS")
print("="*80 + "\n")

# Load data
conn = sqlite3.connect(db_path)
df = pd.read_sql("SELECT * FROM ml_features WHERE season = '21/22'", conn)
conn.close()

print(f"Loaded {len(df):,} snapshots for {df['match_id'].nunique()} matches\n")

# ============================================================================
# MODEL 1 ANALYSIS: will_have_future_arbitrage
# ============================================================================

print("="*80)
print("MODEL 1: Future Arbitrage Detection")
print("="*80 + "\n")

# Group by match and check if arbitrage ever developed
matches = df.groupby('match_id')

model1_stats = {
    'predicted_yes': 0,
    'predicted_no': 0,
    'true_positives': 0,
    'false_positives': 0,
    'true_negatives': 0,
    'false_negatives': 0
}

for match_id, match_df in matches:
    match_df = match_df.sort_values('snapshot_time')
    first_row = match_df.iloc[0]

    # Model 1 prediction (at first snapshot)
    prediction = first_row['will_have_future_arbitrage']

    # Ground truth: Did arbitrage actually develop?
    # Check if combined_inverse ever went below 1.0
    had_arbitrage = (match_df['combined_inverse_current'] < 1.0).any()

    if prediction == 1:
        model1_stats['predicted_yes'] += 1
        if had_arbitrage:
            model1_stats['true_positives'] += 1
        else:
            model1_stats['false_positives'] += 1
    else:
        model1_stats['predicted_no'] += 1
        if not had_arbitrage:
            model1_stats['true_negatives'] += 1
        else:
            model1_stats['false_negatives'] += 1

# Calculate metrics
total = model1_stats['predicted_yes'] + model1_stats['predicted_no']
precision = model1_stats['true_positives'] / model1_stats['predicted_yes'] if model1_stats['predicted_yes'] > 0 else 0
recall = model1_stats['true_positives'] / (model1_stats['true_positives'] + model1_stats['false_negatives']) if (model1_stats['true_positives'] + model1_stats['false_negatives']) > 0 else 0
f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
accuracy = (model1_stats['true_positives'] + model1_stats['true_negatives']) / total

print("PREDICTIONS:")
print(f"  Predicted YES (will have arbitrage): {model1_stats['predicted_yes']:>4} ({model1_stats['predicted_yes']/total*100:5.1f}%)")
print(f"  Predicted NO (won't have arbitrage): {model1_stats['predicted_no']:>4} ({model1_stats['predicted_no']/total*100:5.1f}%)")
print()

print("CONFUSION MATRIX:")
print(f"  True Positives (correct YES):  {model1_stats['true_positives']:>4}")
print(f"  False Positives (wrong YES):   {model1_stats['false_positives']:>4}")
print(f"  True Negatives (correct NO):   {model1_stats['true_negatives']:>4}")
print(f"  False Negatives (wrong NO):    {model1_stats['false_negatives']:>4}")
print()

print("PERFORMANCE METRICS:")
print(f"  Accuracy:  {accuracy*100:>6.2f}% (overall correct predictions)")
print(f"  Precision: {precision*100:>6.2f}% (when we predict YES, how often right?)")
print(f"  Recall:    {recall*100:>6.2f}% (of all arbitrages, how many did we catch?)")
print(f"  F1 Score:  {f1*100:>6.2f}% (harmonic mean of precision/recall)")
print()

print("INTERPRETATION:")
if model1_stats['false_negatives'] > 0:
    print(f"  ⚠️  MISSED OPPORTUNITIES: {model1_stats['false_negatives']} matches had arbitrage but we predicted NO")
    print(f"      These are lost profits! We should investigate why model missed these.")
if model1_stats['false_positives'] > 0:
    print(f"  ⚠️  WASTED EFFORT: {model1_stats['false_positives']} matches predicted YES but never had arbitrage")
    print(f"      We're tracking these matches unnecessarily.")
print()

# ============================================================================
# MODEL 2-4 ANALYSIS: Timing Signals
# ============================================================================

print("="*80)
print("MODELS 2-4: Timing Signal Analysis")
print("="*80 + "\n")

# For matches we actually bet on, analyze timing quality
timing_analysis = {
    'home': {'signals': 0, 'improvements': [], 'worsenings': []},
    'draw': {'signals': 0, 'improvements': [], 'worsenings': []},
    'away': {'signals': 0, 'improvements': [], 'worsenings': []}
}

for match_id, match_df in matches:
    match_df = match_df.sort_values('snapshot_time')
    first_row = match_df.iloc[0]

    # Only analyze matches with future arbitrage
    if first_row['will_have_future_arbitrage'] != 1:
        continue

    for outcome in ['home', 'draw', 'away']:
        timing_col = f'should_bet_{outcome}_now'
        odds_col = f'{outcome}_odds_current'

        # Find when signal fired
        signals = match_df[match_df[timing_col] == 1]

        if len(signals) > 0:
            timing_analysis[outcome]['signals'] += 1

            # Get initial odds
            initial_odds = first_row[odds_col]

            # Get odds when signal fired
            signal_odds = signals.iloc[0][odds_col]

            if pd.notna(initial_odds) and pd.notna(signal_odds):
                change_pct = ((signal_odds - initial_odds) / initial_odds) * 100

                if change_pct > 0:
                    timing_analysis[outcome]['improvements'].append(change_pct)
                else:
                    timing_analysis[outcome]['worsenings'].append(change_pct)

print("TIMING SIGNAL QUALITY:\n")

for outcome in ['home', 'draw', 'away']:
    stats = timing_analysis[outcome]
    total_signals = stats['signals']
    improvements = stats['improvements']
    worsenings = stats['worsenings']

    if total_signals == 0:
        continue

    print(f"{outcome.upper()} Timing:")
    print(f"  Total signals fired: {total_signals}")

    if improvements:
        print(f"  Improved odds: {len(improvements)} ({len(improvements)/total_signals*100:.1f}%)")
        print(f"    Avg improvement: +{np.mean(improvements):.2f}%")
        print(f"    Best improvement: +{max(improvements):.2f}%")

    if worsenings:
        print(f"  Worsened odds: {len(worsenings)} ({len(worsenings)/total_signals*100:.1f}%)")
        print(f"    Avg worsening: {np.mean(worsenings):.2f}%")
        print(f"    Worst worsening: {min(worsenings):.2f}%")

    print()

# ============================================================================
# MISSED SIGNALS ANALYSIS
# ============================================================================

print("="*80)
print("MISSED SIGNALS: When Did Models NOT Signal?")
print("="*80 + "\n")

missed_signals = {
    'home': 0,
    'draw': 0,
    'away': 0
}

for match_id, match_df in matches:
    match_df = match_df.sort_values('snapshot_time')
    first_row = match_df.iloc[0]

    if first_row['will_have_future_arbitrage'] != 1:
        continue

    for outcome in ['home', 'draw', 'away']:
        timing_col = f'should_bet_{outcome}_now'

        # Check if signal EVER fired for this outcome
        if (match_df[timing_col] == 1).any():
            continue
        else:
            missed_signals[outcome] += 1

total_matches_tracked = model1_stats['predicted_yes']

print("Matches where timing models NEVER fired:\n")
for outcome in ['home', 'draw', 'away']:
    count = missed_signals[outcome]
    pct = (count / total_matches_tracked * 100) if total_matches_tracked > 0 else 0
    print(f"  {outcome.capitalize()}: {count} matches ({pct:.1f}%)")
print()

print("IMPLICATION:")
print("  When models don't fire, we bet at last snapshot (suboptimal timing)")
print("  These are opportunities for model improvement!")
print()

# ============================================================================
# IMPROVEMENT OPPORTUNITIES
# ============================================================================

print("="*80)
print("MODEL IMPROVEMENT OPPORTUNITIES")
print("="*80 + "\n")

print("1. MODEL 1 (Future Arbitrage Detection):")
if model1_stats['false_negatives'] > 0:
    print(f"   ⚠️  Fix {model1_stats['false_negatives']} false negatives (missed arbitrages)")
    print(f"      - Retrain with better features")
    print(f"      - Lower threshold for higher recall")
    print(f"      - Analyze what makes these matches different")

if model1_stats['false_positives'] > 0:
    print(f"   ⚠️  Reduce {model1_stats['false_positives']} false positives (wasted tracking)")
    print(f"      - Improve precision with better features")
    print(f"      - Higher threshold to reduce false alarms")

print()

print("2. MODELS 2-4 (Timing Signals):")
for outcome in ['home', 'draw', 'away']:
    if missed_signals[outcome] > 0:
        print(f"   ⚠️  {outcome.capitalize()}: Model doesn't fire on {missed_signals[outcome]} matches")
        print(f"      - Investigate why no signals")
        print(f"      - Retrain with more diverse examples")
        print(f"      - Adjust threshold for more sensitivity")

print()

print("3. NEW MODELS TO CONSIDER:")
print("   💡 Profit magnitude predictor: Predict HOW MUCH profit (not just yes/no)")
print("   💡 Optimal wait time: Predict WHEN odds will be best (not just binary signal)")
print("   💡 Outcome-specific arbitrage: Predict which outcome will have best movement")
print("   💡 Multi-step ahead: Predict odds movements 2-3 snapshots ahead")

print()

print("="*80)
print("NEXT STEPS FOR MODEL IMPROVEMENT")
print("="*80)
print()
print("Priority 1: Analyze false negatives from Model 1")
print("  - What features differentiate missed arbitrages?")
print("  - Can we add new features to catch these?")
print()
print("Priority 2: Improve timing model coverage")
print("  - Why do models not fire on some matches?")
print("  - Can we adjust thresholds for better coverage?")
print()
print("Priority 3: Feature engineering")
print("  - Current features may not capture all patterns")
print("  - Add: bookmaker diversity, odds volatility, time-of-week, etc.")
print()
print("="*80 + "\n")
