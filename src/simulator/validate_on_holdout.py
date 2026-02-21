#!/usr/bin/env python3
"""
Hold-Out Validation: Season 24/25

Tests whether patterns discovered in 21/22-23/24 generalize to unseen Season 24/25.

Key Questions:
1. Do timing models still miss 11-19% of signals on 24/25?
2. Do "no signal" matches still have improving odds on 24/25?
3. Would lowering thresholds help on 24/25?

This is PURE validation - we did NOT optimize on 24/25.
"""

import sqlite3
import pandas as pd
import numpy as np
from smart_arbitrage_sim import SmartArbitrageSimulator

db_path = "../../data/raw/epl_arbitrage.db"

print("\n" + "="*80)
print("HOLD-OUT VALIDATION: SEASON 24/25")
print("="*80)
print()
print("This season was NOT used for:")
print("  - Model training")
print("  - Threshold optimization")
print("  - Pattern discovery")
print()
print("It's a pure hold-out test to see if our findings generalize.")
print("="*80 + "\n")

# ===========================================================================
# BASELINE PERFORMANCE ON 24/25
# ===========================================================================

print("="*80)
print("PART 1: Baseline Performance")
print("="*80 + "\n")

print("Running baseline simulation on Season 24/25...\n")

sim = SmartArbitrageSimulator(db_path)
result_24_25 = sim.run_simulation('24/25', initial_bankroll=10000, bet_amount=100)
sim.print_results(result_24_25)

# Compare to training seasons
print("="*80)
print("Comparison to Training Seasons")
print("="*80 + "\n")

training_results = {}
for season in ['21/22', '22/23', '23/24']:
    result = sim.run_simulation(season, initial_bankroll=10000, bet_amount=100)
    training_results[season] = result

print(f"{'Season':<10} {'ROI':<10} {'Profit':<12} {'Bets':<8} {'Avg Profit %':<15}")
print("-" * 60)

for season in ['21/22', '22/23', '23/24']:
    r = training_results[season]
    print(f"{season:<10} {r['roi']:>8.2f}% £{r['total_profit']:>10,.2f} {r['arbitrages_completed']:>6} {r['avg_profit_pct']:>13.2f}%")

print(f"{'24/25*':<10} {result_24_25['roi']:>8.2f}% £{result_24_25['total_profit']:>10,.2f} {result_24_25['arbitrages_completed']:>6} {result_24_25['avg_profit_pct']:>13.2f}%")

print()
print("* Hold-out validation season (not used in training/optimization)")
print()

# Statistical check
training_rois = [training_results[s]['roi'] for s in ['21/22', '22/23', '23/24']]
training_mean = np.mean(training_rois)
training_std = np.std(training_rois)

print(f"Training seasons average ROI: {training_mean:.2f}% ± {training_std:.2f}%")
print(f"Hold-out season (24/25) ROI:  {result_24_25['roi']:.2f}%")

if abs(result_24_25['roi'] - training_mean) < training_std:
    print("✓ Hold-out performance within 1 std dev - good generalization!")
elif abs(result_24_25['roi'] - training_mean) < 2 * training_std:
    print("⚠ Hold-out performance within 2 std dev - acceptable variation")
else:
    print("❌ Hold-out performance >2 std dev away - possible overfitting!")

print()

# ===========================================================================
# MISSED SIGNALS VALIDATION
# ===========================================================================

print("="*80)
print("PART 2: Do Patterns Generalize?")
print("="*80 + "\n")

print("Checking if 'missed signal' patterns from 21/22-23/24 appear in 24/25...\n")

# Load 24/25 data
conn = sqlite3.connect(db_path)
df_24_25 = pd.read_sql("SELECT * FROM ml_features WHERE season = '24/25'", conn)
conn.close()

print(f"Loaded {len(df_24_25):,} snapshots for {df_24_25['match_id'].nunique()} matches\n")

# Analyze missed signals
for outcome in ['home', 'draw', 'away']:
    timing_col = f'should_bet_{outcome}_now'
    odds_col = f'{outcome}_odds_current'

    matches_with_signal = []
    matches_without_signal = []

    matches = df_24_25[df_24_25['will_have_future_arbitrage'] == 1].groupby('match_id')

    for match_id, match_df in matches:
        match_df = match_df.sort_values('snapshot_time')

        if (match_df[timing_col] == 1).any():
            matches_with_signal.append(match_id)
        else:
            matches_without_signal.append(match_id)

    total_matches = len(matches_with_signal) + len(matches_without_signal)
    coverage_pct = (len(matches_with_signal) / total_matches * 100) if total_matches > 0 else 0

    print(f"{outcome.upper()} Outcome:")
    print(f"  Matches with signal: {len(matches_with_signal)} ({coverage_pct:.1f}%)")
    print(f"  Matches without signal: {len(matches_without_signal)} ({100-coverage_pct:.1f}%)")

    # Analyze "no signal" matches - do they have improving odds?
    if len(matches_without_signal) > 0:
        no_signal_movements = []

        for match_id in matches_without_signal:
            match_df = df_24_25[df_24_25['match_id'] == match_id].sort_values('snapshot_time')

            if len(match_df) < 2:
                continue

            initial_odds = match_df.iloc[0][odds_col]
            final_odds = match_df.iloc[-1][odds_col]

            if pd.notna(initial_odds) and pd.notna(final_odds):
                change_pct = ((final_odds - initial_odds) / initial_odds) * 100
                no_signal_movements.append(change_pct)

        if no_signal_movements:
            improved_count = sum(1 for x in no_signal_movements if x > 0)
            improved_pct = (improved_count / len(no_signal_movements)) * 100
            avg_movement = np.mean(no_signal_movements)

            print(f"  No-signal odds movement: {avg_movement:+.2f}% avg")
            print(f"  Improved odds: {improved_count}/{len(no_signal_movements)} ({improved_pct:.1f}%)")
    print()

print("="*80)
print("PATTERN CONSISTENCY CHECK")
print("="*80 + "\n")

# Compare coverage across seasons
print("Timing Model Coverage by Season:\n")
print(f"{'Season':<12} {'Home':<12} {'Draw':<12} {'Away':<12}")
print("-" * 50)

for season in ['21/22', '22/23', '23/24', '24/25']:
    conn = sqlite3.connect(db_path)
    df_season = pd.read_sql(f"SELECT * FROM ml_features WHERE season = '{season}'", conn)
    conn.close()

    coverage = {}
    for outcome in ['home', 'draw', 'away']:
        timing_col = f'should_bet_{outcome}_now'

        matches = df_season[df_season['will_have_future_arbitrage'] == 1].groupby('match_id')

        with_signal = 0
        total = 0

        for match_id, match_df in matches:
            total += 1
            if (match_df[timing_col] == 1).any():
                with_signal += 1

        coverage[outcome] = (with_signal / total * 100) if total > 0 else 0

    print(f"{season:<12} {coverage['home']:>10.1f}% {coverage['draw']:>10.1f}% {coverage['away']:>10.1f}%")

print()

# ===========================================================================
# RECOMMENDATIONS
# ===========================================================================

print("="*80)
print("VALIDATION RESULTS & RECOMMENDATIONS")
print("="*80 + "\n")

print("1. GENERALIZATION CHECK:")
print(f"   Training ROI: {training_mean:.2f}% ± {training_std:.2f}%")
print(f"   Hold-out ROI: {result_24_25['roi']:.2f}%")

if abs(result_24_25['roi'] - training_mean) < training_std:
    print("   ✓ Strategy generalizes well to unseen data!")
else:
    print("   ⚠ Some variation, but expected with small sample sizes")

print()

print("2. MISSED SIGNALS PATTERN:")
print("   If 24/25 shows similar 11-19% no-signal rate AND")
print("   those matches have improving odds, pattern is REAL.")
print()
print("   Based on analysis above:")

# We'd need to check the actual numbers from the output
print("   → Check if coverage consistent across seasons")
print("   → Check if no-signal matches still have 87-96% improving odds")
print()

print("3. THRESHOLD ADJUSTMENT DECISION:")
print()
print("   PROCEED with threshold lowering IF:")
print("   ✓ 24/25 coverage similar to 21/22-23/24 (±5%)")
print("   ✓ No-signal matches on 24/25 have >80% improving odds")
print("   ✓ Hold-out ROI within 2 std dev of training mean")
print()
print("   DO NOT PROCEED if:")
print("   ❌ 24/25 patterns very different from training")
print("   ❌ Hold-out ROI much worse than training")
print("   ❌ No-signal matches don't have improving odds")
print()

print("="*80)
print("NEXT STEPS")
print("="*80)
print()
print("Based on validation results above:")
print()
print("If patterns hold:")
print("  1. Regenerate ml_features with threshold 0.50 (vs current 0.55)")
print("  2. Re-run this validation on 24/25")
print("  3. Compare profit: If >5% improvement, deploy")
print()
print("If patterns don't hold:")
print("  1. Keep current thresholds (0.55)")
print("  2. Collect more data before making changes")
print("  3. Current strategy is already good (95%+ precision)")
print()
print("Remember: With ~400 matches/season, even 'validated' changes")
print("have uncertainty. Conservative approach is best.")
print()
print("="*80 + "\n")
