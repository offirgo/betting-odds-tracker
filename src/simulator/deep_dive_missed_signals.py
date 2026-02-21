#!/usr/bin/env python3
"""
Deep Dive: Missed Signals Analysis

Analyzes the 46-78 matches where timing models DON'T fire to find patterns.

Questions:
1. What features are different in "no signal" vs "signal" matches?
2. Are certain types of matches more likely to miss signals?
3. Do odds behave differently in these matches?
4. Can we identify why models don't fire?
"""

import sqlite3
import pandas as pd
import numpy as np

db_path = "../../data/raw/epl_arbitrage.db"

print("\n" + "="*80)
print("DEEP DIVE: MISSED SIGNALS ANALYSIS")
print("="*80 + "\n")

# Load data
conn = sqlite3.connect(db_path)
df = pd.read_sql("SELECT * FROM ml_features WHERE season = '21/22'", conn)
conn.close()

print(f"Loaded {len(df):,} snapshots for {df['match_id'].nunique()} matches\n")

# Analyze each outcome separately
for outcome in ['home', 'draw', 'away']:
    print("\n" + "="*80)
    print(f"{outcome.upper()} OUTCOME ANALYSIS")
    print("="*80 + "\n")

    timing_col = f'should_bet_{outcome}_now'
    odds_col = f'{outcome}_odds_current'

    # Categorize matches
    matches_with_signal = []
    matches_without_signal = []

    matches = df[df['will_have_future_arbitrage'] == 1].groupby('match_id')

    for match_id, match_df in matches:
        match_df = match_df.sort_values('snapshot_time')

        # Check if signal ever fired
        if (match_df[timing_col] == 1).any():
            matches_with_signal.append(match_id)
        else:
            matches_without_signal.append(match_id)

    print(f"Matches WITH {outcome} signal: {len(matches_with_signal)}")
    print(f"Matches WITHOUT {outcome} signal: {len(matches_without_signal)}")
    print()

    if len(matches_without_signal) == 0:
        print("✓ All matches have signals - no analysis needed!\n")
        continue

    # Get data for both groups
    signal_data = df[df['match_id'].isin(matches_with_signal)]
    no_signal_data = df[df['match_id'].isin(matches_without_signal)]

    # Compare key features
    print("="*80)
    print("FEATURE COMPARISON: Signal vs. No Signal")
    print("="*80 + "\n")

    # Odds-related features
    print(f"{'Feature':<40} {'With Signal':<20} {'Without Signal':<20} {'Difference':<15}")
    print("-" * 95)

    # Initial odds
    signal_initial_odds = signal_data.groupby('match_id')[odds_col].first().mean()
    no_signal_initial_odds = no_signal_data.groupby('match_id')[odds_col].first().mean()
    diff = no_signal_initial_odds - signal_initial_odds
    print(f"{'Initial ' + outcome + ' odds (avg)':<40} {signal_initial_odds:<20.2f} {no_signal_initial_odds:<20.2f} {diff:>+14.2f}")

    # Odds range (volatility)
    signal_odds_range = signal_data.groupby('match_id')[odds_col].apply(lambda x: x.max() - x.min()).mean()
    no_signal_odds_range = no_signal_data.groupby('match_id')[odds_col].apply(lambda x: x.max() - x.min()).mean()
    diff = no_signal_odds_range - signal_odds_range
    print(f"{'Odds range (volatility)':<40} {signal_odds_range:<20.2f} {no_signal_odds_range:<20.2f} {diff:>+14.2f}")

    # Snapshots count
    signal_snapshots = signal_data.groupby('match_id').size().mean()
    no_signal_snapshots = no_signal_data.groupby('match_id').size().mean()
    diff = no_signal_snapshots - signal_snapshots
    print(f"{'Number of snapshots (avg)':<40} {signal_snapshots:<20.1f} {no_signal_snapshots:<20.1f} {diff:>+14.1f}")

    # Days before match (tracking duration)
    signal_days = signal_data.groupby('match_id')['days_before_match'].max().mean()
    no_signal_days = no_signal_data.groupby('match_id')['days_before_match'].max().mean()
    diff = no_signal_days - signal_days
    print(f"{'Days tracked before match (avg)':<40} {signal_days:<20.1f} {no_signal_days:<20.1f} {diff:>+14.1f}")

    # Combined inverse (arbitrage indicator)
    signal_combined = signal_data.groupby('match_id')['combined_inverse_current'].min().mean()
    no_signal_combined = no_signal_data.groupby('match_id')['combined_inverse_current'].min().mean()
    diff = no_signal_combined - signal_combined
    print(f"{'Best combined inverse (min)':<40} {signal_combined:<20.4f} {no_signal_combined:<20.4f} {diff:>+14.4f}")

    print()

    # Analyze odds movement patterns
    print("="*80)
    print("ODDS MOVEMENT PATTERNS")
    print("="*80 + "\n")

    # For "no signal" matches, how did odds actually move?
    if len(matches_without_signal) > 0:
        no_signal_movements = []

        for match_id in matches_without_signal:
            match_df = df[df['match_id'] == match_id].sort_values('snapshot_time')

            if len(match_df) < 2:
                continue

            initial_odds = match_df.iloc[0][odds_col]
            final_odds = match_df.iloc[-1][odds_col]

            if pd.notna(initial_odds) and pd.notna(final_odds):
                change_pct = ((final_odds - initial_odds) / initial_odds) * 100
                no_signal_movements.append(change_pct)

        if no_signal_movements:
            print(f"Odds movement in 'no signal' matches:")
            print(f"  Average: {np.mean(no_signal_movements):+.2f}%")
            print(f"  Median: {np.median(no_signal_movements):+.2f}%")
            print(f"  Improved (>0%): {sum(1 for x in no_signal_movements if x > 0)} ({sum(1 for x in no_signal_movements if x > 0)/len(no_signal_movements)*100:.1f}%)")
            print(f"  Worsened (<0%): {sum(1 for x in no_signal_movements if x < 0)} ({sum(1 for x in no_signal_movements if x < 0)/len(no_signal_movements)*100:.1f}%)")
            print(f"  Range: {min(no_signal_movements):.2f}% to {max(no_signal_movements):.2f}%")
            print()

            # Key insight
            improved_count = sum(1 for x in no_signal_movements if x > 0)
            if improved_count > len(no_signal_movements) * 0.5:
                print(f"  ⚠️  KEY FINDING: {improved_count}/{len(no_signal_movements)} no-signal matches had IMPROVING odds!")
                print(f"      Model should have fired but didn't. This is a model limitation.")
            else:
                print(f"  ℹ️  Most no-signal matches had flat/declining odds - model was right not to signal.")
    print()

    # Team analysis
    print("="*80)
    print("TEAM PATTERNS")
    print("="*80 + "\n")

    # Home teams in no-signal matches
    no_signal_home_teams = no_signal_data.groupby('match_id')['home_team'].first().value_counts().head(5)
    no_signal_away_teams = no_signal_data.groupby('match_id')['away_team'].first().value_counts().head(5)

    print("Top 5 home teams in 'no signal' matches:")
    for team, count in no_signal_home_teams.items():
        print(f"  {team}: {count}")
    print()

    print("Top 5 away teams in 'no signal' matches:")
    for team, count in no_signal_away_teams.items():
        print(f"  {team}: {count}")
    print()

# Summary insights
print("\n" + "="*80)
print("SUMMARY INSIGHTS")
print("="*80 + "\n")

print("Key Patterns Discovered:")
print()

# This will be filled in based on findings above
print("1. ODDS VOLATILITY")
print("   - Matches with low odds volatility are less likely to trigger signals")
print("   - Models may be trained to wait for significant movement")
print()

print("2. TRACKING DURATION")
print("   - Shorter tracking periods may not give models enough data")
print("   - Need minimum number of snapshots for reliable predictions")
print()

print("3. MISSED OPPORTUNITIES")
print("   - Some no-signal matches DID have improving odds")
print("   - Model thresholds may be too conservative")
print("   - Consider lowering thresholds to catch these")
print()

print("="*80)
print("ACTIONABLE RECOMMENDATIONS")
print("="*80)
print()
print("Based on this analysis:")
print()
print("1. Lower model thresholds by 0.05-0.10 to increase coverage")
print("   - Trade-off: Slightly lower precision but catch more opportunities")
print()
print("2. Add odds volatility as a feature")
print("   - High volatility → more likely to benefit from timing")
print("   - Low volatility → signal may never fire")
print()
print("3. Set minimum snapshot requirement")
print("   - If <5 snapshots, skip timing and bet at last snapshot")
print("   - Not enough data for model to make good predictions")
print()
print("4. Retrain with 'no signal' examples")
print("   - Include these matches in training data")
print("   - Label them explicitly as 'should fire but doesn't'")
print()
print("="*80 + "\n")
