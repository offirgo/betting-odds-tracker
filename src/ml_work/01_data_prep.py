#!/usr/bin/env python3
"""
ML Training - Step 1: Load and Explore the Data
This script loads data from the database and shows you what we're working with
"""

import pandas as pd
import sqlite3


def load_data(db_path='../../data/raw/epl_arbitrage.db'):
    """Load ML features from the database"""

    print("=" * 60)
    print("STEP 1: Loading Data from Database")
    print("=" * 60)

    # Connect to database
    conn = sqlite3.connect(db_path)

    # Load ALL data first (we'll split train/test later)
    print("\nLoading ml_features table...")
    df = pd.read_sql("SELECT * FROM ml_features", conn)
    conn.close()

    print(f"✓ Loaded {len(df)} rows")

    return df


def explore_data(df):
    """Look at the data to understand what we have"""

    print("\n" + "=" * 60)
    print("DATA EXPLORATION")
    print("=" * 60)

    # 1. How many matches and seasons?
    print("\n1. Dataset Overview:")
    print(f"   Total rows: {len(df)}")
    print(f"   Unique matches: {df['match_id'].nunique()}")
    print(f"   Seasons: {df['season'].unique()}")
    print(f"   Rows per season:")
    print(df['season'].value_counts().sort_index())

    # 2. What are we trying to predict?
    print("\n2. Target Variables (what we want to predict):")

    # 2a. Arbitrage existence
    arb_counts = df['will_have_future_arbitrage'].value_counts()
    print(f"\n   Will have arbitrage:")
    print(f"   - Yes (1): {arb_counts.get(1, 0)} rows ({100 * arb_counts.get(1, 0) / len(df):.1f}%)")
    print(f"   - No (0): {arb_counts.get(0, 0)} rows ({100 * arb_counts.get(0, 0) / len(df):.1f}%)")

    # 2b. Profit distribution
    arb_only = df[df['will_have_future_arbitrage'] == 1]
    if len(arb_only) > 0:
        print(f"\n   Profit when arbitrage exists:")
        print(f"   - Average: {arb_only['max_future_profit_percent'].mean():.2f}%")
        print(f"   - Min: {arb_only['max_future_profit_percent'].min():.2f}%")
        print(f"   - Max: {arb_only['max_future_profit_percent'].max():.2f}%")
        print(f"   - Median: {arb_only['max_future_profit_percent'].median():.2f}%")

    # 2c. Betting timing
    print(f"\n   Should bet now distribution:")
    print(
        f"   - Bet home now: {df['should_bet_home_now'].sum()} rows ({100 * df['should_bet_home_now'].sum() / len(df):.1f}%)")
    print(
        f"   - Bet draw now: {df['should_bet_draw_now'].sum()} rows ({100 * df['should_bet_draw_now'].sum() / len(df):.1f}%)")
    print(
        f"   - Bet away now: {df['should_bet_away_now'].sum()} rows ({100 * df['should_bet_away_now'].sum() / len(df):.1f}%)")

    # 3. Feature columns
    print("\n3. Available Features (input data for ML):")
    feature_cols = [col for col in df.columns if col not in [
        'feature_id', 'match_id', 'snapshot_time', 'created_at', 'season',
        'home_team', 'away_team',  # We have home_team_id/away_team_id instead
        # Target columns
        'should_bet_home_now', 'home_odds_will_improve', 'best_future_home_odds', 'snapshots_until_best_home',
        'should_bet_draw_now', 'draw_odds_will_improve', 'best_future_draw_odds', 'snapshots_until_best_draw',
        'should_bet_away_now', 'away_odds_will_improve', 'best_future_away_odds', 'snapshots_until_best_away',
        'will_have_future_arbitrage', 'max_future_profit_percent', 'snapshots_until_arbitrage'
    ]]

    print(f"   Total feature columns: {len(feature_cols)}")
    print("\n   Feature groups:")
    print("   - Current odds: home_odds_current, draw_odds_current, away_odds_current")
    print("   - Historical stats: *_min_historical, *_max_historical, *_mean_historical, *_std_historical")
    print("   - Trends: *_trend, *_change_recent")
    print("   - Context: days_before_match, home_team_id, away_team_id, num_snapshots_seen")

    # 4. Check for missing data
    print("\n4. Data Quality Check:")
    missing = df[feature_cols].isnull().sum()
    if missing.sum() > 0:
        print("   ⚠ Warning: Some columns have missing values:")
        print(missing[missing > 0])
    else:
        print("   ✓ No missing values in feature columns")

    return feature_cols


def show_sample_predictions(df):
    """Show what a prediction task looks like"""

    print("\n" + "=" * 60)
    print("EXAMPLE: What We're Trying to Learn")
    print("=" * 60)

    # Take one example row
    sample = df.iloc[0]

    print("\n📊 Example Snapshot:")
    print(f"   Match: {sample['home_team']} vs {sample['away_team']}")
    print(f"   Time: {sample['days_before_match']:.1f} days before match")
    print(f"   Season: {sample['season']}")

    print("\n📈 Current Odds:")
    print(f"   Home: {sample['home_odds_current']:.2f}")
    print(f"   Draw: {sample['draw_odds_current']:.2f}")
    print(f"   Away: {sample['away_odds_current']:.2f}")

    print("\n🎯 What We Want the Model to Predict:")
    print(f"   1. Will arbitrage exist? {bool(sample['will_have_future_arbitrage'])}")
    if sample['will_have_future_arbitrage']:
        print(f"   2. Expected profit: {sample['max_future_profit_percent']:.2f}%")
    print(f"   3. Bet home now? {bool(sample['should_bet_home_now'])}")
    print(f"   4. Bet draw now? {bool(sample['should_bet_draw_now'])}")
    print(f"   5. Bet away now? {bool(sample['should_bet_away_now'])}")

    print("\n💡 The model will learn patterns from features like:")
    print(f"   - Historical mean home odds: {sample['home_odds_mean_historical']:.2f}")
    print(f"   - Home odds trend: {sample['home_odds_trend']:.4f}")
    print(f"   - Snapshots seen so far: {int(sample['num_snapshots_seen'])}")


def main():
    """Main execution"""

    # Load data
    df = load_data()

    # Explore it
    feature_cols = explore_data(df)

    # Show example
    show_sample_predictions(df)

    print("\n" + "=" * 60)
    print("STEP 1 COMPLETE!")
    print("=" * 60)
    print("\nNext steps:")
    print("1. Split data into train/test sets (by season)")
    print("2. Prepare features and targets")
    print("3. Train the first model (arbitrage detection)")
    print("\nRun this script to see your data before we proceed to training.")


if __name__ == "__main__":
    main()