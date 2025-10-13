#!/usr/bin/env python3
"""
Step 2: Prepare Train and Test Data Sets
We'll separate data by season so the model trains on old data and tests on new data
"""

import pandas as pd
import sqlite3


def load_data(db_path='../../data/raw/epl_arbitrage.db'):
    """Load ML features from the database"""
    print("Loading data from database...")
    conn = sqlite3.connect(db_path)
    df = pd.read_sql("SELECT * FROM ml_features", conn)
    conn.close()
    print(f"✓ Loaded {len(df)} rows\n")
    return df


def split_train_test(df, test_season='24/25'):
    """
    Split data into training and testing sets by season

    Args:
        df: DataFrame with all data
        test_season: Which season to hold out for testing

    Returns:
        train_df, test_df
    """

    print("=" * 60)
    print("STEP 2: Splitting Data into Train and Test Sets")
    print("=" * 60)

    # Important: We split by SEASON, not randomly
    # Why? Because we want to simulate real-world usage:
    # - Train on past seasons (what we know)
    # - Test on future season (what we're predicting)

    print(f"\n📚 Training Set: All seasons EXCEPT {test_season}")
    print(f"🧪 Test Set: Season {test_season} only")

    train_df = df[df['season'] != test_season].copy()
    test_df = df[df['season'] == test_season].copy()

    print(f"\n✓ Training data: {len(train_df)} rows from {train_df['season'].nunique()} seasons")
    print(f"  Seasons: {sorted(train_df['season'].unique())}")
    print(f"  Unique matches: {train_df['match_id'].nunique()}")

    print(f"\n✓ Test data: {len(test_df)} rows from 1 season")
    print(f"  Season: {test_season}")
    print(f"  Unique matches: {test_df['match_id'].nunique()}")

    # Important check: Make sure we have both train and test data
    if len(train_df) == 0:
        print("\n❌ ERROR: No training data! Check your season values.")
        return None, None

    if len(test_df) == 0:
        print(f"\n❌ ERROR: No test data for season {test_season}!")
        return None, None

    # Check class balance
    print("\n" + "=" * 60)
    print("DATA BALANCE CHECK")
    print("=" * 60)

    print("\n📊 Arbitrage Distribution:")
    train_arb = train_df['will_have_future_arbitrage'].value_counts()
    test_arb = test_df['will_have_future_arbitrage'].value_counts()

    print("\nTraining set:")
    print(f"  Has arbitrage: {train_arb.get(1, 0)} ({100 * train_arb.get(1, 0) / len(train_df):.1f}%)")
    print(f"  No arbitrage: {train_arb.get(0, 0)} ({100 * train_arb.get(0, 0) / len(train_df):.1f}%)")

    print("\nTest set:")
    print(f"  Has arbitrage: {test_arb.get(1, 0)} ({100 * test_arb.get(1, 0) / len(test_df):.1f}%)")
    print(f"  No arbitrage: {test_arb.get(0, 0)} ({100 * test_arb.get(0, 0) / len(test_df):.1f}%)")

    # Warning if severely imbalanced
    train_imbalance = max(train_arb.get(0, 0), train_arb.get(1, 0)) / len(train_df)
    if train_imbalance > 0.95:
        print("\n⚠️  WARNING: Data is highly imbalanced (>95% one class)")
        print("   This might make the model biased. We'll handle this in training.")

    return train_df, test_df


def prepare_features_and_targets(train_df, test_df):
    """
    Separate features (X) from targets (y)

    Features = Input data the model sees
    Targets = What we want the model to predict
    """

    print("\n" + "=" * 60)
    print("PREPARING FEATURES AND TARGETS")
    print("=" * 60)

    # Define which columns are features (inputs)
    # These are the columns the model will use to make predictions
    feature_columns = [
        # Context
        'days_before_match',
        'hours_before_match',
        'home_team_id',
        'away_team_id',
        'num_snapshots_seen',

        # Current odds
        'home_odds_current',
        'draw_odds_current',
        'away_odds_current',
        'combined_inverse_current',

        # Historical aggregates - Home
        'home_odds_min_historical',
        'home_odds_max_historical',
        'home_odds_mean_historical',
        'home_odds_std_historical',
        'home_odds_trend',

        # Historical aggregates - Draw
        'draw_odds_min_historical',
        'draw_odds_max_historical',
        'draw_odds_mean_historical',
        'draw_odds_std_historical',
        'draw_odds_trend',

        # Historical aggregates - Away
        'away_odds_min_historical',
        'away_odds_max_historical',
        'away_odds_mean_historical',
        'away_odds_std_historical',
        'away_odds_trend',

        # Recent changes
        'home_odds_change_recent',
        'draw_odds_change_recent',
        'away_odds_change_recent',
    ]

    # Define target columns (what we're predicting)
    target_columns = {
        'arbitrage_exists': 'will_have_future_arbitrage',
        'profit_amount': 'max_future_profit_percent',
        'bet_home_now': 'should_bet_home_now',
        'bet_draw_now': 'should_bet_draw_now',
        'bet_away_now': 'should_bet_away_now',
    }

    print(f"\n✓ Selected {len(feature_columns)} features (input columns)")
    print("\nFeature categories:")
    print("  - Context: 5 features (time, teams, history length)")
    print("  - Current odds: 4 features")
    print("  - Historical stats: 15 features (5 per outcome)")
    print("  - Recent changes: 3 features")

    print(f"\n✓ We have {len(target_columns)} different prediction targets")
    for name, col in target_columns.items():
        print(f"  - {name}: predicts '{col}'")

    # Extract features (X) and targets (y) for training set
    X_train = train_df[feature_columns].copy()
    y_train = {name: train_df[col].copy() for name, col in target_columns.items()}

    # Extract features (X) and targets (y) for test set
    X_test = test_df[feature_columns].copy()
    y_test = {name: test_df[col].copy() for name, col in target_columns.items()}

    # Check for missing values
    print("\n" + "=" * 60)
    print("DATA QUALITY CHECK")
    print("=" * 60)

    missing_train = X_train.isnull().sum().sum()
    missing_test = X_test.isnull().sum().sum()

    if missing_train > 0 or missing_test > 0:
        print(f"\n⚠️  Found missing values:")
        print(f"  Training set: {missing_train} missing values")
        print(f"  Test set: {missing_test} missing values")
        print("\n  We'll need to handle these before training (fill with 0 or mean)")

        # Show which columns have missing values
        if missing_train > 0:
            print("\n  Training set missing values by column:")
            missing_cols = X_train.isnull().sum()
            print(missing_cols[missing_cols > 0])
    else:
        print("\n✓ No missing values found!")

    return X_train, X_test, y_train, y_test, feature_columns


def save_prepared_data(X_train, X_test, y_train, y_test, output_dir='../../data/prepared'):
    """
    Save prepared data to files so we don't have to run this again
    """
    import os

    print("\n" + "=" * 60)
    print("SAVING PREPARED DATA")
    print("=" * 60)

    os.makedirs(output_dir, exist_ok=True)

    # Save features with descriptive names
    X_train.to_csv(f'{output_dir}/features_train.csv', index=False)
    X_test.to_csv(f'{output_dir}/features_test.csv', index=False)

    # Save each target with descriptive names
    target_name_map = {
        'arbitrage_exists': 'target_train_arbitrage_exists.csv',
        'profit_amount': 'target_train_profit_percent.csv',
        'bet_home_now': 'target_train_bet_home_timing.csv',
        'bet_draw_now': 'target_train_bet_draw_timing.csv',
        'bet_away_now': 'target_train_bet_away_timing.csv',
    }

    for name, filename in target_name_map.items():
        y_train[name].to_csv(f'{output_dir}/{filename}', index=False, header=['target'])

    # Save test targets
    target_test_map = {
        'arbitrage_exists': 'target_test_arbitrage_exists.csv',
        'profit_amount': 'target_test_profit_percent.csv',
        'bet_home_now': 'target_test_bet_home_timing.csv',
        'bet_draw_now': 'target_test_bet_draw_timing.csv',
        'bet_away_now': 'target_test_bet_away_timing.csv',
    }

    for name, filename in target_test_map.items():
        y_test[name].to_csv(f'{output_dir}/{filename}', index=False, header=['target'])

    print(f"\n✓ Saved prepared data to: {output_dir}/")
    print(f"\n  Features:")
    print(f"    - features_train.csv ({X_train.shape[0]} rows × {X_train.shape[1]} columns)")
    print(f"    - features_test.csv ({X_test.shape[0]} rows × {X_test.shape[1]} columns)")
    print(f"\n  Training Targets:")
    print(f"    - target_train_arbitrage_exists.csv")
    print(f"    - target_train_profit_percent.csv")
    print(f"    - target_train_bet_home_timing.csv")
    print(f"    - target_train_bet_draw_timing.csv")
    print(f"    - target_train_bet_away_timing.csv")
    print(f"\n  Test Targets:")
    print(f"    - target_test_arbitrage_exists.csv")
    print(f"    - target_test_profit_percent.csv")
    print(f"    - target_test_bet_home_timing.csv")
    print(f"    - target_test_bet_draw_timing.csv")
    print(f"    - target_test_bet_away_timing.csv")


def main():
    """Main execution"""

    # Load data
    df = load_data()

    # Split into train/test by season
    train_df, test_df = split_train_test(df, test_season='24/25')

    if train_df is None:
        return

    # Prepare features and targets
    X_train, X_test, y_train, y_test, feature_columns = prepare_features_and_targets(train_df, test_df)

    # Save for next steps
    save_prepared_data(X_train, X_test, y_train, y_test)

    print("\n" + "=" * 60)
    print("STEP 2 COMPLETE!")
    print("=" * 60)
    print("\n✓ Data is split and ready for training")
    print("✓ Saved prepared data to files")
    print("\nNext step:")
    print("  Train Model 1: Arbitrage Detection (will arbitrage exist?)")


if __name__ == "__main__":
    main()