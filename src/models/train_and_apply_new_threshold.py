#!/usr/bin/env python3
"""
Train models and apply new threshold to database in one go.

This script avoids pickle loading issues by training models fresh
and applying the new threshold immediately.
"""

import sqlite3
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import sys

def load_and_train_models():
    """Load prepared data and train all three timing models."""
    print("\n" + "="*80)
    print("TRAINING TIMING MODELS WITH NEW THRESHOLD")
    print("="*80 + "\n")

    data_dir = '../../data/prepared'

    print("📂 Loading prepared data...")
    X_train_full = pd.read_csv(f'{data_dir}/features_train.csv')
    X_test = pd.read_csv(f'{data_dir}/features_test.csv')

    models = {}

    # Train Home timing model
    print("\n1️⃣  Training Home timing model...")
    y_train_home = pd.read_csv(f'{data_dir}/target_train_bet_home_timing.csv')['target']
    X_train, X_val, y_train, y_val = train_test_split(
        X_train_full, y_train_home, test_size=0.2, random_state=42, stratify=y_train_home
    )

    model_home = RandomForestClassifier(
        n_estimators=300,
        max_depth=8,
        min_samples_split=50,
        min_samples_leaf=20,
        class_weight='balanced',
        random_state=42,
        n_jobs=-1
    )
    model_home.fit(X_train, y_train)
    models['home'] = model_home
    print("   ✓ Home model trained")

    # Train Draw timing model
    print("\n2️⃣  Training Draw timing model...")
    y_train_draw = pd.read_csv(f'{data_dir}/target_train_bet_draw_timing.csv')['target']
    X_train, X_val, y_train, y_val = train_test_split(
        X_train_full, y_train_draw, test_size=0.2, random_state=42, stratify=y_train_draw
    )

    model_draw = RandomForestClassifier(
        n_estimators=300,
        max_depth=8,
        min_samples_split=50,
        min_samples_leaf=20,
        class_weight='balanced',
        random_state=42,
        n_jobs=-1
    )
    model_draw.fit(X_train, y_train)
    models['draw'] = model_draw
    print("   ✓ Draw model trained")

    # Train Away timing model
    print("\n3️⃣  Training Away timing model...")
    y_train_away = pd.read_csv(f'{data_dir}/target_train_bet_away_timing.csv')['target']
    X_train, X_val, y_train, y_val = train_test_split(
        X_train_full, y_train_away, test_size=0.2, random_state=42, stratify=y_train_away
    )

    model_away = RandomForestClassifier(
        n_estimators=300,
        max_depth=8,
        min_samples_split=50,
        min_samples_leaf=20,
        class_weight='balanced',
        random_state=42,
        n_jobs=-1
    )
    model_away.fit(X_train, y_train)
    models['away'] = model_away
    print("   ✓ Away model trained")

    return models, X_train_full.columns.tolist()


def apply_threshold_to_database(models, feature_cols, threshold, suffix):
    """Apply new threshold to all rows in ml_features table."""

    print(f"\n" + "="*80)
    print(f"APPLYING THRESHOLD {threshold} TO DATABASE")
    print("="*80 + "\n")

    db_path = "../../data/raw/epl_arbitrage.db"
    print(f"Connecting to database: {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Check if columns exist
    cursor.execute("PRAGMA table_info(ml_features)")
    existing_columns = [row[1] for row in cursor.fetchall()]

    new_columns = [
        f'should_bet_home_now_{suffix}',
        f'should_bet_draw_now_{suffix}',
        f'should_bet_away_now_{suffix}'
    ]

    # Add new columns if they don't exist
    for col in new_columns:
        if col not in existing_columns:
            print(f"Adding column: {col}")
            cursor.execute(f"ALTER TABLE ml_features ADD COLUMN {col} INTEGER DEFAULT 0")

    conn.commit()
    print()

    # Load data
    print("Loading ml_features data...")
    df = pd.read_sql("SELECT * FROM ml_features", conn)
    print(f"Loaded {len(df):,} rows\n")

    # Get feature columns
    label_cols = ['will_have_future_arbitrage', 'should_bet_home_now', 'should_bet_draw_now',
                  'should_bet_away_now'] + new_columns
    meta_cols = ['match_id', 'season', 'home_team', 'away_team', 'snapshot_time']

    # Ensure we only use features that exist in both the model and database
    available_features = [col for col in feature_cols if col in df.columns]
    print(f"Using {len(available_features)} features for predictions\n")

    # Generate predictions
    print("Generating predictions with new threshold...")
    print(f"Threshold: {threshold}\n")

    total_rows = len(df)
    update_interval = max(1, total_rows // 20)  # Update every 5%

    for outcome in ['home', 'draw', 'away']:
        print(f"Processing {outcome} outcome...")
        model = models[outcome]
        col_name = f'should_bet_{outcome}_now_{suffix}'

        predictions = []

        for idx, row in df.iterrows():
            if idx % update_interval == 0:
                pct = (idx / total_rows) * 100
                print(f"  Progress: {pct:.0f}% ({idx:,}/{total_rows:,})", end='\r')

            try:
                # Prepare features
                features = row[available_features].values.reshape(1, -1)

                # Skip if NaN values
                if pd.isna(features).any():
                    predictions.append(0)
                    continue

                # Get prediction probability
                proba = model.predict_proba(features)[0][1]

                # Apply threshold
                prediction = 1 if proba >= threshold else 0
                predictions.append(prediction)

            except Exception as e:
                predictions.append(0)

        # Update dataframe
        df[col_name] = predictions

        signal_count = sum(predictions)
        signal_pct = (signal_count / len(predictions)) * 100
        print(f"  Complete: {signal_count:,} signals ({signal_pct:.1f}%)" + " " * 20)

    print()

    # Update database
    print("Updating database...")
    for outcome in ['home', 'draw', 'away']:
        col_name = f'should_bet_{outcome}_now_{suffix}'
        print(f"  Writing {col_name}...")

        # Update in batches for efficiency
        batch_size = 1000
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]

            for _, row in batch.iterrows():
                cursor.execute(f"""
                    UPDATE ml_features
                    SET {col_name} = ?
                    WHERE match_id = ? AND snapshot_time = ?
                """, (int(row[col_name]), row['match_id'], row['snapshot_time']))

            if i % (batch_size * 10) == 0:
                conn.commit()  # Commit every 10 batches

        conn.commit()

    print()
    print("✓ Database updated successfully!\n")

    # Summary statistics
    print("="*80)
    print("SUMMARY STATISTICS")
    print("="*80 + "\n")

    print(f"Threshold: {threshold}\n")

    # Compare with original labels
    print(f"{'Outcome':<10} {'Original (0.55)':<20} {'New ({threshold})':<20} {'Change':<15}")
    print("-" * 70)

    for outcome in ['home', 'draw', 'away']:
        orig_col = f'should_bet_{outcome}_now'
        new_col = f'should_bet_{outcome}_now_{suffix}'

        orig_count = df[orig_col].sum()
        new_count = df[new_col].sum()
        change = new_count - orig_count
        change_pct = (change / orig_count * 100) if orig_count > 0 else 0

        orig_pct = (orig_count / len(df)) * 100
        new_pct = (new_count / len(df)) * 100

        print(f"{outcome.capitalize():<10} {orig_count:>6} ({orig_pct:>5.1f}%)      "
              f"{new_count:>6} ({new_pct:>5.1f}%)      {change:>+6} ({change_pct:>+5.1f}%)")

    print()
    conn.close()

    print("="*80)
    print("COMPLETE!")
    print("="*80 + "\n")


if __name__ == "__main__":
    # Get threshold from command line or use default
    threshold = 0.50
    if len(sys.argv) > 1:
        try:
            threshold = float(sys.argv[1])
        except ValueError:
            print(f"Invalid threshold: {sys.argv[1]}")
            print("Usage: python train_and_apply_new_threshold.py [threshold]")
            print("Example: python train_and_apply_new_threshold.py 0.50")
            sys.exit(1)

    suffix = f"t{int(threshold * 100):03d}"

    print(f"\nGenerating labels with threshold: {threshold}")
    print(f"Column suffix: {suffix}\n")

    # Train models
    models, feature_cols = load_and_train_models()

    # Apply to database
    apply_threshold_to_database(models, feature_cols, threshold, suffix)

    print("✓ Labels generated successfully!")
    print(f"\nYou can now run simulations with both:")
    print(f"  - Original (0.55): should_bet_*_now")
    print(f"  - New ({threshold}): should_bet_*_now_{suffix}")
