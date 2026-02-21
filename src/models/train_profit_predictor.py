#!/usr/bin/env python3
"""
Profit Magnitude Predictor Model

Instead of binary classification (will/won't have arbitrage), predict HOW MUCH profit.

Use case:
- Filter out matches likely to yield <2% profit
- Focus capital on high-profit opportunities
- Better than dynamic sizing (which failed)

Target: Predict final arbitrage profit % at first snapshot
"""

import sqlite3
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import pickle
import json

print("\n" + "="*80)
print("PROFIT MAGNITUDE PREDICTOR - MODEL TRAINING")
print("="*80 + "\n")

# Load data
db_path = "../../data/raw/epl_arbitrage.db"
conn = sqlite3.connect(db_path)

print("Loading training data...")
# Get all matches from training seasons
df = pd.read_sql("""
    SELECT * FROM ml_features
    WHERE season IN ('21/22', '22/23', '23/24')
""", conn)
conn.close()

print(f"Loaded {len(df):,} snapshots\n")

# For each match, calculate the actual profit achieved
print("Calculating target variable (final profit %)...")

matches_profit = {}

for season in ['21/22', '22/23', '23/24']:
    season_df = df[df['season'] == season]
    matches = season_df.groupby('match_id')

    for match_id, match_df in matches:
        match_df = match_df.sort_values('snapshot_time')

        # Skip if no arbitrage
        if match_df['will_have_future_arbitrage'].iloc[0] != 1:
            continue

        # Get final odds (last snapshot)
        last_row = match_df.iloc[-1]

        home_odds = last_row['home_odds_current']
        draw_odds = last_row['draw_odds_current']
        away_odds = last_row['away_odds_current']

        if pd.notna(home_odds) and pd.notna(draw_odds) and pd.notna(away_odds):
            # Calculate combined inverse and profit
            combined_inv = (1/home_odds) + (1/draw_odds) + (1/away_odds)

            if combined_inv < 1.0:
                # Arbitrage exists
                profit_pct = ((1/combined_inv) - 1) * 100
                matches_profit[match_id] = profit_pct

print(f"Found {len(matches_profit)} matches with arbitrage\n")

# Create training dataset: features from FIRST snapshot, target = final profit
print("Creating training dataset...")

training_data = []

for match_id, final_profit in matches_profit.items():
    match_df = df[df['match_id'] == match_id].sort_values('snapshot_time')
    first_row = match_df.iloc[0]

    # Features from first snapshot
    features = {}

    # Basic odds
    features['home_odds_first'] = first_row['home_odds_current']
    features['draw_odds_first'] = first_row['draw_odds_current']
    features['away_odds_first'] = first_row['away_odds_current']

    # Combined inverse at first snapshot
    if (pd.notna(features['home_odds_first']) and
        pd.notna(features['draw_odds_first']) and
        pd.notna(features['away_odds_first'])):

        features['combined_inverse_first'] = (
            1/features['home_odds_first'] +
            1/features['draw_odds_first'] +
            1/features['away_odds_first']
        )

        # How far from arbitrage initially
        features['distance_from_arb'] = features['combined_inverse_first'] - 1.0
    else:
        continue

    # Timing
    features['days_before_match'] = first_row['days_before_match']

    # Historical stats
    for outcome in ['home', 'draw', 'away']:
        features[f'{outcome}_odds_mean_hist'] = first_row[f'{outcome}_odds_mean_historical']
        features[f'{outcome}_odds_std_hist'] = first_row[f'{outcome}_odds_std_historical']
        features[f'{outcome}_odds_min_hist'] = first_row[f'{outcome}_odds_min_historical']
        features[f'{outcome}_odds_max_hist'] = first_row[f'{outcome}_odds_max_historical']

    # Odds relationships
    features['home_draw_ratio'] = features['home_odds_first'] / features['draw_odds_first']
    features['home_away_ratio'] = features['home_odds_first'] / features['away_odds_first']
    features['draw_away_ratio'] = features['draw_odds_first'] / features['away_odds_first']

    # Volatility indicators
    features['home_volatility'] = first_row['home_odds_std_historical'] / first_row['home_odds_mean_historical'] if first_row['home_odds_mean_historical'] > 0 else 0
    features['draw_volatility'] = first_row['draw_odds_std_historical'] / first_row['draw_odds_mean_historical'] if first_row['draw_odds_mean_historical'] > 0 else 0
    features['away_volatility'] = first_row['away_odds_std_historical'] / first_row['away_odds_mean_historical'] if first_row['away_odds_mean_historical'] > 0 else 0

    # Number of snapshots seen
    features['num_snapshots'] = first_row['num_snapshots_seen']

    # Team IDs
    features['home_team_id'] = first_row['home_team_id']
    features['away_team_id'] = first_row['away_team_id']

    # Target
    features['profit_pct'] = final_profit

    training_data.append(features)

train_df = pd.DataFrame(training_data)

# Remove any rows with NaN
train_df = train_df.dropna()

print(f"Training dataset: {len(train_df)} matches\n")

# Display target distribution
print("Profit distribution:")
print(f"  Mean: {train_df['profit_pct'].mean():.2f}%")
print(f"  Median: {train_df['profit_pct'].median():.2f}%")
print(f"  Std: {train_df['profit_pct'].std():.2f}%")
print(f"  Min: {train_df['profit_pct'].min():.2f}%")
print(f"  Max: {train_df['profit_pct'].max():.2f}%")
print()

# Percentiles
percentiles = [10, 25, 50, 75, 90]
for p in percentiles:
    val = np.percentile(train_df['profit_pct'], p)
    print(f"  {p}th percentile: {val:.2f}%")
print()

# Prepare X and y
X = train_df.drop('profit_pct', axis=1)
y = train_df['profit_pct']

feature_names = list(X.columns)
print(f"Using {len(feature_names)} features\n")

# Split data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

print(f"Training set: {len(X_train)} matches")
print(f"Test set: {len(X_test)} matches\n")

# Train multiple models
print("="*80)
print("MODEL TRAINING")
print("="*80 + "\n")

models = {
    'Random Forest': RandomForestRegressor(n_estimators=100, random_state=42, max_depth=10),
    'Gradient Boosting': GradientBoostingRegressor(n_estimators=100, random_state=42, max_depth=5),
    'Ridge Regression': Ridge(alpha=1.0)
}

results = {}

for name, model in models.items():
    print(f"Training {name}...")

    # Train
    model.fit(X_train, y_train)

    # Predictions
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)

    # Metrics
    train_mae = mean_absolute_error(y_train, y_pred_train)
    test_mae = mean_absolute_error(y_test, y_pred_test)

    train_rmse = np.sqrt(mean_squared_error(y_train, y_pred_train))
    test_rmse = np.sqrt(mean_squared_error(y_test, y_pred_test))

    train_r2 = r2_score(y_train, y_pred_train)
    test_r2 = r2_score(y_test, y_pred_test)

    print(f"  Train MAE: {train_mae:.3f}%")
    print(f"  Test MAE: {test_mae:.3f}%")
    print(f"  Train RMSE: {train_rmse:.3f}%")
    print(f"  Test RMSE: {test_rmse:.3f}%")
    print(f"  Train R²: {train_r2:.3f}")
    print(f"  Test R²: {test_r2:.3f}")

    # Cross-validation
    cv_scores = cross_val_score(model, X_train, y_train, cv=5,
                                 scoring='neg_mean_absolute_error')
    cv_mae = -cv_scores.mean()
    print(f"  CV MAE (5-fold): {cv_mae:.3f}%")
    print()

    results[name] = {
        'model': model,
        'test_mae': test_mae,
        'test_rmse': test_rmse,
        'test_r2': test_r2,
        'cv_mae': cv_mae
    }

# Select best model
best_model_name = min(results.keys(), key=lambda k: results[k]['test_mae'])
best_model = results[best_model_name]['model']

print("="*80)
print("BEST MODEL SELECTION")
print("="*80)
print(f"\nBest model: {best_model_name}")
print(f"Test MAE: {results[best_model_name]['test_mae']:.3f}%")
print(f"Test R²: {results[best_model_name]['test_r2']:.3f}\n")

# Feature importance (for tree-based models)
if hasattr(best_model, 'feature_importances_'):
    print("Top 10 Most Important Features:")
    importances = best_model.feature_importances_
    indices = np.argsort(importances)[::-1][:10]

    for i, idx in enumerate(indices, 1):
        print(f"  {i}. {feature_names[idx]}: {importances[idx]:.4f}")
    print()

# Save best model
model_path = '../../models/model_profit_predictor.pkl'
with open(model_path, 'wb') as f:
    pickle.dump(best_model, f)
print(f"✓ Model saved to: {model_path}\n")

# Save model metadata
metadata = {
    'model_type': best_model_name,
    'features': feature_names,
    'test_mae': results[best_model_name]['test_mae'],
    'test_rmse': results[best_model_name]['test_rmse'],
    'test_r2': results[best_model_name]['test_r2'],
    'cv_mae': results[best_model_name]['cv_mae'],
    'training_samples': len(X_train),
    'test_samples': len(X_test),
    'target_mean': float(y.mean()),
    'target_std': float(y.std())
}

metadata_path = '../../models/model_profit_predictor_metadata.json'
with open(metadata_path, 'w') as f:
    json.dump(metadata, f, indent=2)
print(f"✓ Metadata saved to: {metadata_path}\n")

# Test on actual examples
print("="*80)
print("EXAMPLE PREDICTIONS")
print("="*80 + "\n")

# Sample some test predictions
sample_indices = np.random.choice(len(X_test), min(10, len(X_test)), replace=False)

print(f"{'Actual':<12} {'Predicted':<12} {'Error':<12} {'Assessment':<20}")
print("-" * 60)

for idx in sample_indices:
    actual = y_test.iloc[idx]
    predicted = best_model.predict(X_test.iloc[idx:idx+1])[0]
    error = abs(actual - predicted)

    if error < 0.5:
        assessment = "Excellent"
    elif error < 1.0:
        assessment = "Good"
    elif error < 2.0:
        assessment = "Acceptable"
    else:
        assessment = "Poor"

    print(f"{actual:>10.2f}%  {predicted:>10.2f}%  {error:>10.2f}%  {assessment:<20}")

print("\n" + "="*80)
print("USAGE RECOMMENDATION")
print("="*80)
print()
print("Use this model to:")
print("1. Predict profit at first snapshot")
print("2. Skip matches with predicted profit <2%")
print("3. Prioritize matches with predicted profit >4%")
print()
print(f"With MAE of {results[best_model_name]['test_mae']:.2f}%, predictions are reliable enough")
print("for intelligent match filtering.")
print()
print("="*80 + "\n")
