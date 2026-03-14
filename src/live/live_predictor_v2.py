#!/usr/bin/env python3
"""
Live Prediction Engine V2

Trains models fresh and makes predictions on live data using correct features.
Tracks historical odds snapshots to build required features.
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from datetime import datetime, timezone
import json
import os


class LivePredictorV2:
    """Prediction engine that trains fresh models and tracks match history."""

    def __init__(self, data_dir='../../data/prepared'):
        """
        Initialize predictor by training fresh models.

        Args:
            data_dir: Directory with prepared training data
        """
        self.data_dir = data_dir
        self.models = {}
        self.thresholds = {
            'home': 0.55,
            'draw': 0.575,
            'away': 0.35
        }
        self.feature_columns = None
        self.historical_snapshots = {}  # Track snapshots per match
        self.team_mapping = {}  # Map team names to IDs
        self.next_team_id = 1

        # Train models on initialization
        self.train_models()

    def train_models(self):
        """Train timing models fresh to avoid pickle compatibility issues."""
        print("\n" + "="*70)
        print("TRAINING FRESH ML MODELS")
        print("="*70 + "\n")

        # Load prepared training data
        print("Loading training data...")
        X_train_full = pd.read_csv(f'{self.data_dir}/features_train.csv')
        self.feature_columns = X_train_full.columns.tolist()
        print(f"  Features: {len(self.feature_columns)} columns")
        print(f"  Samples: {len(X_train_full):,} rows\n")

        # Train each timing model
        for outcome in ['home', 'draw', 'away']:
            print(f"Training {outcome} timing model...")

            # Load target
            target_file = f'{self.data_dir}/target_train_bet_{outcome}_timing.csv'
            y_train = pd.read_csv(target_file)['target']

            # Split for validation
            X_train, X_val, y_train_split, y_val = train_test_split(
                X_train_full, y_train, test_size=0.2, random_state=42, stratify=y_train
            )

            # Train Random Forest
            model = RandomForestClassifier(
                n_estimators=300,
                max_depth=8,
                min_samples_split=50,
                min_samples_leaf=20,
                class_weight='balanced',
                random_state=42,
                n_jobs=-1
            )
            model.fit(X_train, y_train_split)

            # Evaluate on validation set
            val_acc = model.score(X_val, y_val)
            y_pred_proba = model.predict_proba(X_val)[:, 1]

            # Count signals at threshold
            signals = (y_pred_proba >= self.thresholds[outcome]).sum()
            signal_pct = (signals / len(y_val)) * 100

            self.models[outcome] = model

            print(f"  ✓ Accuracy: {val_acc:.3f}")
            print(f"  ✓ Threshold: {self.thresholds[outcome]:.3f}")
            print(f"  ✓ Signals: {signals} ({signal_pct:.1f}%)\n")

        print("="*70)
        print("✓ ALL MODELS TRAINED SUCCESSFULLY")
        print("="*70 + "\n")

    def get_or_create_team_id(self, team_name):
        """Map team name to consistent ID."""
        if team_name not in self.team_mapping:
            self.team_mapping[team_name] = self.next_team_id
            self.next_team_id += 1
        return self.team_mapping[team_name]

    def save_snapshot(self, match):
        """Save current odds snapshot for historical tracking."""
        match_id = match['match_id']

        if match_id not in self.historical_snapshots:
            self.historical_snapshots[match_id] = []

        snapshot = {
            'snapshot_time': datetime.now(timezone.utc).isoformat(),
            'snapshot_time_unix': int(datetime.now(timezone.utc).timestamp()),
            'home_odds': match['best_odds']['home'],
            'draw_odds': match['best_odds']['draw'],
            'away_odds': match['best_odds']['away'],
            'combined_inverse': (1/match['best_odds']['home'] +
                                1/match['best_odds']['draw'] +
                                1/match['best_odds']['away'])
        }

        self.historical_snapshots[match_id].append(snapshot)

    def extract_features(self, match):
        """
        Extract ML features matching training data format.

        Args:
            match: Current match with odds

        Returns:
            DataFrame with features, or None if insufficient data
        """
        match_id = match['match_id']
        best = match['best_odds']

        # Save current snapshot
        self.save_snapshot(match)

        # Get historical snapshots for this match
        snapshots = self.historical_snapshots.get(match_id, [])

        # Need at least 2 snapshots for trends
        if len(snapshots) < 2:
            return None

        # Calculate time features
        commence_time = datetime.fromisoformat(match['commence_time'].replace('Z', '+00:00'))
        snapshot_time = datetime.now(timezone.utc)
        time_diff = (commence_time - snapshot_time).total_seconds()
        days_before = time_diff / 86400
        hours_before = time_diff / 3600

        # Team IDs
        home_team_id = self.get_or_create_team_id(match['home_team'])
        away_team_id = self.get_or_create_team_id(match['away_team'])

        # Extract odds history
        home_odds_history = [s['home_odds'] for s in snapshots]
        draw_odds_history = [s['draw_odds'] for s in snapshots]
        away_odds_history = [s['away_odds'] for s in snapshots]

        # Current odds
        home_odds_current = best['home']
        draw_odds_current = best['draw']
        away_odds_current = best['away']
        combined_inverse_current = (1/home_odds_current + 1/draw_odds_current + 1/away_odds_current)

        # Historical statistics
        features = {
            # Time features
            'days_before_match': days_before,
            'hours_before_match': hours_before,

            # Team features
            'home_team_id': home_team_id,
            'away_team_id': away_team_id,

            # Tracking features
            'num_snapshots_seen': len(snapshots),

            # Current odds
            'home_odds_current': home_odds_current,
            'draw_odds_current': draw_odds_current,
            'away_odds_current': away_odds_current,
            'combined_inverse_current': combined_inverse_current,

            # Home odds statistics
            'home_odds_min_historical': np.min(home_odds_history),
            'home_odds_max_historical': np.max(home_odds_history),
            'home_odds_mean_historical': np.mean(home_odds_history),
            'home_odds_std_historical': np.std(home_odds_history) if len(home_odds_history) > 1 else 0,
            'home_odds_trend': home_odds_history[-1] - home_odds_history[0],

            # Draw odds statistics
            'draw_odds_min_historical': np.min(draw_odds_history),
            'draw_odds_max_historical': np.max(draw_odds_history),
            'draw_odds_mean_historical': np.mean(draw_odds_history),
            'draw_odds_std_historical': np.std(draw_odds_history) if len(draw_odds_history) > 1 else 0,
            'draw_odds_trend': draw_odds_history[-1] - draw_odds_history[0],

            # Away odds statistics
            'away_odds_min_historical': np.min(away_odds_history),
            'away_odds_max_historical': np.max(away_odds_history),
            'away_odds_mean_historical': np.mean(away_odds_history),
            'away_odds_std_historical': np.std(away_odds_history) if len(away_odds_history) > 1 else 0,
            'away_odds_trend': away_odds_history[-1] - away_odds_history[0],

            # Recent changes (last snapshot to current)
            'home_odds_change_recent': home_odds_history[-1] - home_odds_history[-2] if len(home_odds_history) >= 2 else 0,
            'draw_odds_change_recent': draw_odds_history[-1] - draw_odds_history[-2] if len(draw_odds_history) >= 2 else 0,
            'away_odds_change_recent': away_odds_history[-1] - away_odds_history[-2] if len(away_odds_history) >= 2 else 0,
        }

        return pd.DataFrame([features])

    def analyze_match(self, match):
        """
        Analyze match and generate betting recommendations.

        Args:
            match: Current match with odds

        Returns:
            Dict with analysis and recommendations
        """
        match_id = match['match_id']
        best = match['best_odds']

        # Extract features
        features_df = self.extract_features(match)

        # Initialize analysis
        analysis = {
            'match_id': match_id,
            'home_team': match['home_team'],
            'away_team': match['away_team'],
            'commence_time': match['commence_time'],
            'current_odds': {
                'home': best['home'],
                'draw': best['draw'],
                'away': best['away']
            },
            'snapshots_count': len(self.historical_snapshots.get(match_id, [])),
            'timing_predictions': None,
            'recommendation': None
        }

        # Check if we can make predictions yet
        if features_df is None:
            analysis['recommendation'] = {
                'action': 'TRACKING',
                'reason': 'Insufficient historical data - need at least 2 snapshots',
                'next_steps': f'Currently have {len(self.historical_snapshots.get(match_id, []))} snapshot(s). Keep tracking.'
            }
            return analysis

        # Ensure features match training columns
        missing_cols = set(self.feature_columns) - set(features_df.columns)
        if missing_cols:
            # Add missing columns with zeros
            for col in missing_cols:
                features_df[col] = 0

        # Reorder to match training
        features_df = features_df[self.feature_columns]

        # Run timing predictions
        timing_predictions = {}
        for outcome in ['home', 'draw', 'away']:
            model = self.models[outcome]
            threshold = self.thresholds[outcome]

            # Get prediction
            proba = model.predict_proba(features_df)[0][1]
            should_bet = proba >= threshold

            timing_predictions[outcome] = {
                'probability': float(proba),
                'threshold': float(threshold),
                'should_bet_now': bool(should_bet),
                'confidence': 'HIGH' if proba >= threshold + 0.1 else ('MEDIUM' if proba >= threshold else 'LOW')
            }

        analysis['timing_predictions'] = timing_predictions

        # Generate recommendation based on YOUR strategy
        recommendation = self.generate_recommendation(best, timing_predictions)
        analysis['recommendation'] = recommendation

        return analysis

    def generate_recommendation(self, best_odds, timing_predictions):
        """
        Generate recommendation using YOUR betting strategy:
        1. Bet on 2 highest odds immediately
        2. Wait for timing model signal for 3rd bet (lowest odds)
        """
        # Sort outcomes by odds (highest to lowest)
        sorted_outcomes = sorted([
            ('home', best_odds['home']),
            ('draw', best_odds['draw']),
            ('away', best_odds['away'])
        ], key=lambda x: x[1], reverse=True)

        high_1_name, high_1_odds = sorted_outcomes[0]
        high_2_name, high_2_odds = sorted_outcomes[1]
        low_name, low_odds = sorted_outcomes[2]

        # Check timing signal for the LOW odds outcome (the closing bet)
        low_prediction = timing_predictions[low_name]
        should_bet_third = low_prediction['should_bet_now']

        if should_bet_third:
            return {
                'action': 'BET_ALL_THREE',
                'reason': f'Timing model signals to complete arbitrage with {low_name.upper()} bet',
                'strategy': {
                    'bet_1': f'{high_1_name.upper()}: Bet £X at {high_1_odds:.2f} (highest odds)',
                    'bet_2': f'{high_2_name.upper()}: Bet £Y at {high_2_odds:.2f} (2nd highest odds)',
                    'bet_3': f'{low_name.upper()}: Bet £Z at {low_odds:.2f} ⚡ SIGNAL FIRED (prob: {low_prediction["probability"]:.3f})',
                },
                'ml_confidence': low_prediction['confidence'],
                'signal_probability': low_prediction['probability']
            }
        else:
            return {
                'action': 'BET_TWO_AND_WAIT',
                'reason': f'Bet on 2 highest odds, wait for {low_name.upper()} timing signal',
                'strategy': {
                    'bet_1': f'{high_1_name.upper()}: Bet £X at {high_1_odds:.2f} NOW',
                    'bet_2': f'{high_2_name.upper()}: Bet £Y at {high_2_odds:.2f} NOW',
                    'wait': f'{low_name.upper()}: WAIT for signal (current prob: {low_prediction["probability"]:.3f}, need: {low_prediction["threshold"]:.3f})',
                },
                'monitoring': f'Watch {low_name.upper()} - waiting for probability to reach {low_prediction["threshold"]:.3f}',
                'current_gap': low_prediction['threshold'] - low_prediction['probability']
            }

    def save_tracking_state(self, filepath='../../data/live_tracking_state.json'):
        """Save historical snapshots to file."""
        data = {
            'historical_snapshots': self.historical_snapshots,
            'team_mapping': self.team_mapping,
            'next_team_id': self.next_team_id,
            'last_update': datetime.now(timezone.utc).isoformat()
        }

        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)

    def load_tracking_state(self, filepath='../../data/live_tracking_state.json'):
        """Load historical snapshots from file."""
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                data = json.load(f)

            self.historical_snapshots = data.get('historical_snapshots', {})
            self.team_mapping = data.get('team_mapping', {})
            self.next_team_id = data.get('next_team_id', 1)

            print(f"Loaded tracking state: {len(self.historical_snapshots)} matches")
            for match_id, snapshots in self.historical_snapshots.items():
                print(f"  {match_id}: {len(snapshots)} snapshots")


if __name__ == "__main__":
    print("="*70)
    print("LIVE PREDICTOR V2 - Using Fresh Models & Correct Features")
    print("="*70)

    # Initialize (trains models fresh)
    predictor = LivePredictorV2()

    print("\n✓ Predictor ready!")
    print("\nStrategy:")
    print("  1. Track matches starting ~1 week before kickoff")
    print("  2. Bet on 2 HIGHEST odds immediately")
    print("  3. Wait for ML timing signal for 3rd bet (lowest odds)")
    print("  4. Signal fires when model predicts arbitrage will complete")
    print()
