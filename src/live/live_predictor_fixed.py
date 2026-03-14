#!/usr/bin/env python3
"""
Live Prediction Engine - FIXED

Loads pre-trained models using joblib and extracts correct features.
Implements YOUR betting strategy:
  1. Bet on 2 highest odds immediately
  2. Wait for ML timing signal for 3rd bet (lowest odds)
"""

import joblib
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import json
import os


class LivePredictor:
    """Prediction engine using pre-trained timing models."""

    def __init__(self, models_dir='../../models/winners'):
        """
        Initialize predictor with pre-trained models.

        Args:
            models_dir: Directory containing trained models
        """
        self.models_dir = models_dir
        self.models = {}
        self.thresholds = {}
        self.historical_snapshots = {}  # Track snapshots per match
        self.team_mapping = {}  # Map team names to IDs
        self.next_team_id = 1

        # Load pre-trained models
        self.load_models()

        # Load tracking state if exists
        self.load_tracking_state()

    def load_models(self):
        """Load pre-trained ML models using joblib."""
        print("\n" + "="*70)
        print("LOADING PRE-TRAINED ML MODELS")
        print("="*70 + "\n")

        # Model files
        model_files = {
            'home': 'model_home_timing_strong_precision.pkl',
            'draw': 'model_draw_timing_strong_lower_false_alarms.pkl',
            'away': 'model_away_timing_strong_precision.pkl'
        }

        threshold_files = {
            'home': 'threshold_home_timing_strong_precision.txt',
            'draw': 'threshold_draw_timing_strong_lower_false_alarms.txt',
            'away': 'threshold_away_timing_strong_precision.txt'
        }

        for outcome in ['home', 'draw', 'away']:
            model_path = os.path.join(self.models_dir, model_files[outcome])
            threshold_path = os.path.join(self.models_dir, threshold_files[outcome])

            # Load model with joblib (works!)
            try:
                self.models[outcome] = joblib.load(model_path)
                print(f"✓ Loaded {outcome} timing model")
                print(f"  Type: {type(self.models[outcome])}")

                # Load threshold
                with open(threshold_path, 'r') as f:
                    self.thresholds[outcome] = float(f.read().strip())
                print(f"  Threshold: {self.thresholds[outcome]:.3f}\n")

            except Exception as e:
                print(f"✗ Error loading {outcome} model: {e}")
                raise

        print("="*70)
        print("✓ ALL MODELS LOADED SUCCESSFULLY")
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

        Features expected by model:
        - days_before_match, hours_before_match
        - home_team_id, away_team_id
        - num_snapshots_seen
        - home/draw/away_odds_current
        - combined_inverse_current
        - home/draw/away_odds min/max/mean/std/trend historical
        - home/draw/away_odds_change_recent

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

        # Build features dict matching EXACT training format
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
                'next_steps': f'Currently have {len(self.historical_snapshots.get(match_id, []))} snapshot(s). Keep tracking.',
                'status': 'building_history'
            }
            return analysis

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
        recommendation = self.generate_recommendation(best, timing_predictions, features_df)
        analysis['recommendation'] = recommendation

        return analysis

    def generate_recommendation(self, best_odds, timing_predictions, features_df):
        """
        Generate recommendation using YOUR betting strategy:
        1. Bet on 2 highest odds immediately
        2. Wait for timing model signal for 3rd bet (lowest odds)

        Args:
            best_odds: Current best odds dict
            timing_predictions: ML timing predictions
            features_df: Extracted features

        Returns:
            Dict with recommendation
        """
        days_before = features_df['days_before_match'].iloc[0]

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
                'reason': f'ML timing model signals to complete arbitrage with {low_name.upper()} bet',
                'status': 'signal_fired',
                'strategy': {
                    'bet_1': f'{high_1_name.upper()}: Bet at {high_1_odds:.2f} (highest odds)',
                    'bet_2': f'{high_2_name.upper()}: Bet at {high_2_odds:.2f} (2nd highest)',
                    'bet_3': f'{low_name.upper()}: Bet at {low_odds:.2f} ⚡ SIGNAL FIRED',
                },
                'ml_details': {
                    'outcome': low_name,
                    'probability': low_prediction['probability'],
                    'threshold': low_prediction['threshold'],
                    'confidence': low_prediction['confidence']
                },
                'days_before_match': days_before
            }
        else:
            gap = low_prediction['threshold'] - low_prediction['probability']

            return {
                'action': 'BET_TWO_AND_WAIT',
                'reason': f'Bet 2 highest odds now, wait for {low_name.upper()} timing signal',
                'status': 'waiting_for_signal',
                'strategy': {
                    'bet_1': f'{high_1_name.upper()}: Bet at {high_1_odds:.2f} NOW',
                    'bet_2': f'{high_2_name.upper()}: Bet at {high_2_odds:.2f} NOW',
                    'wait': f'{low_name.upper()}: WAIT for signal',
                },
                'ml_details': {
                    'outcome': low_name,
                    'probability': low_prediction['probability'],
                    'threshold': low_prediction['threshold'],
                    'gap': gap,
                    'confidence': low_prediction['confidence']
                },
                'days_before_match': days_before,
                'monitoring': f'Watching {low_name.upper()} - need probability to reach {low_prediction["threshold"]:.3f} (currently {low_prediction["probability"]:.3f}, gap: {gap:.3f})'
            }

    def save_tracking_state(self, filepath='../../data/live_tracking_state.json'):
        """Save historical snapshots to file for persistence."""
        data = {
            'historical_snapshots': self.historical_snapshots,
            'team_mapping': self.team_mapping,
            'next_team_id': self.next_team_id,
            'last_update': datetime.now(timezone.utc).isoformat()
        }

        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)

        print(f"Saved tracking state: {len(self.historical_snapshots)} matches")

    def load_tracking_state(self, filepath='../../data/live_tracking_state.json'):
        """Load historical snapshots from file."""
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r') as f:
                    data = json.load(f)

                self.historical_snapshots = data.get('historical_snapshots', {})
                self.team_mapping = data.get('team_mapping', {})
                self.next_team_id = data.get('next_team_id', 1)

                print(f"Loaded tracking state from {data.get('last_update', 'unknown')}")
                print(f"  Tracking {len(self.historical_snapshots)} matches")
                for match_id, snapshots in list(self.historical_snapshots.items())[:3]:
                    print(f"    {match_id}: {len(snapshots)} snapshots")
                if len(self.historical_snapshots) > 3:
                    print(f"    ... and {len(self.historical_snapshots) - 3} more")
                print()

            except Exception as e:
                print(f"Error loading tracking state: {e}")


if __name__ == "__main__":
    print("="*70)
    print("LIVE PREDICTOR - Using Pre-Trained Models")
    print("="*70)

    # Initialize
    predictor = LivePredictor()

    print("\n✓ Predictor ready!")
    print("\nBETTING STRATEGY:")
    print("  1. Track matches starting ~1 week before kickoff")
    print("  2. Bet on 2 HIGHEST odds immediately")
    print("  3. Wait for ML timing signal for 3rd bet (lowest odds)")
    print("  4. Signal fires when model predicts arbitrage will complete")
    print()
