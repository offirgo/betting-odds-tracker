#!/usr/bin/env python3
"""
Live Prediction Engine

Runs trained ML models on real-time odds data to identify betting opportunities.
"""

import sys
import os
import pickle
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import sqlite3


class LivePredictor:
    """Runs ML models on live odds data to predict betting opportunities."""

    def __init__(self, models_dir='../../models/winners'):
        """
        Initialize live predictor with trained models.

        Args:
            models_dir: Directory containing trained models
        """
        self.models_dir = models_dir
        self.models = {}
        self.thresholds = {}
        self.load_models()

    def load_models(self):
        """Load trained ML models and thresholds."""
        print("Loading ML models...")

        # Load timing models
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

            try:
                with open(model_path, 'rb') as f:
                    self.models[outcome] = pickle.load(f)
                print(f"  ✓ Loaded {outcome} timing model")

                with open(threshold_path, 'r') as f:
                    self.thresholds[outcome] = float(f.read().strip())
                print(f"  ✓ Loaded {outcome} threshold: {self.thresholds[outcome]:.3f}")

            except Exception as e:
                print(f"  ✗ Error loading {outcome} model: {e}")
                raise

        print(f"✓ All models loaded successfully\n")

    def extract_features(self, match, historical_snapshots=None):
        """
        Extract ML features from live match data.

        Args:
            match: Current match odds data
            historical_snapshots: Previous snapshots for this match (for trends)

        Returns:
            DataFrame with features ready for prediction
        """
        best = match['best_odds']

        # Calculate days before match
        commence_time = datetime.fromisoformat(match['commence_time'].replace('Z', '+00:00'))
        snapshot_time = datetime.now(timezone.utc)
        days_before = (commence_time - snapshot_time).total_seconds() / 86400

        # Basic features
        features = {
            'home_odds_current': best['home'],
            'draw_odds_current': best['draw'],
            'away_odds_current': best['away'],
            'days_before_match': days_before,
        }

        # Calculate implied probabilities
        features['home_prob_implied'] = 1 / best['home'] if best['home'] > 0 else 0
        features['draw_prob_implied'] = 1 / best['draw'] if best['draw'] > 0 else 0
        features['away_prob_implied'] = 1 / best['away'] if best['away'] > 0 else 0

        # Market efficiency
        total_prob = features['home_prob_implied'] + features['draw_prob_implied'] + features['away_prob_implied']
        features['market_overround'] = total_prob - 1.0

        # Odds ratios
        features['home_away_odds_ratio'] = best['home'] / best['away'] if best['away'] > 0 else 0
        features['draw_home_odds_ratio'] = best['draw'] / best['home'] if best['home'] > 0 else 0
        features['draw_away_odds_ratio'] = best['draw'] / best['away'] if best['away'] > 0 else 0

        # If we have historical snapshots, calculate trends
        if historical_snapshots and len(historical_snapshots) > 0:
            # Sort by time
            historical_snapshots.sort(key=lambda x: x['snapshot_time_unix'])

            # Get previous snapshot
            prev = historical_snapshots[-1]
            prev_best = prev['best_odds']

            # Odds changes
            features['home_odds_change'] = best['home'] - prev_best['home']
            features['draw_odds_change'] = best['draw'] - prev_best['draw']
            features['away_odds_change'] = best['away'] - prev_best['away']

            # Percentage changes
            features['home_odds_pct_change'] = (features['home_odds_change'] / prev_best['home'] * 100) if prev_best['home'] > 0 else 0
            features['draw_odds_pct_change'] = (features['draw_odds_change'] / prev_best['draw'] * 100) if prev_best['draw'] > 0 else 0
            features['away_odds_pct_change'] = (features['away_odds_change'] / prev_best['away'] * 100) if prev_best['away'] > 0 else 0

            # Volatility (if we have multiple snapshots)
            if len(historical_snapshots) >= 2:
                home_odds_history = [s['best_odds']['home'] for s in historical_snapshots] + [best['home']]
                draw_odds_history = [s['best_odds']['draw'] for s in historical_snapshots] + [best['draw']]
                away_odds_history = [s['best_odds']['away'] for s in historical_snapshots] + [best['away']]

                features['home_odds_volatility'] = np.std(home_odds_history)
                features['draw_odds_volatility'] = np.std(draw_odds_history)
                features['away_odds_volatility'] = np.std(away_odds_history)
            else:
                features['home_odds_volatility'] = 0
                features['draw_odds_volatility'] = 0
                features['away_odds_volatility'] = 0

        else:
            # No historical data - set defaults
            for field in ['home_odds_change', 'draw_odds_change', 'away_odds_change',
                         'home_odds_pct_change', 'draw_odds_pct_change', 'away_odds_pct_change',
                         'home_odds_volatility', 'draw_odds_volatility', 'away_odds_volatility']:
                features[field] = 0

        # Additional derived features
        features['min_odds'] = min(best['home'], best['draw'], best['away'])
        features['max_odds'] = max(best['home'], best['draw'], best['away'])
        features['odds_range'] = features['max_odds'] - features['min_odds']

        # Favorite/underdog indicators
        features['is_home_favorite'] = 1 if best['home'] == features['min_odds'] else 0
        features['is_away_favorite'] = 1 if best['away'] == features['min_odds'] else 0
        features['is_draw_likely'] = 1 if best['draw'] < 3.5 else 0

        return pd.DataFrame([features])

    def predict_timing_signals(self, features_df):
        """
        Run timing models to predict if we should bet now.

        Args:
            features_df: DataFrame with extracted features

        Returns:
            Dict with predictions for each outcome
        """
        predictions = {}

        for outcome in ['home', 'draw', 'away']:
            model = self.models[outcome]
            threshold = self.thresholds[outcome]

            try:
                # Get prediction probability
                proba = model.predict_proba(features_df)[0][1]

                # Apply threshold
                should_bet = proba >= threshold

                predictions[outcome] = {
                    'probability': float(proba),
                    'threshold': float(threshold),
                    'should_bet_now': bool(should_bet),
                    'confidence': 'high' if proba >= threshold + 0.1 else ('medium' if proba >= threshold else 'low')
                }

            except Exception as e:
                print(f"Error predicting {outcome}: {e}")
                predictions[outcome] = {
                    'probability': 0.0,
                    'threshold': float(threshold),
                    'should_bet_now': False,
                    'confidence': 'error',
                    'error': str(e)
                }

        return predictions

    def analyze_match(self, match, historical_snapshots=None):
        """
        Complete analysis of a match - extract features and run predictions.

        Args:
            match: Current match odds data
            historical_snapshots: Previous snapshots for trend analysis

        Returns:
            Dict with complete analysis including arbitrage and timing predictions
        """
        # Extract features
        features_df = self.extract_features(match, historical_snapshots)

        # Run timing predictions
        timing_predictions = self.predict_timing_signals(features_df)

        # Check for arbitrage opportunity
        best = match['best_odds']
        combined_inverse = (1/best['home']) + (1/best['draw']) + (1/best['away'])
        has_arbitrage = combined_inverse < 1.0

        if has_arbitrage:
            stake = 100
            guaranteed_return = stake / combined_inverse
            profit = guaranteed_return - stake
            profit_pct = (profit / stake) * 100
        else:
            profit_pct = 0

        # Build complete analysis
        analysis = {
            'match_id': match['match_id'],
            'home_team': match['home_team'],
            'away_team': match['away_team'],
            'commence_time': match['commence_time'],
            'days_before_match': features_df['days_before_match'].iloc[0],
            'current_odds': {
                'home': best['home'],
                'draw': best['draw'],
                'away': best['away']
            },
            'arbitrage': {
                'exists': has_arbitrage,
                'profit_pct': profit_pct,
                'combined_inverse': combined_inverse
            },
            'timing_predictions': timing_predictions,
            'recommendation': self.generate_recommendation(has_arbitrage, timing_predictions, best)
        }

        return analysis

    def generate_recommendation(self, has_arbitrage, timing_predictions, best_odds):
        """
        Generate betting recommendation based on arbitrage and timing signals.

        Args:
            has_arbitrage: Whether arbitrage opportunity exists
            timing_predictions: Timing model predictions
            best_odds: Best available odds

        Returns:
            Dict with recommendation
        """
        if not has_arbitrage:
            return {
                'action': 'WAIT',
                'reason': 'No arbitrage opportunity - combined inverse >= 1.0',
                'next_steps': 'Wait for odds to improve'
            }

        # Identify which outcomes to bet on
        sorted_odds = sorted([
            ('home', best_odds['home']),
            ('draw', best_odds['draw']),
            ('away', best_odds['away'])
        ], key=lambda x: x[1], reverse=True)

        high_odd_1, high_odd_2, low_odd = sorted_odds[0][0], sorted_odds[1][0], sorted_odds[2][0]

        # Check if timing signal fired for the low odds outcome
        should_bet_third = timing_predictions[low_odd]['should_bet_now']

        if should_bet_third:
            return {
                'action': 'BET_NOW',
                'reason': f'Arbitrage exists AND timing signal fired for {low_odd}',
                'bet_strategy': {
                    'step_1': f'Bet {high_odd_1} immediately',
                    'step_2': f'Bet {high_odd_2} immediately',
                    'step_3': f'Bet {low_odd} NOW (signal fired)',
                    'third_outcome_probability': timing_predictions[low_odd]['probability']
                }
            }
        else:
            return {
                'action': 'PREPARE',
                'reason': f'Arbitrage exists but waiting for {low_odd} timing signal',
                'bet_strategy': {
                    'step_1': f'Bet {high_odd_1} immediately',
                    'step_2': f'Bet {high_odd_2} immediately',
                    'step_3': f'WAIT for {low_odd} signal (prob: {timing_predictions[low_odd]["probability"]:.3f}, threshold: {timing_predictions[low_odd]["threshold"]:.3f})',
                },
                'monitor': f'Watch {low_odd} odds and wait for timing signal'
            }


if __name__ == "__main__":
    # Example usage
    print("Live Prediction Engine")
    print("=" * 60)

    predictor = LivePredictor()

    # Example match data
    example_match = {
        'match_id': 'test123',
        'home_team': 'Arsenal',
        'away_team': 'Chelsea',
        'commence_time': '2024-03-20T19:45:00Z',
        'best_odds': {
            'home': 2.10,
            'draw': 3.40,
            'away': 3.80
        }
    }

    print("\nAnalyzing example match...")
    analysis = predictor.analyze_match(example_match)

    print(f"\n{analysis['home_team']} vs {analysis['away_team']}")
    print(f"Days before: {analysis['days_before_match']:.1f}")
    print(f"Odds: H {analysis['current_odds']['home']:.2f} | D {analysis['current_odds']['draw']:.2f} | A {analysis['current_odds']['away']:.2f}")
    print(f"\nArbitrage: {analysis['arbitrage']['exists']} ({analysis['arbitrage']['profit_pct']:.2f}% profit)")
    print(f"\nTiming Signals:")
    for outcome, pred in analysis['timing_predictions'].items():
        signal = "✓" if pred['should_bet_now'] else "✗"
        print(f"  {signal} {outcome.capitalize()}: {pred['probability']:.3f} (threshold: {pred['threshold']:.3f}) - {pred['confidence']}")

    print(f"\nRecommendation: {analysis['recommendation']['action']}")
    print(f"  {analysis['recommendation']['reason']}")
