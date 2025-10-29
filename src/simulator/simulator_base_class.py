#!/usr/bin/env python3
"""
Betting Arbitrage Simulator

This module provides a simulator for testing betting strategies on historical odds data.
It allows for backtesting the complete arbitrage system including all five models:
1. Should I bet on this game? (arbitrage potential)
2. What outcome to bet on? (Home/Draw/Away)
3-5. When to bet on each outcome? (timing models)

Usage:
    simulator = BettingSimulator(db_path, models_dir)
    results = simulator.run_simulation(start_date, end_date, initial_bankroll)
"""

import os
import sqlite3
import pandas as pd
import numpy as np
import joblib
from datetime import datetime, timedelta
import logging
import json

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("BettingSimulator")


class BettingSimulator:
    """Simulator for testing betting strategies on historical odds data."""

    def __init__(self, db_path, models_dir, config=None):
        """
        Initialize the betting simulator.

        Args:
            db_path (str): Path to the SQLite database with historical odds data
            models_dir (str): Directory containing trained models and thresholds
            config (dict, optional): Configuration parameters for the simulator
        """
        # Store paths
        self.db_path = db_path
        self.models_dir = models_dir

        # Initialize configuration with defaults
        self.config = {
            # Time thresholds for forced betting (hours before match)
            'time_thresholds': {
                'first_fallback': 6,  # 6 hours before match
                'second_fallback': 3,  # 3 hours before match
                'final_fallback': 1  # 1 hour before match (must bet)
            },

            # Bet sizing options
            'bet_sizing': {
                'method': 'fixed',  # 'fixed', 'percentage', 'kelly'
                'fixed_amount': 100,  # Amount for fixed bets
                'bankroll_percent': 1  # Percentage for percentage-based bets
            },

            # Simulation parameters
            'simulation': {
                'time_step': 1,  # Hours to advance in each simulation step
                'max_games': None,  # Max number of games to simulate (None for all)
                'track_all_games': False  # If True, track all games regardless of Model 1
            }
        }

        # Override defaults with provided config
        if config:
            self._update_config(config)

        # Initialize state variables
        self.current_time = None
        self.end_time = None
        self.bankroll = 0
        self.active_games = {}
        self.completed_games = []
        self.betting_history = []

        # Load models and thresholds
        self.models = {}
        self.thresholds = {}
        self._load_models()

        logger.info(f"Simulator initialized with database: {db_path}")
        logger.info(f"Loaded {len(self.models)} models from {models_dir}")

    def _update_config(self, config):
        """Update configuration with user-provided values."""
        for category, settings in config.items():
            if category in self.config:
                if isinstance(self.config[category], dict):
                    self.config[category].update(settings)
                else:
                    self.config[category] = settings
            else:
                self.config[category] = settings

    def _load_models(self):
        """Load all models and thresholds from the models directory."""
        try:
            # Model files to load - only high profit model and timing models
            model_files = {
                # High profit arbitrage detection (>3%)
                'high_profit': 'model_high_profit_gt3_0pct_improved_strong.pkl',

                # Timing models
                'home_timing': 'model_home_timing_strong_precision.pkl',
                'draw_timing': 'model_draw_timing_strong_lower_false_alarms.pkl',
                'away_timing': 'model_away_timing_strong_precision.pkl'
            }

            # Threshold files
            threshold_files = {
                'home_timing': 'threshold_home_timing_strong_precision.txt',
                'draw_timing': 'threshold_draw_timing_strong_lower_false_alarms.txt',
                'away_timing': 'threshold_away_timing_strong_precision.txt'
            }

            # Check if models are in winners subdirectory
            if os.path.exists(os.path.join(self.models_dir, 'winners')):
                winners_dir = os.path.join(self.models_dir, 'winners')
                logger.info(f"Found 'winners' subdirectory, loading models from there")
            else:
                winners_dir = self.models_dir

            # Load models
            for name, filename in model_files.items():
                try:
                    # Try loading from winners directory first
                    model_path = os.path.join(winners_dir, filename)
                    if os.path.exists(model_path):
                        self.models[name] = joblib.load(model_path)
                        logger.info(f"Loaded {name} model from {model_path}")
                    else:
                        # Try main models directory
                        model_path = os.path.join(self.models_dir, filename)
                        if os.path.exists(model_path):
                            self.models[name] = joblib.load(model_path)
                            logger.info(f"Loaded {name} model from {model_path}")
                        else:
                            logger.warning(f"Model file not found: {filename}")
                except Exception as e:
                    logger.error(f"Error loading {name} model: {e}")

            # Load thresholds
            for name, filename in threshold_files.items():
                try:
                    # Try loading from winners directory first
                    threshold_path = os.path.join(winners_dir, filename)
                    if os.path.exists(threshold_path):
                        with open(threshold_path, 'r') as f:
                            self.thresholds[name] = float(f.read().strip())
                        logger.info(f"Loaded {name} threshold: {self.thresholds[name]:.3f}")
                    else:
                        # Try main models directory
                        threshold_path = os.path.join(self.models_dir, filename)
                        if os.path.exists(threshold_path):
                            with open(threshold_path, 'r') as f:
                                self.thresholds[name] = float(f.read().strip())
                            logger.info(f"Loaded {name} threshold: {self.thresholds[name]:.3f}")
                        else:
                            # Default thresholds if files don't exist
                            if name == 'home_timing':
                                self.thresholds[name] = 0.55  # From model results
                            elif name == 'draw_timing':
                                self.thresholds[name] = 0.575  # From model results
                            elif name == 'away_timing':
                                self.thresholds[name] = 0.55  # From model results
                            logger.warning(f"Using default threshold for {name}: {self.thresholds[name]}")
                except Exception as e:
                    logger.error(f"Error loading {name} threshold: {e}")
                    # Set default threshold
                    self.thresholds[name] = 0.5

            # Log missing models
            required_models = ['high_profit', 'home_timing', 'draw_timing', 'away_timing']
            missing_models = [name for name in required_models if name not in self.models]
            if missing_models:
                logger.warning(f"Missing required models: {missing_models}")
            else:
                logger.info("All required models loaded successfully")

        except Exception as e:
            logger.error(f"Error in _load_models: {e}")
            self.thresholds[name]}")
            except Exception as e:
            logger.error(f"Error loading {name} threshold: {e}")
            # Set default threshold
            self.thresholds[name] = 0.5

        # Log missing models
        required_models = ['should_bet', 'home_timing', 'draw_timing', 'away_timing']
        missing_models = [name for name in required_models if name not in self.models]
        if missing_models:
            logger.warning(f"Missing required models: {missing_models}")
        else:
            logger.info("All required models loaded successfully")

    except Exception as e:
    logger.error(f"Error in _load_models: {e}")


def connect_to_db(self):
    """Connect to the SQLite database."""
    try:
        conn = sqlite3.connect(self.db_path)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return None


def get_simulation_timespan(self):
    """
    Get the available time range in the database for simulation.

    Returns:
        tuple: (earliest_time, latest_time) as datetime objects
    """
    conn = self.connect_to_db()
    if not conn:
        return None, None

    try:
        cursor = conn.cursor()

        # Get earliest timestamp
        cursor.execute("SELECT MIN(timestamp) FROM odds_history")
        earliest = cursor.fetchone()[0]

        # Get latest timestamp
        cursor.execute("SELECT MAX(timestamp) FROM odds_history")
        latest = cursor.fetchone()[0]

        # Get earliest and latest commence times
        cursor.execute("SELECT MIN(commence_time), MAX(commence_time) FROM events")
        earliest_match, latest_match = cursor.fetchone()

        # Convert to datetime objects
        earliest_time = datetime.fromisoformat(earliest.replace('Z', '+00:00'))
        latest_time = datetime.fromisoformat(latest.replace('Z', '+00:00'))
        earliest_match_time = datetime.fromisoformat(earliest_match.replace('Z', '+00:00'))
        latest_match_time = datetime.fromisoformat(latest_match.replace('Z', '+00:00'))

        logger.info(f"Database contains odds from {earliest_time} to {latest_time}")
        logger.info(f"Matches in database from {earliest_match_time} to {latest_match_time}")

        return earliest_time, latest_time

    except Exception as e:
        logger.error(f"Error getting simulation timespan: {e}")
        return None, None
    finally:
        conn.close()


def get_events_in_timerange(self, start_time, end_time, limit=None):
    """
    Get all events that start between start_time and end_time.

    Args:
        start_time (datetime): Start of the time range
        end_time (datetime): End of the time range
        limit (int, optional): Maximum number of events to return

    Returns:
        list: List of event dictionaries
    """
    conn = self.connect_to_db()
    if not conn:
        return []

    try:
        cursor = conn.cursor()

        # Format datetime objects for SQLite query
        start_str = start_time.isoformat()
        end_str = end_time.isoformat()

        # Build query
        query = """
                SELECT e.event_id, e.home_team, e.away_team, e.commence_time, e.sport,
                       COUNT(DISTINCT oh.bookmaker) as bookmaker_count
                FROM events e
                JOIN odds_history oh ON e.event_id = oh.event_id
                WHERE e.commence_time BETWEEN ? AND ?
                GROUP BY e.event_id
                HAVING bookmaker_count > 0
                ORDER BY e.commence_time
            """

        # Add limit if specified
        if limit:
            query += f" LIMIT {int(limit)}"

        # Execute query
        cursor.execute(query, (start_str, end_str))

        events = []
        for row in cursor.fetchall():
            event_id, home_team, away_team, commence_time, sport, bookmaker_count = row

            # Convert ISO string to datetime
            commence_dt = datetime.fromisoformat(commence_time.replace('Z', '+00:00'))

            event = {
                'event_id': event_id,
                'home_team': home_team,
                'away_team': away_team,
                'commence_time': commence_dt,
                'sport': sport,
                'bookmaker_count': bookmaker_count,
                'state': 'new',  # Initial state for simulation tracking
                'bets': [],  # Will store bets placed during simulation
            }
            events.append(event)

        logger.info(f"Found {len(events)} events between {start_time} and {end_time}")
        return events

    except Exception as e:
        logger.error(f"Error getting events in timerange: {e}")
        return []
    finally:
        conn.close()


def get_odds_snapshots(self, event_id, before_time=None):
    """
    Get all odds snapshots for an event, optionally before a specific time.

    Args:
        event_id (str): ID of the event
        before_time (datetime, optional): Only get snapshots before this time

    Returns:
        list: List of odds snapshots sorted by timestamp
    """
    conn = self.connect_to_db()
    if not conn:
        return []

    try:
        cursor = conn.cursor()

        # Build base query
        query = """
                SELECT oh.timestamp, oh.bookmaker, oh.market, oh.outcome, oh.price
                FROM odds_history oh
                WHERE oh.event_id = ?
            """

        params = [event_id]

        # Add time constraint if specified
        if before_time:
            query += " AND oh.timestamp <= ?"
            params.append(before_time.isoformat())

        # Order by timestamp
        query += " ORDER BY oh.timestamp"

        # Execute query
        cursor.execute(query, params)

        # Organize results by timestamp
        snapshots = {}
        for row in cursor.fetchall():
            timestamp, bookmaker, market, outcome, price = row

            # Convert ISO string to datetime
            timestamp_dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))

            # Create timestamp entry if it doesn't exist
            if timestamp_dt not in snapshots:
                snapshots[timestamp_dt] = {
                    'timestamp': timestamp_dt,
                    'markets': {}
                }

            # Create market entry if it doesn't exist
            if market not in snapshots[timestamp_dt]['markets']:
                snapshots[timestamp_dt]['markets'][market] = {}

            # Create outcome entry if it doesn't exist
            if outcome not in snapshots[timestamp_dt]['markets'][market]:
                snapshots[timestamp_dt]['markets'][market][outcome] = {}

            # Add bookmaker price
            snapshots[timestamp_dt]['markets'][market][outcome][bookmaker] = price

        # Convert to sorted list
        snapshot_list = [snapshots[ts] for ts in sorted(snapshots.keys())]

        return snapshot_list

    except Exception as e:
        logger.error(f"Error getting odds snapshots for event {event_id}: {e}")
        return []
    finally:
        conn.close()


def extract_features(self, event, snapshots, current_time):
    """
    Extract features for model predictions from event and odds snapshots.

    Args:
        event (dict): Event information
        snapshots (list): List of odds snapshots
        current_time (datetime): Current simulation time

    Returns:
        dict: Features for model input
    """
    # This is a simplified implementation - actual feature extraction
    # should match exactly what was used in model training

    features = {}

    # Skip if no snapshots
    if not snapshots:
        return None

    # Get timing features
    hours_until_start = (event['commence_time'] - current_time).total_seconds() / 3600
    features['days_before_match'] = hours_until_start / 24
    features['hours_before_match'] = hours_until_start

    # Use the most recent snapshot for current odds
    latest_snapshot = snapshots[-1]

    # Only proceed if we have h2h market
    if 'h2h' in latest_snapshot['markets']:
        h2h_market = latest_snapshot['markets']['h2h']

        # Get best current odds for each outcome
        if 'home' in h2h_market:
            home_odds = list(h2h_market['home'].values())
            if home_odds:
                features['home_odds_current'] = max(home_odds)

        if 'draw' in h2h_market:
            draw_odds = list(h2h_market['draw'].values())
            if draw_odds:
                features['draw_odds_current'] = max(draw_odds)

        if 'away' in h2h_market:
            away_odds = list(h2h_market['away'].values())
            if away_odds:
                features['away_odds_current'] = max(away_odds)

        # Calculate combined inverse (arbitrage indicator)
        all_odds = []
        for outcome in ['home', 'draw', 'away']:
            if outcome in h2h_market and h2h_market[outcome]:
                all_odds.append(max(h2h_market[outcome].values()))

        if len(all_odds) == 3:  # Only if we have all three outcomes
            features['combined_inverse_current'] = sum(1 / o for o in all_odds)

    # Get historical odds stats
    features['num_snapshots_seen'] = len(snapshots)

    # Calculate historical stats for each outcome
    for outcome in ['home', 'draw', 'away']:
        prices = []

        for snapshot in snapshots:
            if 'h2h' in snapshot['markets'] and outcome in snapshot['markets']['h2h']:
                outcome_prices = list(snapshot['markets']['h2h'][outcome].values())
                if outcome_prices:
                    prices.append(max(outcome_prices))

        if prices:
            features[f'{outcome}_odds_mean_historical'] = np.mean(prices)
            features[f'{outcome}_odds_std_historical'] = np.std(prices)
            features[f'{outcome}_odds_min_historical'] = min(prices)
            features[f'{outcome}_odds_max_historical'] = max(prices)

    # Placeholder for team IDs - in a real implementation, you would have a mapping
    features['home_team_id'] = hash(event['home_team']) % 1000
    features['away_team_id'] = hash(event['away_team']) % 1000

    return features


def prepare_features_df(self, features_dict):
    """
    Convert feature dictionary to properly formatted DataFrame for model prediction.

    Args:
        features_dict (dict): Dictionary of features

    Returns:
        pandas.DataFrame: Features formatted for model input
    """
    if not features_dict:
        return None

    # Create a single-row DataFrame
    df = pd.DataFrame([features_dict])

    # Check if we have the key features needed
    required_features = ['combined_inverse_current', 'hours_before_match', 'days_before_match']
    if not all(feat in df.columns for feat in required_features):
        logger.warning("Missing required features for prediction")
        return None

    # For any missing features, fill with reasonable defaults or medians
    # This is just an example - adjust based on your specific feature needs
    if 'home_odds_current' not in df.columns:
        df['home_odds_current'] = 2.0
    if 'draw_odds_current' not in df.columns:
        df['draw_odds_current'] = 3.5
    if 'away_odds_current' not in df.columns:
        df['away_odds_current'] = 4.0

    # Fill missing historical stats with reasonable defaults
    for outcome in ['home', 'draw', 'away']:
        if f'{outcome}_odds_mean_historical' not in df.columns:
            df[f'{outcome}_odds_mean_historical'] = df[f'{outcome}_odds_current']
        if f'{outcome}_odds_std_historical' not in df.columns:
            df[f'{outcome}_odds_std_historical'] = 0.1
        if f'{outcome}_odds_min_historical' not in df.columns:
            df[f'{outcome}_odds_min_historical'] = df[f'{outcome}_odds_current'] * 0.9
        if f'{outcome}_odds_max_historical' not in df.columns:
            df[f'{outcome}_odds_max_historical'] = df[f'{outcome}_odds_current'] * 1.1

    return df


def run_simulation(self, start_date, end_date, initial_bankroll, profit_margin=0.03, output_file=None,
                   profit_margins=None):
    """
    Run the betting simulation from start_date to end_date.

    Args:
        start_date (datetime): Start date of simulation
        end_date (datetime): End date of simulation
        initial_bankroll (float): Starting bankroll amount
        profit_margin (float): Target profit margin for arbitrage (default 3%)
        output_file (str, optional): Path to save detailed results
        profit_margins (list, optional): List of profit margins to test (e.g. [0.01, 0.02, 0.03, 0.05])
                                       If provided, runs multiple simulations with different margins

    Returns:
        dict or list: Simulation results for single run or multiple runs with different margins
    """
    # If profit_margins is provided, run multiple simulations
    if profit_margins and isinstance(profit_margins, list):
        logger.info(f"Running multiple simulations with profit margins: {profit_margins}")

        all_results = []

        for margin in profit_margins:
            logger.info(f"Running simulation with profit margin: {margin:.1%}")

            # Run simulation with this margin
            result = self._run_single_simulation(start_date, end_date, initial_bankroll, margin)

            # Add margin info to result
            result['profit_margin'] = margin
            all_results.append(result)

            # If output file provided, save each result
            if output_file:
                margin_output = output_file.replace('.json', f'_margin_{margin:.3f}.json')
                with open(margin_output, 'w') as f:
                    json.dump(result, f, default=str, indent=2)

        # Sort results by profit
        all_results.sort(key=lambda x: x['profit'], reverse=True)

        # Create summary comparing results
        summary = {
            'best_margin': all_results[0]['profit_margin'],
            'best_profit': all_results[0]['profit'],
            'best_roi': all_results[0]['roi'],
            'all_results': all_results
        }

        # Save summary if output file provided
        if output_file:
            summary_output = output_file.replace('.json', '_summary.json')
            with open(summary_output, 'w') as f:
                json.dump(summary, f, default=str, indent=2)

        return summary

    # Otherwise run single simulation
    return self._run_single_simulation(start_date, end_date, initial_bankroll, profit_margin, output_file)


def _run_single_simulation(self, start_date, end_date, initial_bankroll, profit_margin, output_file=None):
    """
    Run a single betting simulation with the specified profit margin.

    Args:
        start_date (datetime): Start date of simulation
        end_date (datetime): End date of simulation
        initial_bankroll (float): Starting bankroll amount
        profit_margin (float): Target profit margin for arbitrage
        output_file (str, optional): Path to save detailed results

    Returns:
        dict: Simulation results including profits, stats, etc.
    """
    # Initialize state variables
    self.current_time = start_date
    self.end_time = end_date
    self.bankroll = initial_bankroll
    self.active_games = {}
    self.completed_games = []
    self.betting_history = []

    # Calculate combined inverse threshold from profit margin
    max_combined_inverse = 1.0 - profit_margin

    logger.info(f"Starting simulation from {start_date} to {end_date}")
    logger.info(f"Initial bankroll: ${initial_bankroll:.2f}")
    logger.info(f"Targeting profit margin: {profit_margin:.1%} (max combined inverse: {max_combined_inverse:.3f})")

    # The actual simulation loop will be implemented later

    # Return placeholder results
    results = {
        'start_date': start_date,
        'end_date': end_date,
        'initial_bankroll': initial_bankroll,
        'final_bankroll': initial_bankroll,  # Placeholder
        'profit': 0,  # Placeholder
        'roi': 0,  # Placeholder
        'profit_margin_used': profit_margin,
        'max_combined_inverse_used': max_combined_inverse,
        'num_games': 0,  # Placeholder
        'num_bets': 0,  # Placeholder
        'win_rate': 0  # Placeholder
    }

    # Save results if output file provided
    if output_file:
        with open(output_file, 'w') as f:
            json.dump(results, f, default=str, indent=2)

    return results


# Example usage (will be in a separate script later)
if __name__ == "__main__":
    # This is just for demonstration/testing the initial structure
    db_path = "../../data/odds_history.db"
    models_dir = "../../models"

    # Create simulator
    simulator = BettingSimulator(db_path, models_dir)

    # Get available timespan
    start, end = simulator.get_simulation_timespan()
    if start and end:
        print(f"Available data from {start} to {end}")

        # Run a short test simulation (just a skeleton at this point)
        test_start = start + timedelta(days=1)  # Start 1 day in
        test_end = test_start + timedelta(days=7)  # Simulate 1 week

        # Test with default profit margin (3%)
        print("\nRunning single simulation with default 3% profit margin:")
        single_result = simulator.run_simulation(
            test_start,
            test_end,
            10000,
            output_file="simulation_result_default.json"
        )

        # Test different profit margins
        print("\nRunning multiple simulations with different profit margins:")
        profit_margins = [0.01, 0.02, 0.03, 0.05, 0.10]
        results = simulator.run_simulation(
            test_start,
            test_end,
            10000,
            output_file="simulation_results.json",
            profit_margins=profit_margins
        )

        print("\nSimulation results summary:")
        print(f"Best profit margin: {results['best_margin']:.1%}")
        print(f"Best profit: ${results['best_profit']:.2f}")
        print(f"Best ROI: {results['best_roi']:.2%}")

        print("\nAll tested profit margins:")
        for result in results['all_results']:
            print(f"Margin: {result['profit_margin']:.1%}, Profit: ${result['profit']:.2f}, ROI: {result['roi']:.2%}")