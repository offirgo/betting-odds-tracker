#!/usr/bin/env python3
"""
Test script for the BettingSimulator class

This script tests basic functionality of the simulator:
1. Model loading
2. Database connection
3. Data access
4. Configuration updates

Usage:
    python test_simulator.py
"""

import os
import sys
from datetime import datetime, timedelta
import logging

# Add parent directory to path to import BettingSimulator
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from   betting_simulator import BettingSimulator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("TestSimulator")


def test_model_loading():
    """Test that models can be loaded correctly"""
    print("\n===== Testing Model Loading =====")

    # Path to models directory
    models_dir = "../models"  # Adjust as needed

    # Create simulator with just the models directory
    simulator = BettingSimulator(db_path=None, models_dir=models_dir)

    # Check if models were loaded
    loaded_models = list(simulator.models.keys())
    print(f"Loaded models: {loaded_models}")

    # Check if thresholds were loaded
    loaded_thresholds = list(simulator.thresholds.keys())
    print(f"Loaded thresholds: {loaded_thresholds}")

    # Print threshold values
    for name, threshold in simulator.thresholds.items():
        print(f"  {name}: {threshold:.3f}")

    return len(simulator.models) > 0


def test_database_connection():
    """Test connection to the odds database"""
    print("\n===== Testing Database Connection =====")

    # Path to database
    db_path = "../data/odds_history.db"  # Adjust as needed

    # Create simulator with just the database path
    simulator = BettingSimulator(db_path=db_path, models_dir=None)

    # Try connecting to the database
    conn = simulator.connect_to_db()
    if conn:
        print("✓ Successfully connected to database")

        # Get some basic stats from the database
        try:
            cursor = conn.cursor()

            # Count events
            cursor.execute("SELECT COUNT(*) FROM events")
            event_count = cursor.fetchone()[0]
            print(f"Found {event_count} events in database")

            # Count odds snapshots
            cursor.execute("SELECT COUNT(*) FROM odds_history")
            odds_count = cursor.fetchone()[0]
            print(f"Found {odds_count} odds snapshots in database")

            # Get date range
            cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM odds_history")
            min_date, max_date = cursor.fetchone()
            print(f"Odds data from {min_date} to {max_date}")

            conn.close()
            return True
        except Exception as e:
            print(f"✗ Error querying database: {e}")
            return False
    else:
        print("✗ Failed to connect to database")
        return False


def test_data_access():
    """Test accessing event data and odds snapshots"""
    print("\n===== Testing Data Access =====")

    # Paths
    db_path = "../data/odds_history.db"  # Adjust as needed
    models_dir = "../models"  # Adjust as needed

    # Create simulator
    simulator = BettingSimulator(db_path=db_path, models_dir=models_dir)

    # Get simulation timespan
    start, end = simulator.get_simulation_timespan()
    if start and end:
        print(f"Available data from {start} to {end}")

        # Try to get events for a week
        test_start = start + timedelta(days=1)  # Start 1 day in
        test_end = test_start + timedelta(days=7)  # 1 week period

        events = simulator.get_events_in_timerange(test_start, test_end, limit=5)
        if events:
            print(f"Found {len(events)} events between {test_start} and {test_end}")

            # Print first event details
            if len(events) > 0:
                event = events[0]
                print(f"\nSample event:")
                print(f"  ID: {event['event_id']}")
                print(f"  Teams: {event['home_team']} vs {event['away_team']}")
                print(f"  Start time: {event['commence_time']}")

                # Try to get odds snapshots for this event
                snapshots = simulator.get_odds_snapshots(event['event_id'])
                if snapshots:
                    print(f"  Found {len(snapshots)} odds snapshots for this event")

                    # Print a sample odds snapshot
                    if len(snapshots) > 0:
                        snapshot = snapshots[0]
                        print(f"\nSample snapshot at {snapshot['timestamp']}:")

                        # Print H2H market if available
                        if 'h2h' in snapshot['markets']:
                            h2h = snapshot['markets']['h2h']
                            print(f"  H2H market:")
                            for outcome, bookmakers in h2h.items():
                                best_odds = max(bookmakers.values()) if bookmakers else 0
                                print(f"    {outcome}: {best_odds:.2f} (best odds)")

                        # Extract features for this event at this time
                        features = simulator.extract_features(event, [snapshot], snapshot['timestamp'])
                        if features:
                            print(f"\nExtracted {len(features)} features for model input")

                            # Print a few key features
                            key_features = ['combined_inverse_current', 'hours_before_match']
                            for feature in key_features:
                                if feature in features:
                                    print(f"  {feature}: {features[feature]}")

                            return True
                        else:
                            print("✗ Failed to extract features")
                    else:
                        print("✗ No snapshot details available")
                else:
                    print("✗ Failed to get odds snapshots")
            else:
                print("✗ No event details available")
        else:
            print("✗ Failed to get events")
    else:
        print("✗ Failed to get simulation timespan")

    return False


def test_configuration():
    """Test configuration updates"""
    print("\n===== Testing Configuration Updates =====")

    # Create simulator with default config
    simulator = BettingSimulator(db_path=None, models_dir=None)

    # Print default config
    print("Default config:")
    for category, settings in simulator.config.items():
        print(f"  {category}:")
        for key, value in settings.items():
            print(f"    {key}: {value}")

    # Create custom config
    custom_config = {
        'time_thresholds': {
            'first_fallback': 12,  # Change from 6 to 12 hours
            'final_fallback': 2  # Change from 1 to 2 hours
        },
        'simulation': {
            'time_step': 0.5  # Change from 1 to 0.5 hours
        },
        'custom_category': {  # Add new category
            'new_setting': True
        }
    }

    # Create new simulator with custom config
    simulator2 = BettingSimulator(db_path=None, models_dir=None, config=custom_config)

    # Print updated config
    print("\nUpdated config:")
    for category, settings in simulator2.config.items():
        print(f"  {category}:")
        for key, value in settings.items():
            print(f"    {key}: {value}")

    # Verify specific changes
    success = True
    if simulator2.config['time_thresholds']['first_fallback'] != 12:
        print("✗ first_fallback was not updated correctly")
        success = False

    if simulator2.config['simulation']['time_step'] != 0.5:
        print("✗ time_step was not updated correctly")
        success = False

    if 'custom_category' not in simulator2.config:
        print("✗ custom_category was not added")
        success = False

    if success:
        print("✓ Configuration updated successfully")

    return success


def main():
    """Run all tests"""
    print("Starting tests for BettingSimulator...")

    tests = [
        test_model_loading,
        test_database_connection,
        test_data_access,
        test_configuration
    ]

    results = []
    for test in tests:
        result = test()
        results.append(result)

    # Print summary
    print("\n===== Test Summary =====")
    for i, (test, result) in enumerate(zip(tests, results)):
        status = "PASS" if result else "FAIL"
        print(f"{i + 1}. {test.__name__}: {status}")

    overall = all(results)
    print(f"\nOverall: {'SUCCESS' if overall else 'FAILURE'}")

    return 0 if overall else 1


if __name__ == "__main__":
    sys.exit(main())