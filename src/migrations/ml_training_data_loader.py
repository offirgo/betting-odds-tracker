#!/usr/bin/env python3
"""
Migration: Create ml_features table for machine learning training data
Run this once to add the table to your existing database
"""

import sqlite3
import os
from datetime import datetime


def run_migration(db_path='../../data/raw/epl_arbitrage.db'):
    """Create the ml_features table"""

    if not os.path.exists(db_path):
        print(f"Error: Database not found at {db_path}")
        return False

    print(f"Running migration on: {db_path}")
    print(f"Migration: Create ml_features table")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 60)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Create ml_features table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS ml_features (
            feature_id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id TEXT NOT NULL,
            snapshot_time TEXT NOT NULL,
            season TEXT,

            -- Basic match info
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            days_before_match REAL,

            -- Current snapshot odds (what we see NOW)
            home_odds_current REAL,
            draw_odds_current REAL,
            away_odds_current REAL,
            combined_inverse_current REAL,

            -- Historical odds (what we saw BEFORE - looking back)
            home_odds_1snapshot_ago REAL,
            home_odds_2snapshots_ago REAL,
            home_odds_3snapshots_ago REAL,
            draw_odds_1snapshot_ago REAL,
            draw_odds_2snapshots_ago REAL,
            draw_odds_3snapshots_ago REAL,
            away_odds_1snapshot_ago REAL,
            away_odds_2snapshots_ago REAL,
            away_odds_3snapshots_ago REAL,

            -- Calculated pattern features
            home_odds_change_rate REAL,
            draw_odds_change_rate REAL,
            away_odds_change_rate REAL,
            home_odds_volatility REAL,
            draw_odds_volatility REAL,
            away_odds_volatility REAL,

            -- TARGETS (what we want to predict - looking forward)
            has_future_arbitrage BOOLEAN,
            max_future_profit_percent REAL,
            snapshots_until_best_opportunity INTEGER,

            -- Metadata
            created_at TEXT,

            FOREIGN KEY (match_id) REFERENCES matches(match_id)
        )
        ''')

        # Create indexes for efficient querying
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_ml_match 
        ON ml_features(match_id)
        ''')

        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_ml_season 
        ON ml_features(season)
        ''')

        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_ml_arbitrage 
        ON ml_features(has_future_arbitrage, max_future_profit_percent)
        ''')

        conn.commit()

        # Verify the table was created
        cursor.execute('''
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='ml_features'
        ''')

        if cursor.fetchone():
            print("✓ Table 'ml_features' created successfully")

            # Show table structure
            cursor.execute("PRAGMA table_info(ml_features)")
            columns = cursor.fetchall()
            print(f"\nTable has {len(columns)} columns:")
            for col in columns[:5]:  # Show first 5
                print(f"  - {col[1]} ({col[2]})")
            print(f"  ... and {len(columns) - 5} more columns")

            print("\n✓ Indexes created successfully")
            print("\nMigration completed successfully!")
            return True
        else:
            print("✗ Table creation failed")
            return False

    except sqlite3.Error as e:
        print(f"✗ Migration failed: {e}")
        conn.rollback()
        return False

    finally:
        conn.close()


def rollback_migration(db_path='../../data/raw/epl_arbitrage.db'):
    """Rollback: Drop the ml_features table"""

    print(f"Rolling back migration on: {db_path}")
    print("Dropping ml_features table...")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute("DROP TABLE IF EXISTS ml_features")
        conn.commit()
        print("✓ Rollback completed successfully")
        return True
    except sqlite3.Error as e:
        print(f"✗ Rollback failed: {e}")
        return False
    finally:
        conn.close()


def main():
    """Run the migration"""
    import argparse

    parser = argparse.ArgumentParser(description='ML Features Table Migration')
    parser.add_argument('--rollback', action='store_true',
                        help='Rollback the migration (drop table)')
    parser.add_argument('--db-path', default='../../data/raw/epl_arbitrage.db',
                        help='Path to the database file')

    args = parser.parse_args()

    if args.rollback:
        rollback_migration(args.db_path)
    else:
        run_migration(args.db_path)


if __name__ == "__main__":
    main()