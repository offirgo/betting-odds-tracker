#!/usr/bin/env python3
"""
Migration v2: Create ml_features table with improved structure
- Per-outcome predictions (home/draw/away separately)
- All historical snapshots aggregated
- Team-specific features
"""

import sqlite3
import os
from datetime import datetime


def run_migration(db_path='../../data/raw/epl_arbitrage.db'):
    """Create the improved ml_features table"""

    if not os.path.exists(db_path):
        print(f"Error: Database not found at {db_path}")
        return False

    print(f"Running migration v2 on: {db_path}")
    print(f"Migration: Create ml_features table (version 2)")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 60)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Drop old version if exists
        cursor.execute("DROP TABLE IF EXISTS ml_features")

        # Create new ml_features table
        cursor.execute('''
        CREATE TABLE ml_features (
            feature_id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id TEXT NOT NULL,
            snapshot_time TEXT NOT NULL,
            season TEXT,

            -- ==== MATCH CONTEXT ====
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            days_before_match REAL,
            hours_before_match REAL,

            -- ==== CURRENT SNAPSHOT (what we see NOW) ====
            home_odds_current REAL,
            draw_odds_current REAL,
            away_odds_current REAL,
            combined_inverse_current REAL,

            -- ==== HISTORICAL AGGREGATES (all past snapshots) ====
            num_snapshots_seen INTEGER,  -- How many snapshots we've observed so far

            -- Home odds history
            home_odds_min_historical REAL,
            home_odds_max_historical REAL,
            home_odds_mean_historical REAL,
            home_odds_std_historical REAL,
            home_odds_trend REAL,  -- Linear regression slope

            -- Draw odds history
            draw_odds_min_historical REAL,
            draw_odds_max_historical REAL,
            draw_odds_mean_historical REAL,
            draw_odds_std_historical REAL,
            draw_odds_trend REAL,

            -- Away odds history
            away_odds_min_historical REAL,
            away_odds_max_historical REAL,
            away_odds_mean_historical REAL,
            away_odds_std_historical REAL,
            away_odds_trend REAL,

            -- Recent changes (last 3 snapshots if available)
            home_odds_change_recent REAL,  -- Change from 3 snapshots ago to now
            draw_odds_change_recent REAL,
            away_odds_change_recent REAL,

            -- ==== TEAM-SPECIFIC FEATURES ====
            -- These can be calculated from historical data or added manually
            home_team_id INTEGER,  -- Encoded team ID for ML
            away_team_id INTEGER,

            -- ==== TARGETS - PER OUTCOME (what we want to predict) ====
            -- For HOME outcome
            should_bet_home_now BOOLEAN,  -- Is NOW a good time to bet home?
            home_odds_will_improve BOOLEAN,  -- Will home odds get better later?
            best_future_home_odds REAL,  -- Best home odds that will appear later
            snapshots_until_best_home INTEGER,  -- How long until best home odds

            -- For DRAW outcome
            should_bet_draw_now BOOLEAN,
            draw_odds_will_improve BOOLEAN,
            best_future_draw_odds REAL,
            snapshots_until_best_draw INTEGER,

            -- For AWAY outcome
            should_bet_away_now BOOLEAN,
            away_odds_will_improve BOOLEAN,
            best_future_away_odds REAL,
            snapshots_until_best_away INTEGER,

            -- Overall arbitrage targets
            will_have_future_arbitrage BOOLEAN,  -- Combining best future odds
            max_future_profit_percent REAL,
            snapshots_until_arbitrage INTEGER,

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
        CREATE INDEX IF NOT EXISTS idx_ml_teams
        ON ml_features(home_team, away_team)
        ''')

        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_ml_days_before
        ON ml_features(days_before_match)
        ''')

        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_ml_targets
        ON ml_features(should_bet_home_now, should_bet_draw_now, should_bet_away_now)
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

            print(f"\nTable structure ({len(columns)} columns):")
            print("\nContext columns:")
            for col in columns[1:8]:  # Match context
                print(f"  - {col[1]} ({col[2]})")

            print("\nCurrent snapshot:")
            for col in columns[8:12]:
                print(f"  - {col[1]} ({col[2]})")

            print("\nHistorical aggregates:")
            for col in columns[12:28]:
                print(f"  - {col[1]} ({col[2]})")

            print(f"\nTeam features and targets: {len(columns) - 28} more columns")

            print("\n✓ Indexes created successfully")
            print("\nMigration v2 completed successfully!")
            print("\nKey improvements:")
            print("  - Per-outcome predictions (home/draw/away)")
            print("  - All historical snapshots aggregated")
            print("  - Team-specific features")
            print("  - Trend analysis (linear regression)")
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

    parser = argparse.ArgumentParser(description='ML Features Table Migration v2')
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