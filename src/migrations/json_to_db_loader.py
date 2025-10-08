#!/usr/bin/env python3
"""
Load saved JSON snapshots into the database
Separate from API fetching for better modularity
"""

import sqlite3
import json
import os
from datetime import datetime, timezone
import glob


class JSONToDatabaseLoader:
    def __init__(self, db_path='../../data/raw/epl_arbitrage.db'):
        self.db_path = db_path
        # Create directory if it doesn't exist
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
        self.setup_database()

    def setup_database(self):
        """Create database schema if it doesn't exist"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Matches table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS matches (
            match_id TEXT PRIMARY KEY,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            commence_time TEXT NOT NULL,
            season TEXT,
            created_at TEXT
        )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_commence_time ON matches(commence_time)')

        # Odds Snapshots table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS odds_snapshots (
            snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id TEXT NOT NULL,
            snapshot_time TEXT NOT NULL,
            days_before_match REAL,
            hours_before_match REAL,
            FOREIGN KEY (match_id) REFERENCES matches(match_id)
        )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_match_snapshot ON odds_snapshots(match_id, snapshot_time)')

        # Bookmaker Odds table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS bookmaker_odds (
            odds_id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_id INTEGER NOT NULL,
            bookmaker_key TEXT NOT NULL,
            bookmaker_title TEXT,
            home_odds REAL NOT NULL,
            draw_odds REAL NOT NULL,
            away_odds REAL NOT NULL,
            implied_probability_sum REAL,
            bookmaker_margin REAL,
            FOREIGN KEY (snapshot_id) REFERENCES odds_snapshots(snapshot_id)
        )
        ''')
        cursor.execute(
            'CREATE INDEX IF NOT EXISTS idx_snapshot_bookmaker ON bookmaker_odds(snapshot_id, bookmaker_key)')

        # Best Odds Analysis table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS best_odds_analysis (
            analysis_id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_id INTEGER NOT NULL,
            best_home_odds REAL,
            best_home_bookmaker TEXT,
            best_draw_odds REAL,
            best_draw_bookmaker TEXT,
            best_away_odds REAL,
            best_away_bookmaker TEXT,
            combined_inverse_odds REAL,
            arbitrage_opportunity BOOLEAN,
            potential_profit_percent REAL,
            FOREIGN KEY (snapshot_id) REFERENCES odds_snapshots(snapshot_id)
        )
        ''')

        # Match odds analysis table - one row per match with best odds across all time
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS match_odds_analysis (
            analysis_id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id TEXT NOT NULL,
            home_snapshot_id INTEGER,
            draw_snapshot_id INTEGER,
            away_snapshot_id INTEGER,
            home_odds REAL,
            home_bookmaker TEXT,
            home_days_before REAL,
            draw_odds REAL,
            draw_bookmaker TEXT,
            draw_days_before REAL,
            away_odds REAL,
            away_bookmaker TEXT,
            away_days_before REAL,
            combined_inverse_odds REAL,
            potential_profit_percent REAL,
            discovered_at TEXT,
            FOREIGN KEY (match_id) REFERENCES matches(match_id)
        )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_match_analysis ON match_odds_analysis(match_id)')

        conn.commit()
        conn.close()
        print(f"Database initialized at: {self.db_path}")

    def calculate_time_before_match(self, snapshot_time, commence_time):
        """Calculate days and hours before match"""
        snapshot_dt = datetime.fromisoformat(snapshot_time.replace('Z', '+00:00'))
        commence_dt = datetime.fromisoformat(commence_time.replace('Z', '+00:00'))

        time_diff = commence_dt - snapshot_dt
        days_before = time_diff.total_seconds() / (24 * 3600)
        hours_before = time_diff.total_seconds() / 3600

        return days_before, hours_before

    def load_json_file(self, json_path, season='24/25'):
        """
        Load a single JSON snapshot file into the database

        Args:
            json_path: Path to the JSON file
            season: Season identifier (e.g., '24/25')

        Returns:
            dict: Summary of what was loaded
        """
        print(f"\nLoading: {json_path}")

        # Load JSON
        with open(json_path, 'r') as f:
            data = json.load(f)

        snapshot_time = data.get('timestamp')
        events = data.get('data', [])

        if not snapshot_time or not events:
            print("  Warning: No timestamp or events found in JSON")
            return {'success': False, 'events': 0}

        print(f"  Snapshot time: {snapshot_time}")
        print(f"  Events in JSON: {len(events)}")

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        events_added = 0
        snapshots_added = 0
        odds_added = 0

        for event in events:
            match_id = event['id']
            home_team = event['home_team']
            away_team = event['away_team']
            commence_time = event['commence_time']

            # Calculate time before match
            days_before, hours_before = self.calculate_time_before_match(
                snapshot_time, commence_time
            )

            # Insert or update match
            cursor.execute('''
            INSERT OR REPLACE INTO matches 
            (match_id, home_team, away_team, commence_time, season, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (match_id, home_team, away_team, commence_time, season,
                  datetime.now(timezone.utc).isoformat()))

            if cursor.rowcount > 0:
                events_added += 1

            # Check if this snapshot already exists
            cursor.execute('''
            SELECT snapshot_id FROM odds_snapshots 
            WHERE match_id = ? AND snapshot_time = ?
            ''', (match_id, snapshot_time))

            existing = cursor.fetchone()

            if existing:
                print(f"  Snapshot already exists for match {match_id} at {snapshot_time}, skipping...")
                continue

            # Insert odds snapshot
            cursor.execute('''
            INSERT INTO odds_snapshots 
            (match_id, snapshot_time, days_before_match, hours_before_match)
            VALUES (?, ?, ?, ?)
            ''', (match_id, snapshot_time, days_before, hours_before))

            snapshot_id = cursor.lastrowid
            snapshots_added += 1

            # Track best odds for this snapshot
            best_home = {'odds': 0, 'bookmaker': None}
            best_draw = {'odds': 0, 'bookmaker': None}
            best_away = {'odds': 0, 'bookmaker': None}

            # Process bookmaker odds
            for bookmaker in event.get('bookmakers', []):
                bookmaker_key = bookmaker['key']
                bookmaker_title = bookmaker['title']

                # Find h2h market
                for market in bookmaker.get('markets', []):
                    if market['key'] == 'h2h':
                        outcomes = market['outcomes']

                        home_odds = draw_odds = away_odds = None

                        for outcome in outcomes:
                            if outcome['name'] == home_team:
                                home_odds = outcome['price']
                            elif outcome['name'] == away_team:
                                away_odds = outcome['price']
                            elif outcome['name'] == 'Draw':
                                draw_odds = outcome['price']

                        if home_odds and draw_odds and away_odds:
                            # Calculate bookmaker margin
                            implied_prob_sum = (1 / home_odds) + (1 / draw_odds) + (1 / away_odds)
                            margin = (implied_prob_sum - 1) * 100

                            # Track best odds
                            if home_odds > best_home['odds']:
                                best_home = {'odds': home_odds, 'bookmaker': bookmaker_key}
                            if draw_odds > best_draw['odds']:
                                best_draw = {'odds': draw_odds, 'bookmaker': bookmaker_key}
                            if away_odds > best_away['odds']:
                                best_away = {'odds': away_odds, 'bookmaker': bookmaker_key}

                            # Insert odds
                            cursor.execute('''
                            INSERT INTO bookmaker_odds 
                            (snapshot_id, bookmaker_key, bookmaker_title,
                             home_odds, draw_odds, away_odds,
                             implied_probability_sum, bookmaker_margin)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (snapshot_id, bookmaker_key, bookmaker_title,
                                  home_odds, draw_odds, away_odds,
                                  implied_prob_sum, margin))

                            odds_added += 1

            # Store best odds analysis
            if all([best_home['odds'], best_draw['odds'], best_away['odds']]):
                combined_inverse = (1 / best_home['odds']) + (1 / best_draw['odds']) + (1 / best_away['odds'])
                is_arbitrage = combined_inverse < 1.0
                profit_percent = ((1 - combined_inverse) * 100) if is_arbitrage else 0

                cursor.execute('''
                INSERT INTO best_odds_analysis
                (snapshot_id, best_home_odds, best_home_bookmaker,
                 best_draw_odds, best_draw_bookmaker,
                 best_away_odds, best_away_bookmaker,
                 combined_inverse_odds, arbitrage_opportunity, potential_profit_percent)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (snapshot_id,
                      best_home['odds'], best_home['bookmaker'],
                      best_draw['odds'], best_draw['bookmaker'],
                      best_away['odds'], best_away['bookmaker'],
                      combined_inverse, is_arbitrage, profit_percent))

        conn.commit()
        conn.close()

        summary = {
            'success': True,
            'events': events_added,
            'snapshots': snapshots_added,
            'odds_records': odds_added
        }

        print(f"  ✓ Loaded: {events_added} events, {snapshots_added} snapshots, {odds_added} odds records")
        return summary

    def load_directory(self, json_dir='../../data/historical_snapshots', season='24/25'):
        """
        Load all JSON files from a directory

        Args:
            json_dir: Directory containing JSON files
            season: Season identifier

        Returns:
            list: Summary for each file loaded
        """
        pattern = os.path.join(json_dir, 'odds_snapshot_*.json')
        json_files = glob.glob(pattern)

        if not json_files:
            print(f"No JSON files found in {json_dir}")
            return []

        print(f"\nFound {len(json_files)} JSON files to load")
        print("=" * 60)

        results = []
        for json_file in sorted(json_files):
            result = self.load_json_file(json_file, season)
            results.append(result)

        print("\n" + "=" * 60)
        print("SUMMARY:")
        total_events = sum(r['events'] for r in results if r['success'])
        total_snapshots = sum(r['snapshots'] for r in results if r['success'])
        total_odds = sum(r['odds_records'] for r in results if r['success'])

        print(f"  Files processed: {len(results)}")
        print(f"  Total events: {total_events}")
        print(f"  Total snapshots: {total_snapshots}")
        print(f"  Total odds records: {total_odds}")

        return results

    def verify_database(self):
        """Check what's in the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM matches")
        match_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM odds_snapshots")
        snapshot_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM bookmaker_odds")
        odds_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM best_odds_analysis WHERE arbitrage_opportunity = 1")
        arbitrage_count = cursor.fetchone()[0]

        conn.close()

        print("\n" + "=" * 60)
        print("DATABASE VERIFICATION:")
        print(f"  Matches: {match_count}")
        print(f"  Snapshots: {snapshot_count}")
        print(f"  Odds records: {odds_count}")
        print(f"  Arbitrage opportunities: {arbitrage_count}")
        print("=" * 60)


def main():
    """Test loading JSON into database"""
    print("JSON to Database Loader - Testing")
    print("=" * 60)

    # Initialize loader
    loader = JSONToDatabaseLoader()

    # Load all JSON files from the default directory
    loader.load_directory()

    # Verify what was loaded
    loader.verify_database()


if __name__ == "__main__":
    main()