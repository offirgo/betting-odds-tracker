import requests
import pandas as pd
import time
import json
import logging
from datetime import datetime, timezone, timedelta
import sqlite3
import os
from dotenv import load_dotenv
import argparse
import traceback


#TODO 2. run automaticall
#TODO run for a week or so and then start tinkering with the data


class OddsTracker:
    def __init__(self, sport='soccer', regions=['uk', 'eu'], markets=['h2h', 'spreads', 'totals'],
                 raw_data_dir='../../data/raw',snapshot_dir = '../../data/snapshots' ,log_file='../../logs/odds_tracker.log'):
        load_dotenv()
        self.api_key = os.environ.get("ODDS_API_KEY")
        self.sport = sport
        self.regions = regions
        self.markets = markets
        self.base_url = "https://api.the-odds-api.com/v4/sports"

        self.setup_logging(log_file)
        self.logger.info("OddsTracker initialized")

        os.makedirs(raw_data_dir, exist_ok=True)
        self.db_path = os.path.join(raw_data_dir, "odds_history.db")
        self.json_backup_dir = snapshot_dir
        self.setup_database()

    def setup_database(self):
        self.logger.info("Database setup complete")

        """Create SQLite database for storing odds history"""
        try:

            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Create tables if they don't exist
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                sport TEXT,
                home_team TEXT,
                away_team TEXT,
                commence_time TEXT,
                created_at TEXT
            )
            ''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS odds_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT,
                bookmaker TEXT,
                market TEXT,
                outcome TEXT,
                price REAL,
                timestamp TEXT,
                FOREIGN KEY (event_id) REFERENCES events (event_id)
            )
            ''')
            # Add indexes for frequently queried columns
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_odds_event_id ON odds_history(event_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_odds_bookmaker ON odds_history(bookmaker)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_odds_market ON odds_history(market)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_odds_timestamp ON odds_history(timestamp)')

            # Compound indexes for common query combinations
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_odds_event_bookmaker ON odds_history(event_id, bookmaker)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_odds_event_market ON odds_history(event_id, market)')

            conn.commit()
            conn.close()
            self.logger.info("Database setup complete")

        except Exception as e:
            self.logger.error(f"Database setup error: {e}\n{traceback.format_exc()}")
            raise
    def setup_logging(self, log_file):

        """Configure logging to file and console"""
        self.logger = logging.getLogger('OddsTracker')
        self.logger.setLevel(logging.INFO)

        # Create log directory if it doesn't exist
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # File handler
        file_handler = logging.FileHandler(log_file)
        file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_format)
        self.logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(file_format)
        self.logger.addHandler(console_handler)
    def fetch_odds(self):
        """Fetch current odds from the API"""
        odds_data = []

        for region in self.regions:
            url = f"{self.base_url}/{self.sport}/odds"
            params = {
                'apiKey': self.api_key,
                'regions': region,
                'markets': ','.join(self.markets),
                'dateFormat': 'iso'
            }
            try:
                self.logger.info(f"Fetching odds for {region} region")
                response = requests.get(url, params=params)

                if response.status_code == 200:
                    data = response.json()
                    odds_data.extend(data)
                    self.logger.info(f"Fetched {len(data)} events for {region} region")
                else:
                    self.logger.error(f"Error fetching data for {region}: {response.status_code}")
                    self.logger.error(response.text)
            except Exception as e:
                self.logger.error(f"Exception during API request for {region}: {e}\n{traceback.format_exc()}")

        self.logger.info(f"Total events fetched: {len(odds_data)}")
        return odds_data

    def store_odds(self, odds_data):
        try:
            """Store odds data in SQLite database"""
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            current_time =  datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            events_added = 0
            odds_records_added = 0

            for event in odds_data:
                # Insert or update event data
                event_id = event['id']
                cursor.execute('''
                INSERT OR IGNORE INTO events (event_id, sport, home_team, away_team, commence_time, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    event_id,
                    self.sport,
                    event['home_team'],
                    event['away_team'],
                    event['commence_time'],
                    current_time
                ))
                events_added += cursor.rowcount

                # Insert odds data
                for bookmaker in event['bookmakers']:
                    for market in bookmaker['markets']:
                        market_name = market['key']
                        for outcome in market['outcomes']:
                            cursor.execute('''
                            INSERT INTO odds_history (event_id, bookmaker, market, outcome, price, timestamp)
                            VALUES (?, ?, ?, ?, ?, ?)
                            ''', (
                                event_id,
                                bookmaker['key'],
                                market_name,
                                outcome['name'],
                                outcome['price'],
                                current_time
                            ))
                            odds_records_added += 1

            conn.commit()
            self.logger.info(f"Stored odds data at {current_time}: {events_added} events, {odds_records_added} odds records")
            conn.close()

            stats_file = "../../logs/collection_stats.csv"
            exists = os.path.exists(stats_file)
            with open(stats_file, "a") as f:
                if not exists:
                    f.write("timestamp,events,odds_records\n")
                f.write(f"{current_time},{events_added},{odds_records_added}\n")
            return events_added, odds_records_added
        except Exception as e:
            self.logger.error(f"Error storing odds data: {e}\n{traceback.format_exc()}")
            raise

    def save_json_backup(self, odds_data):
        """Save a JSON backup of the odds data"""
        try:
            # Create backups directory if it doesn't exist
            backup_dir = "odds_backups"
            if not os.path.exists(backup_dir):
                os.makedirs(backup_dir)

            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            backup_path = os.path.join(self.json_backup_dir, f"odds_snapshot_{timestamp}.json")

            with open(backup_path, "w") as f:
                json.dump(odds_data, f)

            self.logger.info(f"Backup saved to {backup_path}")

            # Clean up old backups (keep only the last 20)
            backup_files = sorted([os.path.join(self.json_backup_dir, f) for f in os.listdir(self.json_backup_dir)
                                   if f.startswith("odds_snapshot_") and f.endswith(".json")])

            if len(backup_files) > 20:
                files_to_delete = backup_files[:-20]
                for file_path in files_to_delete:
                    os.remove(file_path)
                    self.logger.info(f"Deleted old backup: {file_path}")
        except Exception as e:
            self.logger.error(f"Error saving JSON backup: {e}\n{traceback.format_exc()}")

    def run_collection(self, interval_hours=6, days=7):
        """Run data collection at specified interval for a number of days"""
        intervals = int((days * 24) / interval_hours)
        self.logger.info(f"Starting data collection: {intervals} intervals over {days} days (every {interval_hours} hours)")

        for i in range(intervals):
            try:
                current_time_obj = datetime.now(timezone.utc)
                current_time = current_time_obj.strftime("%Y-%m-%d %H:%M:%S")
                self.logger.info(f"Collection cycle {i + 1}/{intervals} started at {current_time}")

                odds_data = self.fetch_odds()
                events, odds = self.store_odds(odds_data)

                self.save_json_backup(odds_data)

                db_size = os.path.getsize(self.db_path) / (1024 * 1024)  # Size in MB
                self.logger.info(f"Current database size: {db_size:.2f} MB")


                # Wait for next interval (unless it's the last one)
                if i < intervals - 1:
                    next_collection_obj = current_time_obj + timedelta(hours=float(interval_hours))
                    next_collection = next_collection_obj.strftime("%Y-%m-%d %H:%M:%S")

                    wait_seconds = interval_hours * 3600
                    self.logger.info(f"Waiting {interval_hours} hours until next collection at {next_collection}")
                    time.sleep(wait_seconds)


            except Exception as e:
                self.logger.error(f"Error in collection cycle {i + 1}: {e}\n{traceback.format_exc()}")
                self.logger.info("Will attempt to continue with next cycle")
                time.sleep(300)

    def query_odds_history(self, event_id=None, bookmaker=None, market=None):
        """Query the database for historical odds movements"""
        try:
            conn = sqlite3.connect(self.db_path)
            query = "SELECT * FROM odds_history WHERE 1=1"
            params = []

            if event_id:
                query += " AND event_id = ?"
                params.append(event_id)

            if bookmaker:
                query += " AND bookmaker = ?"
                params.append(bookmaker)

            if market:
                query += " AND market = ?"
                params.append(market)

            self.logger.info(f"Running query: {query} with params {params}")
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            self.logger.info(f"Query returned {len(df)} records")
            return df
        except Exception as e:
            self.logger.error(f"Error querying odds history: {e}\n{traceback.format_exc()}")
            raise


def main():
    parser = argparse.ArgumentParser(description='Betting Odds Tracker')
    # parser.add_argument('--api-key', required=True, help='API key for the-odds-api.com')
    parser.add_argument('--sport', default='soccer', help='Sport to track (default: soccer)')
    parser.add_argument('--regions', default='uk,eu', help='Comma-separated list of regions (default: uk,eu)')
    parser.add_argument('--markets', default='h2h,spreads,totals',
                        help='Comma-separated list of markets (default: h2h,spreads,totals)')
    parser.add_argument('--interval', type=float, default=6, help='Collection interval in hours (default: 6)')
    parser.add_argument('--days', type=int, default=30, help='Number of days to run (default: 30)')
    parser.add_argument('--log-file', default='logs/odds_tracker.log',
                        help='Log file path (default: logs/odds_tracker.log)')

    args = parser.parse_args()

    # Initialize tracker
    tracker = OddsTracker(
        sport=args.sport,
        regions=args.regions.split(','),
        markets=args.markets.split(','),
        log_file=args.log_file
    )

    # Run collection
    tracker.run_collection(interval_hours=args.interval, days=args.days)


if __name__ == "__main__":
    main()