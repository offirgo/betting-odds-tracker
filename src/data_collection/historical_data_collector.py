#!/usr/bin/env python3
"""
Step 1: Fetch historical odds data from The Odds API and save as JSON
"""

import requests
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time


class HistoricalOddsCollector:
    def __init__(self, backup_dir='../../data/historical_snapshots'):
        load_dotenv()
        self.api_key = os.environ.get("ODDS_API_KEY")
        self.base_url = "https://api.the-odds-api.com/v4/historical/sports"
        self.backup_dir = backup_dir

        # Create backup directory if it doesn't exist
        os.makedirs(self.backup_dir, exist_ok=True)

        if not self.api_key:
            raise ValueError("ODDS_API_KEY not found in environment variables")

    def check_if_snapshot_exists(self, date_iso):
        """
        Check if a JSON file for this date already exists

        Args:
            date_iso: ISO 8601 format timestamp

        Returns:
            tuple: (exists: bool, filepath: str or None)
        """
        # Generate expected filename
        clean_timestamp = date_iso.replace(':', '-').replace('Z', '')
        expected_filename = f"odds_snapshot_{clean_timestamp}.json"
        filepath = os.path.join(self.backup_dir, expected_filename)

        if os.path.exists(filepath):
            return True, filepath
        return False, None

    def fetch_historical_snapshot(self, sport='soccer_epl', regions='uk',
                                  markets='h2h', date_iso=None, skip_if_exists=True):
        """
        Fetch a single historical odds snapshot

        Args:
            sport: Sport key (e.g., 'soccer_epl')
            regions: Comma-separated regions (e.g., 'uk,eu')
            markets: Comma-separated markets (e.g., 'h2h')
            date_iso: ISO 8601 format timestamp (e.g., '2024-08-09T12:00:00Z')
            skip_if_exists: If True, skip API call if JSON already exists

        Returns:
            dict: API response with odds data, or None if skipped
        """
        # Check if file already exists
        if skip_if_exists:
            exists, filepath = self.check_if_snapshot_exists(date_iso)
            if exists:
                print(f"Snapshot for {date_iso} already exists at {filepath}")
                print(f"Skipping API call to save credits")
                return None

        url = f"{self.base_url}/{sport}/odds"

        params = {
            'apiKey': self.api_key,
            'regions': regions,
            'markets': markets,
            'date': date_iso,
            'oddsFormat': 'decimal',
            'dateFormat': 'iso'
        }

        print(f"Fetching snapshot for {date_iso}...")
        print(f"URL: {url}")
        print(f"Params: {params}")

        try:
            response = requests.get(url, params=params)

            # Print quota information
            if 'x-requests-remaining' in response.headers:
                print(f"API Quota - Remaining: {response.headers.get('x-requests-remaining')}, "
                      f"Used: {response.headers.get('x-requests-used')}, "
                      f"Last Cost: {response.headers.get('x-requests-last')}")

            if response.status_code == 200:
                data = response.json()

                # Check if we got data
                events = data.get('data', [])
                timestamp = data.get('timestamp', 'unknown')

                print(f"Success! Retrieved {len(events)} events")
                print(f"Actual snapshot timestamp: {timestamp}")

                return data
            else:
                print(f"Error: Status {response.status_code}")
                print(f"Response: {response.text}")
                return None

        except Exception as e:
            print(f"Exception occurred: {e}")
            return None

    def save_snapshot_to_json(self, data, custom_filename=None, date_iso=None):
        """
        Save snapshot data to JSON file

        Args:
            data: The API response data
            custom_filename: Optional custom filename
            date_iso: The date_iso used in the API call (for consistent naming)

        Returns:
            str: Path to saved file
        """
        if not data:
            print("No data to save")
            return None

        if custom_filename:
            filename = custom_filename
        else:
            # Use the date_iso parameter if provided, otherwise fall back to timestamp from data
            if date_iso:
                clean_timestamp = date_iso.replace(':', '-').replace('Z', '')
            else:
                timestamp = data.get('timestamp', datetime.now().isoformat())
                clean_timestamp = timestamp.replace(':', '-').replace('Z', '')

            filename = f"odds_snapshot_{clean_timestamp}.json"

        filepath = os.path.join(self.backup_dir, filename)

        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)

        print(f"Saved snapshot to: {filepath}")
        return filepath

    def collect_date_range(self, start_date_iso, end_date_iso,
                           time_of_day='12:00:00', delay_seconds=2):
        """
        Collect snapshots for a range of dates

        Args:
            start_date_iso: Start date in 'YYYY-MM-DD' format
            end_date_iso: End date in 'YYYY-MM-DD' format
            time_of_day: Time to use for all snapshots (HH:MM:SS)
            delay_seconds: Delay between API calls to avoid rate limiting

        Returns:
            list: Paths to saved JSON files
        """
        start_date = datetime.fromisoformat(start_date_iso)
        end_date = datetime.fromisoformat(end_date_iso)

        current_date = start_date
        saved_files = []

        while current_date <= end_date:
            # Format date with time
            date_str = f"{current_date.strftime('%Y-%m-%d')}T{time_of_day}Z"

            # Fetch snapshot (will skip if exists)
            data = self.fetch_historical_snapshot(date_iso=date_str)

            if data:
                # Save to JSON
                filepath = self.save_snapshot_to_json(data,date_iso=date_str)
                saved_files.append(filepath)

            # Move to next day
            current_date += timedelta(days=1)

            # Delay to avoid rate limiting (only if we actually made an API call)
            if data and current_date <= end_date:
                print(f"Waiting {delay_seconds} seconds before next request...\n")
                time.sleep(delay_seconds)

        print(f"\nCompleted! Saved {len(saved_files)} snapshots")
        return saved_files

    def test_single_snapshot(self):
        """Test fetching a single snapshot"""
        print("Testing single snapshot fetch...")

        # Test with August 10, 2024
        test_date = "2024-08-10T12:00:00Z"

        data = self.fetch_historical_snapshot(date_iso=test_date)

        if data:
            print("\nSnapshot structure:")
            print(f"  - timestamp: {data.get('timestamp')}")
            print(f"  - previous_timestamp: {data.get('previous_timestamp')}")
            print(f"  - next_timestamp: {data.get('next_timestamp')}")
            print(f"  - number of events: {len(data.get('data', []))}")

            # Show first event details
            if data.get('data'):
                first_event = data['data'][0]
                print(f"\nFirst event sample:")
                print(f"  - Match: {first_event.get('home_team')} vs {first_event.get('away_team')}")
                print(f"  - Commence: {first_event.get('commence_time')}")
                print(f"  - Bookmakers: {len(first_event.get('bookmakers', []))}")

            # Save it
            filepath = self.save_snapshot_to_json(data,date_iso=test_date)

            # Verify saved file
            if filepath and os.path.exists(filepath):
                print(f"\nVerifying saved file...")
                with open(filepath, 'r') as f:
                    loaded_data = json.load(f)
                print(f"File verified: {len(loaded_data.get('data', []))} events loaded")

            return data
        else:
            print("File already exists or failed to fetch snapshot")
            return None


def main():
    """Main execution"""
    print("=" * 60)
    print("Historical Odds Data Collector - Step 1: Fetch & Save JSON")
    print("=" * 60)

    collector = HistoricalOddsCollector()

    # Test with a single snapshot first
    print("\n--- Testing Single Snapshot ---")
    data = collector.test_single_snapshot()

    print("\n" + "=" * 60)
    print("Collector ready!")
    print("  1. ✓ Can fetch historical data from API")
    print("  2. ✓ Can save to JSON format")
    print("  3. ✓ Skips existing files to save credits")
    print("=" * 60)


if __name__ == "__main__":
    main()