#!/usr/bin/env python3
"""
Real-time Odds Data Fetcher

Fetches live odds from The Odds API for EPL matches.
Free tier: 500 requests/month

API: https://the-odds-api.com/
"""

import requests
import json
from datetime import datetime, timezone
import time
import os


class OddsFetcher:
    """Fetches real-time odds from The Odds API."""

    def __init__(self, api_key=None):
        """
        Initialize odds fetcher.

        Args:
            api_key: The Odds API key (get from https://the-odds-api.com/)
        """
        self.api_key = api_key or os.getenv('ODDS_API_KEY')
        if not self.api_key:
            raise ValueError("API key required. Set ODDS_API_KEY environment variable or pass api_key parameter")

        self.base_url = "https://api.the-odds-api.com/v4"
        self.sport = "soccer_epl"  # English Premier League
        self.regions = "uk"  # UK bookmakers
        self.markets = "h2h"  # Head-to-head (1X2)
        self.odds_format = "decimal"

    def get_remaining_requests(self):
        """Check how many API requests remain this month."""
        url = f"{self.base_url}/sports/{self.sport}/odds"
        params = {
            'apiKey': self.api_key,
            'regions': self.regions,
            'markets': self.markets,
            'oddsFormat': self.odds_format
        }

        response = requests.get(url, params=params)

        if response.status_code == 200:
            remaining = response.headers.get('x-requests-remaining')
            used = response.headers.get('x-requests-used')
            return {
                'remaining': int(remaining) if remaining else None,
                'used': int(used) if used else None
            }
        else:
            print(f"Error checking requests: {response.status_code}")
            return None

    def fetch_live_odds(self):
        """
        Fetch current odds for upcoming EPL matches.

        Returns:
            List of matches with odds from multiple bookmakers
        """
        url = f"{self.base_url}/sports/{self.sport}/odds"
        params = {
            'apiKey': self.api_key,
            'regions': self.regions,
            'markets': self.markets,
            'oddsFormat': self.odds_format
        }

        print(f"Fetching live odds from The Odds API...")
        response = requests.get(url, params=params)

        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            print(response.text)
            return None

        # Check API usage
        remaining = response.headers.get('x-requests-remaining')
        used = response.headers.get('x-requests-used')
        print(f"API Usage: {used} used, {remaining} remaining this month")

        data = response.json()
        return self.parse_odds_data(data)

    def parse_odds_data(self, raw_data):
        """
        Parse raw API response into our format.

        Args:
            raw_data: Raw JSON from The Odds API

        Returns:
            List of matches with structured odds data
        """
        matches = []

        for event in raw_data:
            match_data = {
                'match_id': event['id'],
                'home_team': event['home_team'],
                'away_team': event['away_team'],
                'commence_time': event['commence_time'],
                'commence_time_unix': int(datetime.fromisoformat(event['commence_time'].replace('Z', '+00:00')).timestamp()),
                'snapshot_time': datetime.now(timezone.utc).isoformat(),
                'snapshot_time_unix': int(time.time()),
                'bookmakers': []
            }

            # Parse bookmaker odds
            for bookmaker in event.get('bookmakers', []):
                bookmaker_data = {
                    'name': bookmaker['key'],
                    'title': bookmaker['title'],
                    'last_update': bookmaker['last_update']
                }

                # Extract H2H (home/draw/away) odds
                for market in bookmaker.get('markets', []):
                    if market['key'] == 'h2h':
                        odds = {}
                        for outcome in market['outcomes']:
                            if outcome['name'] == match_data['home_team']:
                                odds['home'] = outcome['price']
                            elif outcome['name'] == match_data['away_team']:
                                odds['away'] = outcome['price']
                            else:
                                odds['draw'] = outcome['price']

                        bookmaker_data['odds'] = odds
                        break

                match_data['bookmakers'].append(bookmaker_data)

            # Calculate best odds across all bookmakers
            match_data['best_odds'] = self.get_best_odds(match_data['bookmakers'])

            matches.append(match_data)

        return matches

    def get_best_odds(self, bookmakers):
        """
        Find the best available odds across all bookmakers.

        Args:
            bookmakers: List of bookmaker data

        Returns:
            Dict with best odds for each outcome
        """
        best = {
            'home': 0,
            'draw': 0,
            'away': 0,
            'home_bookmaker': None,
            'draw_bookmaker': None,
            'away_bookmaker': None
        }

        for bm in bookmakers:
            if 'odds' not in bm:
                continue

            odds = bm['odds']

            if odds.get('home', 0) > best['home']:
                best['home'] = odds['home']
                best['home_bookmaker'] = bm['name']

            if odds.get('draw', 0) > best['draw']:
                best['draw'] = odds['draw']
                best['draw_bookmaker'] = bm['name']

            if odds.get('away', 0) > best['away']:
                best['away'] = odds['away']
                best['away_bookmaker'] = bm['name']

        return best

    def check_arbitrage_opportunity(self, match):
        """
        Quick check if arbitrage opportunity exists.

        Args:
            match: Match data with odds

        Returns:
            Dict with arbitrage status and profit %
        """
        best = match['best_odds']

        if best['home'] == 0 or best['draw'] == 0 or best['away'] == 0:
            return {'has_arbitrage': False, 'profit_pct': 0}

        combined_inverse = (1/best['home']) + (1/best['draw']) + (1/best['away'])

        has_arbitrage = combined_inverse < 1.0

        if has_arbitrage:
            # Calculate guaranteed profit percentage
            stake = 100
            guaranteed_return = stake / combined_inverse
            profit = guaranteed_return - stake
            profit_pct = (profit / stake) * 100

            return {
                'has_arbitrage': True,
                'profit_pct': profit_pct,
                'combined_inverse': combined_inverse
            }
        else:
            return {
                'has_arbitrage': False,
                'profit_pct': 0,
                'combined_inverse': combined_inverse
            }

    def save_snapshot(self, matches, filepath):
        """Save odds snapshot to JSON file."""
        data = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'matches': matches
        }

        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)

        print(f"Saved snapshot to {filepath}")


if __name__ == "__main__":
    # Example usage
    print("The Odds API - Live Odds Fetcher")
    print("=" * 60)
    print()
    print("SETUP REQUIRED:")
    print("1. Get free API key from https://the-odds-api.com/")
    print("2. Set environment variable: export ODDS_API_KEY='your_key'")
    print("3. Free tier: 500 requests/month")
    print()

    api_key = os.getenv('ODDS_API_KEY')

    if not api_key:
        print("ERROR: ODDS_API_KEY environment variable not set")
        print("Run: export ODDS_API_KEY='your_api_key_here'")
        exit(1)

    fetcher = OddsFetcher(api_key)

    # Check remaining requests
    print("Checking API quota...")
    quota = fetcher.get_remaining_requests()
    if quota:
        print(f"Requests remaining: {quota['remaining']}/500")
        print()

    # Fetch live odds
    matches = fetcher.fetch_live_odds()

    if matches:
        print(f"\nFound {len(matches)} upcoming EPL matches:\n")

        for match in matches:
            print(f"{match['home_team']} vs {match['away_team']}")
            print(f"  Kick-off: {match['commence_time']}")

            best = match['best_odds']
            print(f"  Best odds: H {best['home']:.2f} | D {best['draw']:.2f} | A {best['away']:.2f}")

            arb = fetcher.check_arbitrage_opportunity(match)
            if arb['has_arbitrage']:
                print(f"  ✓ ARBITRAGE: {arb['profit_pct']:.2f}% profit")
            else:
                print(f"  ✗ No arbitrage ({arb['combined_inverse']:.4f})")

            print()

        # Save to file
        fetcher.save_snapshot(matches, '../../data/live_odds_snapshot.json')
