#!/usr/bin/env python3
"""
Arbitrage Betting Simulator - Works directly with ml_features data

This simulator tests arbitrage betting strategies on historical data.
In arbitrage betting, we bet on ALL outcomes of a match at different bookmakers
to guarantee profit regardless of the outcome.

Example: If we find:
  - Home win at 2.1 (Bookmaker A)
  - Draw at 4.0 (Bookmaker B)
  - Away win at 3.5 (Bookmaker C)

Combined inverse = 1/2.1 + 1/4.0 + 1/3.5 = 0.976 < 1.0

This means we can distribute £100 across all three bets and guarantee profit!
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
import json


class ArbitrageSimulator:
    """Simulates arbitrage betting on historical odds data."""

    def __init__(self, db_path):
        """Initialize the simulator."""
        self.db_path = db_path
        self.conn = None

    def connect(self):
        """Connect to database."""
        self.conn = sqlite3.connect(self.db_path)

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()

    def calculate_arbitrage_stakes(self, home_odds, draw_odds, away_odds, total_stake=100):
        """
        Calculate optimal stake distribution for arbitrage betting.

        Args:
            home_odds: Decimal odds for home win
            draw_odds: Decimal odds for draw
            away_odds: Decimal odds for away win
            total_stake: Total amount to bet across all outcomes

        Returns:
            dict: Stakes for each outcome and guaranteed profit
        """
        # Calculate combined inverse (arbitrage indicator)
        combined_inverse = (1/home_odds) + (1/draw_odds) + (1/away_odds)

        # No arbitrage if combined_inverse >= 1.0
        if combined_inverse >= 1.0:
            return None

        # Calculate optimal stakes (proportional to inverse odds)
        home_stake = total_stake * (1/home_odds) / combined_inverse
        draw_stake = total_stake * (1/draw_odds) / combined_inverse
        away_stake = total_stake * (1/away_odds) / combined_inverse

        # Calculate guaranteed return (same regardless of outcome)
        guaranteed_return = home_stake * home_odds  # Same for all outcomes
        guaranteed_profit = guaranteed_return - total_stake
        profit_percent = (guaranteed_profit / total_stake) * 100

        return {
            'home_stake': home_stake,
            'draw_stake': draw_stake,
            'away_stake': away_stake,
            'total_stake': total_stake,
            'guaranteed_return': guaranteed_return,
            'guaranteed_profit': guaranteed_profit,
            'profit_percent': profit_percent,
            'combined_inverse': combined_inverse
        }

    def run_season_simulation(self, season, initial_bankroll=10000,
                             bet_amount=100, min_profit_pct=1.0,
                             use_timing_models=False):
        """
        Run arbitrage simulation for a full season.

        Args:
            season: Season to simulate (e.g., '21/22')
            initial_bankroll: Starting bankroll
            bet_amount: Amount to stake per arbitrage opportunity
            min_profit_pct: Minimum profit percentage to accept arbitrage (default 1%)
            use_timing_models: If True, only bet when timing models say 'now'

        Returns:
            dict: Simulation results
        """
        self.connect()

        print(f"\n{'='*70}")
        print(f"ARBITRAGE BETTING SIMULATION - SEASON {season}")
        print(f"{'='*70}")
        print(f"Initial Bankroll: £{initial_bankroll:,.2f}")
        print(f"Bet Amount per Opportunity: £{bet_amount:,.2f}")
        print(f"Minimum Profit Threshold: {min_profit_pct}%")
        print(f"Using Timing Models: {use_timing_models}")
        print(f"{'='*70}\n")

        # Load all ml_features for the season
        query = """
            SELECT * FROM ml_features
            WHERE season = ?
            ORDER BY match_id, snapshot_time
        """
        df = pd.read_sql(query, self.conn, params=(season,))

        print(f"Loaded {len(df):,} snapshots for {df['match_id'].nunique()} matches\n")

        # Track simulation state
        bankroll = initial_bankroll
        bets_placed = []
        matches_tracked = {}

        # Process each match
        for match_id in df['match_id'].unique():
            match_df = df[df['match_id'] == match_id].sort_values('snapshot_time')

            # Track if we've already bet on this match
            if match_id in matches_tracked:
                continue

            # Get match info
            home_team = match_df.iloc[0]['home_team']
            away_team = match_df.iloc[0]['away_team']

            # Look for arbitrage opportunities in each snapshot
            for idx, row in match_df.iterrows():
                # Skip if we don't have all odds
                if pd.isna(row['home_odds_current']) or pd.isna(row['draw_odds_current']) or pd.isna(row['away_odds_current']):
                    continue

                # Check if timing models say to bet (if we're using them)
                if use_timing_models:
                    # For arbitrage, we need ALL three outcomes to be "ready"
                    # This is strict but ensures we're following the model
                    if not (row['should_bet_home_now'] and
                           row['should_bet_draw_now'] and
                           row['should_bet_away_now']):
                        continue

                # Calculate arbitrage opportunity
                arb = self.calculate_arbitrage_stakes(
                    row['home_odds_current'],
                    row['draw_odds_current'],
                    row['away_odds_current'],
                    total_stake=bet_amount
                )

                # Check if profitable arbitrage exists
                if arb and arb['profit_percent'] >= min_profit_pct:
                    # Check if we have enough bankroll
                    if bankroll >= bet_amount:
                        # Place the arbitrage bets
                        bets_placed.append({
                            'match_id': match_id,
                            'home_team': home_team,
                            'away_team': away_team,
                            'snapshot_time': row['snapshot_time'],
                            'days_before_match': row['days_before_match'],
                            'hours_before_match': row['hours_before_match'],
                            'home_odds': row['home_odds_current'],
                            'draw_odds': row['draw_odds_current'],
                            'away_odds': row['away_odds_current'],
                            'combined_inverse': arb['combined_inverse'],
                            'total_stake': arb['total_stake'],
                            'home_stake': arb['home_stake'],
                            'draw_stake': arb['draw_stake'],
                            'away_stake': arb['away_stake'],
                            'guaranteed_profit': arb['guaranteed_profit'],
                            'profit_percent': arb['profit_percent']
                        })

                        # Update bankroll
                        bankroll -= arb['total_stake']
                        bankroll += arb['guaranteed_return']

                        # Mark match as bet on
                        matches_tracked[match_id] = True

                        print(f"✓ {home_team} vs {away_team}")
                        print(f"  {row['days_before_match']:.1f} days before | "
                              f"Profit: £{arb['guaranteed_profit']:.2f} ({arb['profit_percent']:.2f}%) | "
                              f"Bankroll: £{bankroll:,.2f}")

                        # Move to next match
                        break

        self.close()

        # Calculate final stats
        total_staked = sum(b['total_stake'] for b in bets_placed)
        total_profit = sum(b['guaranteed_profit'] for b in bets_placed)
        final_bankroll = bankroll
        roi = ((final_bankroll - initial_bankroll) / initial_bankroll) * 100

        results = {
            'season': season,
            'initial_bankroll': initial_bankroll,
            'final_bankroll': final_bankroll,
            'total_profit': total_profit,
            'roi': roi,
            'bet_amount': bet_amount,
            'min_profit_pct': min_profit_pct,
            'use_timing_models': use_timing_models,
            'total_matches_available': df['match_id'].nunique(),
            'arbitrage_opportunities_found': len(bets_placed),
            'total_staked': total_staked,
            'average_profit_per_bet': total_profit / len(bets_placed) if bets_placed else 0,
            'average_profit_pct': np.mean([b['profit_percent'] for b in bets_placed]) if bets_placed else 0,
            'min_profit_achieved': min([b['profit_percent'] for b in bets_placed]) if bets_placed else 0,
            'max_profit_achieved': max([b['profit_percent'] for b in bets_placed]) if bets_placed else 0,
            'bets': bets_placed
        }

        return results

    def print_results(self, results):
        """Print simulation results in a nice format."""
        print(f"\n{'='*70}")
        print(f"SIMULATION RESULTS - SEASON {results['season']}")
        print(f"{'='*70}\n")

        print("FINANCIAL SUMMARY:")
        print(f"  Initial Bankroll:     £{results['initial_bankroll']:>12,.2f}")
        print(f"  Final Bankroll:       £{results['final_bankroll']:>12,.2f}")
        print(f"  Total Profit:         £{results['total_profit']:>12,.2f}")
        print(f"  ROI:                  {results['roi']:>13.2f}%\n")

        print("BETTING ACTIVITY:")
        print(f"  Total Matches:        {results['total_matches_available']:>14,}")
        print(f"  Arbitrages Found:     {results['arbitrage_opportunities_found']:>14,}")
        print(f"  Coverage:             {(results['arbitrage_opportunities_found']/results['total_matches_available']*100):>13.1f}%")
        print(f"  Total Staked:         £{results['total_staked']:>12,.2f}\n")

        print("PROFIT METRICS:")
        print(f"  Avg Profit/Bet:       £{results['average_profit_per_bet']:>12,.2f}")
        print(f"  Avg Profit %:         {results['average_profit_pct']:>13.2f}%")
        print(f"  Min Profit %:         {results['min_profit_achieved']:>13.2f}%")
        print(f"  Max Profit %:         {results['max_profit_achieved']:>13.2f}%\n")

        print(f"{'='*70}\n")


if __name__ == "__main__":
    # Database path
    db_path = "../../data/raw/epl_arbitrage.db"

    # Create simulator
    simulator = ArbitrageSimulator(db_path)

    # Run simulation for season 21/22
    print("Running arbitrage simulation...")
    results = simulator.run_season_simulation(
        season='21/22',
        initial_bankroll=10000,
        bet_amount=100,
        min_profit_pct=1.0,  # Only take arbitrages with 1%+ profit
        use_timing_models=False  # Set to True to use ML timing predictions
    )

    # Print results
    simulator.print_results(results)

    # Save detailed results to JSON
    output_file = '../../data/simulation_results_21-22.json'
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    print(f"Detailed results saved to: {output_file}")
