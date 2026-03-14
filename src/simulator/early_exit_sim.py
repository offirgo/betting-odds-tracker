#!/usr/bin/env python3
"""
Optimization #3: Early Exit Strategy

Instead of always waiting for timing signal, exit early if:
1. Odds have improved significantly (creating arbitrage opportunity)
2. Odds improvement trend suggests optimal time to bet

This capitalizes on favorable odds movements without waiting for model signal.

Expected improvement: +3-5% profit
"""

import sqlite3
import pandas as pd
import numpy as np
import json
import sys


class EarlyExitSimulator:
    """Arbitrage simulator with early exit on favorable odds movements."""

    def __init__(self, db_path, threshold_version='original'):
        """
        Initialize simulator.

        Args:
            db_path: Path to database
            threshold_version: Which threshold labels to use
        """
        self.db_path = db_path
        self.threshold_version = threshold_version

        # Determine column suffix
        if threshold_version in ['original', '055', '0.55']:
            self.column_suffix = ''
            self.threshold_value = 0.55
        else:
            thresh_num = threshold_version.replace('.', '')
            self.column_suffix = f'_t{thresh_num}'
            self.threshold_value = float(threshold_version)

    def calculate_three_way_stakes(self, odds1, odds2, odds3, total_stake=100):
        """Calculate stakes for arbitrage."""
        combined_inverse = (1/odds1) + (1/odds2) + (1/odds3)

        if combined_inverse >= 1.0:
            return None

        stake1 = total_stake * (1/odds1) / combined_inverse
        stake2 = total_stake * (1/odds2) / combined_inverse
        stake3 = total_stake * (1/odds3) / combined_inverse

        guaranteed_return = stake1 * odds1
        guaranteed_profit = guaranteed_return - total_stake
        profit_percent = (guaranteed_profit / total_stake) * 100

        return {
            'stakes': (stake1, stake2, stake3),
            'total_stake': total_stake,
            'guaranteed_return': guaranteed_return,
            'guaranteed_profit': guaranteed_profit,
            'profit_percent': profit_percent,
            'combined_inverse': combined_inverse
        }

    def should_exit_early(self, bet1_odds, bet2_odds, bet3_odds_current, bet3_odds_initial,
                         min_profit_pct=1.0):
        """
        Determine if we should exit early based on odds improvement.

        Args:
            bet1_odds: Odds for first bet (already placed)
            bet2_odds: Odds for second bet (already placed)
            bet3_odds_current: Current odds for third outcome
            bet3_odds_initial: Initial odds for third outcome
            min_profit_pct: Minimum profit percentage to exit early

        Returns:
            (should_exit, profit_pct) tuple
        """
        # Check if odds have improved (increased)
        if bet3_odds_current <= bet3_odds_initial:
            return (False, 0)

        # Calculate potential arbitrage with current odds
        stakes_result = self.calculate_three_way_stakes(
            bet1_odds, bet2_odds, bet3_odds_current
        )

        if stakes_result is None:
            return (False, 0)

        # Exit early if we can lock in decent profit now
        if stakes_result['profit_percent'] >= min_profit_pct:
            return (True, stakes_result['profit_percent'])

        return (False, stakes_result['profit_percent'])

    def run_simulation(self, season, initial_bankroll=10000, bet_amount=100,
                      min_profit_pct=1.0):
        """
        Run arbitrage simulation with early exit strategy.

        Args:
            season: Season to simulate
            initial_bankroll: Starting capital
            bet_amount: Amount to bet per arbitrage
            min_profit_pct: Minimum profit % to trigger early exit
        """
        conn = sqlite3.connect(self.db_path)

        print(f"\n{'='*70}")
        print(f"EARLY EXIT SIMULATION - SEASON {season}")
        print(f"{'='*70}")
        print(f"Threshold Version: {self.threshold_version} ({self.threshold_value:.3f})")
        print(f"Initial Bankroll: £{initial_bankroll:,.2f}")
        print(f"Bet Amount: £{bet_amount:,.2f}")
        print(f"Min Profit for Early Exit: {min_profit_pct:.1f}%")
        print(f"{'='*70}\n")

        # Load all snapshots
        query = """
            SELECT * FROM ml_features
            WHERE season = ?
            ORDER BY match_id, snapshot_time DESC
        """
        df = pd.read_sql(query, conn, params=(season,))
        conn.close()

        print(f"Loaded {len(df):,} snapshots for {df['match_id'].nunique()} matches\n")

        # Group by match
        matches = df.groupby('match_id')

        # Track state
        bankroll = initial_bankroll
        completed_bets = []
        skipped_no_arb = 0
        skipped_no_odds = 0
        early_exits = 0
        signal_exits = 0
        fallback_exits = 0

        # Process each match
        for match_id, match_df in matches:
            match_df = match_df.sort_values('snapshot_time')

            first_row = match_df.iloc[0]
            home_team = first_row['home_team']
            away_team = first_row['away_team']

            # STEP 1: Check if match will have future arbitrage
            if first_row['will_have_future_arbitrage'] != 1:
                skipped_no_arb += 1
                continue

            # Check odds available
            if pd.isna(first_row['home_odds_current']) or pd.isna(first_row['draw_odds_current']) or pd.isna(first_row['away_odds_current']):
                skipped_no_odds += 1
                continue

            # STEP 2: Identify 2 highest odds, bet immediately
            odds_map = {
                'home': first_row['home_odds_current'],
                'draw': first_row['draw_odds_current'],
                'away': first_row['away_odds_current']
            }

            sorted_outcomes = sorted(odds_map.items(), key=lambda x: x[1], reverse=True)

            bet1_outcome, bet1_odds = sorted_outcomes[0]
            bet2_outcome, bet2_odds = sorted_outcomes[1]
            bet3_outcome, bet3_odds = sorted_outcomes[2]

            bet3_odds_initial = bet3_odds

            # STEP 3: Monitor for early exit OR timing signal
            bet3_placed = False
            bet3_odds_final = bet3_odds
            bet3_snapshot_time = None
            bet3_days_before = None
            exit_reason = None

            timing_column = f'should_bet_{bet3_outcome}_now{self.column_suffix}'

            for idx, row in match_df.iterrows():
                if row['snapshot_time'] == first_row['snapshot_time']:
                    continue

                # Update current odds
                odds_column = f'{bet3_outcome}_odds_current'
                if pd.notna(row[odds_column]):
                    bet3_odds_final = row[odds_column]

                # Check for early exit opportunity
                should_exit, potential_profit = self.should_exit_early(
                    bet1_odds, bet2_odds, bet3_odds_final, bet3_odds_initial,
                    min_profit_pct
                )

                if should_exit and not bet3_placed:
                    bet3_placed = True
                    bet3_snapshot_time = row['snapshot_time']
                    bet3_days_before = row['days_before_match']
                    exit_reason = 'early_exit'
                    early_exits += 1
                    break

                # Check for timing signal
                should_bet_now = row.get(timing_column, 0) == 1

                if should_bet_now and not bet3_placed:
                    bet3_placed = True
                    bet3_snapshot_time = row['snapshot_time']
                    bet3_days_before = row['days_before_match']
                    exit_reason = 'timing_signal'
                    signal_exits += 1
                    break

            # STEP 4: Fallback - last snapshot
            if not bet3_placed:
                last_row = match_df.iloc[-1]
                bet3_snapshot_time = last_row['snapshot_time']
                bet3_days_before = last_row['days_before_match']
                exit_reason = 'fallback'
                fallback_exits += 1

                odds_column = f'{bet3_outcome}_odds_current'
                if pd.notna(last_row[odds_column]):
                    bet3_odds_final = last_row[odds_column]

            # Calculate final arbitrage
            final_odds_list = {
                'home': bet1_odds if bet1_outcome == 'home' else (bet2_odds if bet2_outcome == 'home' else bet3_odds_final),
                'draw': bet1_odds if bet1_outcome == 'draw' else (bet2_odds if bet2_outcome == 'draw' else bet3_odds_final),
                'away': bet1_odds if bet1_outcome == 'away' else (bet2_odds if bet2_outcome == 'away' else bet3_odds_final)
            }

            stakes_result = self.calculate_three_way_stakes(
                final_odds_list['home'],
                final_odds_list['draw'],
                final_odds_list['away'],
                total_stake=bet_amount
            )

            if stakes_result is None:
                continue

            if bankroll < bet_amount:
                break

            # Complete the bet
            bankroll -= stakes_result['total_stake']
            bankroll += stakes_result['guaranteed_return']

            completed_bets.append({
                'match_id': match_id,
                'home_team': home_team,
                'away_team': away_team,
                'bet3_outcome': bet3_outcome,
                'bet3_odds_initial': bet3_odds_initial,
                'bet3_odds_final': bet3_odds_final,
                'odds_improvement': bet3_odds_final - bet3_odds_initial,
                'odds_improvement_pct': ((bet3_odds_final - bet3_odds_initial) / bet3_odds_initial) * 100,
                'days_before': bet3_days_before,
                'exit_reason': exit_reason,
                'total_stake': stakes_result['total_stake'],
                'profit': stakes_result['guaranteed_profit'],
                'profit_pct': stakes_result['profit_percent']
            })

        # Results
        total_profit = sum(b['profit'] for b in completed_bets)
        roi = ((bankroll - initial_bankroll) / initial_bankroll) * 100

        results = {
            'season': season,
            'threshold_version': self.threshold_version,
            'threshold_value': self.threshold_value,
            'min_profit_pct': min_profit_pct,
            'initial_bankroll': initial_bankroll,
            'final_bankroll': bankroll,
            'total_profit': total_profit,
            'roi': roi,
            'total_matches': len(matches),
            'skipped_no_future_arb': skipped_no_arb,
            'skipped_no_odds': skipped_no_odds,
            'arbitrages_completed': len(completed_bets),
            'early_exits': early_exits,
            'signal_exits': signal_exits,
            'fallback_exits': fallback_exits,
            'early_exit_rate': (early_exits / len(completed_bets) * 100) if completed_bets else 0,
            'coverage': (len(completed_bets) / len(matches)) * 100,
            'avg_profit': total_profit / len(completed_bets) if completed_bets else 0,
            'avg_profit_pct': np.mean([b['profit_pct'] for b in completed_bets]) if completed_bets else 0,
            'avg_odds_improvement': np.mean([b['odds_improvement'] for b in completed_bets]) if completed_bets else 0,
            'bets': completed_bets
        }

        return results

    def print_results(self, results):
        """Print results."""
        print(f"\n{'='*70}")
        print(f"FINAL RESULTS - SEASON {results['season']}")
        print(f"Threshold: {results['threshold_version']} ({results['threshold_value']:.3f})")
        print(f"{'='*70}\n")

        print("FINANCIAL:")
        print(f"  Initial: £{results['initial_bankroll']:>12,.2f}")
        print(f"  Final:   £{results['final_bankroll']:>12,.2f}")
        print(f"  Profit:  £{results['total_profit']:>12,.2f}")
        print(f"  ROI:     {results['roi']:>13.2f}%\n")

        print("EARLY EXIT STRATEGY:")
        print(f"  Min profit for early exit: {results['min_profit_pct']:.1f}%")
        print(f"  Early exits:        {results['early_exits']:>8,} ({results['early_exit_rate']:.1f}%)")
        print(f"  Timing signal exits:{results['signal_exits']:>8,}")
        print(f"  Fallback exits:     {results['fallback_exits']:>8,}")
        print(f"  Avg odds improvement: {results['avg_odds_improvement']:>6.3f}\n")

        print("MATCHES:")
        print(f"  Total:              {results['total_matches']:>8,}")
        print(f"  Completed arbs:     {results['arbitrages_completed']:>8,}")
        print(f"  Coverage:           {results['coverage']:>7.1f}%")
        print(f"  Avg Profit/Bet:     £{results['avg_profit']:>7,.2f}")
        print(f"  Avg Profit %:       {results['avg_profit_pct']:>7.2f}%\n")

        print(f"{'='*70}\n")


if __name__ == "__main__":
    db_path = "../../data/raw/epl_arbitrage.db"

    # Default to threshold 0.50
    threshold_version = '050'

    if len(sys.argv) > 1:
        threshold_version = sys.argv[1]

    print(f"\nRunning early exit simulation with threshold: {threshold_version}\n")

    sim = EarlyExitSimulator(db_path, threshold_version)
    results = sim.run_simulation('24/25', initial_bankroll=10000, bet_amount=100, min_profit_pct=1.0)

    if results:
        sim.print_results(results)

        # Save
        output_file = f'../../data/simulation_early_exit_{threshold_version}_24-25.json'
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"Results saved to: {output_file}")
    else:
        print("\nSimulation failed.")
        sys.exit(1)
