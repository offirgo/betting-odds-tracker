#!/usr/bin/env python3
"""
Optimization #7: Transaction Cost Awareness

Account for real-world transaction costs:
- Betting exchange commission (typically 2-5%)
- Skip arbitrages where profit < costs
- Focus on higher-quality opportunities

Expected improvement: +2-3% profit (by avoiding unprofitable bets)
"""

import sqlite3
import pandas as pd
import numpy as np
import json
import sys


class TransactionCostSimulator:
    """Arbitrage simulator accounting for transaction costs."""

    def __init__(self, db_path, threshold_version='original', commission_rate=0.02):
        """
        Initialize simulator.

        Args:
            db_path: Path to database
            threshold_version: Which threshold labels to use
            commission_rate: Commission as decimal (0.02 = 2%)
        """
        self.db_path = db_path
        self.threshold_version = threshold_version
        self.commission_rate = commission_rate

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

        # Apply commission
        commission = guaranteed_return * self.commission_rate
        net_profit = guaranteed_profit - commission
        net_profit_percent = (net_profit / total_stake) * 100

        return {
            'stakes': (stake1, stake2, stake3),
            'total_stake': total_stake,
            'guaranteed_return': guaranteed_return,
            'gross_profit': guaranteed_profit,
            'gross_profit_percent': profit_percent,
            'commission': commission,
            'net_profit': net_profit,
            'net_profit_percent': net_profit_percent,
            'combined_inverse': combined_inverse
        }

    def run_simulation(self, season, initial_bankroll=10000, bet_amount=100):
        """Run arbitrage simulation with transaction costs."""
        conn = sqlite3.connect(self.db_path)

        print(f"\n{'='*70}")
        print(f"TRANSACTION COST SIMULATION - SEASON {season}")
        print(f"{'='*70}")
        print(f"Threshold Version: {self.threshold_version} ({self.threshold_value:.3f})")
        print(f"Initial Bankroll: £{initial_bankroll:,.2f}")
        print(f"Bet Amount: £{bet_amount:,.2f}")
        print(f"Commission Rate: {self.commission_rate:.1%}")
        print(f"Min Net Profit: Skip if profit < commission")
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
        skipped_unprofitable = 0

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

            # STEP 3: Wait for timing signal
            bet3_placed = False
            bet3_odds_final = bet3_odds
            bet3_snapshot_time = None
            bet3_days_before = None

            timing_column = f'should_bet_{bet3_outcome}_now{self.column_suffix}'

            for idx, row in match_df.iterrows():
                if row['snapshot_time'] == first_row['snapshot_time']:
                    continue

                should_bet_now = row.get(timing_column, 0) == 1

                odds_column = f'{bet3_outcome}_odds_current'
                if pd.notna(row[odds_column]):
                    bet3_odds_final = row[odds_column]

                if should_bet_now and not bet3_placed:
                    bet3_placed = True
                    bet3_snapshot_time = row['snapshot_time']
                    bet3_days_before = row['days_before_match']
                    break

            # STEP 4: Fallback - last snapshot
            if not bet3_placed:
                last_row = match_df.iloc[-1]
                bet3_snapshot_time = last_row['snapshot_time']
                bet3_days_before = last_row['days_before_match']

                odds_column = f'{bet3_outcome}_odds_current'
                if pd.notna(last_row[odds_column]):
                    bet3_odds_final = last_row[odds_column]

            # Calculate final arbitrage WITH transaction costs
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

            # FILTER: Skip if net profit is negative or too small
            if stakes_result['net_profit'] <= 0:
                skipped_unprofitable += 1
                continue

            if bankroll < bet_amount:
                break

            # Complete the bet
            bankroll -= stakes_result['total_stake']
            bankroll += stakes_result['guaranteed_return']
            bankroll -= stakes_result['commission']  # Deduct commission

            completed_bets.append({
                'match_id': match_id,
                'home_team': home_team,
                'away_team': away_team,
                'days_before': bet3_days_before,
                'total_stake': stakes_result['total_stake'],
                'gross_profit': stakes_result['gross_profit'],
                'gross_profit_pct': stakes_result['gross_profit_percent'],
                'commission': stakes_result['commission'],
                'net_profit': stakes_result['net_profit'],
                'net_profit_pct': stakes_result['net_profit_percent'],
                'signal_fired': bet3_placed
            })

        # Results
        total_gross_profit = sum(b['gross_profit'] for b in completed_bets)
        total_commission = sum(b['commission'] for b in completed_bets)
        total_net_profit = sum(b['net_profit'] for b in completed_bets)
        roi = ((bankroll - initial_bankroll) / initial_bankroll) * 100

        signals_fired = sum(1 for b in completed_bets if b['signal_fired'])
        no_signals = len(completed_bets) - signals_fired

        results = {
            'season': season,
            'threshold_version': self.threshold_version,
            'threshold_value': self.threshold_value,
            'commission_rate': self.commission_rate,
            'initial_bankroll': initial_bankroll,
            'final_bankroll': bankroll,
            'total_gross_profit': total_gross_profit,
            'total_commission': total_commission,
            'total_net_profit': total_net_profit,
            'roi': roi,
            'total_matches': len(matches),
            'skipped_no_future_arb': skipped_no_arb,
            'skipped_no_odds': skipped_no_odds,
            'skipped_unprofitable': skipped_unprofitable,
            'arbitrages_completed': len(completed_bets),
            'signals_fired': signals_fired,
            'no_signals': no_signals,
            'signal_coverage': (signals_fired / len(completed_bets) * 100) if completed_bets else 0,
            'coverage': (len(completed_bets) / len(matches)) * 100,
            'avg_gross_profit': total_gross_profit / len(completed_bets) if completed_bets else 0,
            'avg_commission': total_commission / len(completed_bets) if completed_bets else 0,
            'avg_net_profit': total_net_profit / len(completed_bets) if completed_bets else 0,
            'avg_net_profit_pct': np.mean([b['net_profit_pct'] for b in completed_bets]) if completed_bets else 0,
            'bets': completed_bets
        }

        return results

    def print_results(self, results):
        """Print results."""
        print(f"\n{'='*70}")
        print(f"FINAL RESULTS - SEASON {results['season']}")
        print(f"Threshold: {results['threshold_version']} ({results['threshold_value']:.3f})")
        print(f"Commission: {results['commission_rate']:.1%}")
        print(f"{'='*70}\n")

        print("FINANCIAL:")
        print(f"  Initial:       £{results['initial_bankroll']:>12,.2f}")
        print(f"  Final:         £{results['final_bankroll']:>12,.2f}")
        print(f"  Gross Profit:  £{results['total_gross_profit']:>12,.2f}")
        print(f"  Commission:    £{results['total_commission']:>12,.2f}")
        print(f"  Net Profit:    £{results['total_net_profit']:>12,.2f}")
        print(f"  ROI:           {results['roi']:>13.2f}%\n")

        print("MATCHES:")
        print(f"  Total:              {results['total_matches']:>8,}")
        print(f"  Skipped unprofitable:{results['skipped_unprofitable']:>7,}")
        print(f"  Completed arbs:     {results['arbitrages_completed']:>8,}")
        print(f"  Coverage:           {results['coverage']:>7.1f}%\n")

        print("PROFIT ANALYSIS:")
        print(f"  Avg Gross Profit:   £{results['avg_gross_profit']:>7,.2f}")
        print(f"  Avg Commission:     £{results['avg_commission']:>7,.2f}")
        print(f"  Avg Net Profit:     £{results['avg_net_profit']:>7,.2f}")
        print(f"  Avg Net Profit %:   {results['avg_net_profit_pct']:>7.2f}%\n")

        print(f"{'='*70}\n")


if __name__ == "__main__":
    db_path = "../../data/raw/epl_arbitrage.db"

    # Default to threshold 0.55
    threshold_version = 'original'

    if len(sys.argv) > 1:
        threshold_version = sys.argv[1]

    print(f"\nRunning transaction cost simulation with threshold: {threshold_version}\n")

    sim = TransactionCostSimulator(db_path, threshold_version, commission_rate=0.02)
    results = sim.run_simulation('24/25', initial_bankroll=10000, bet_amount=100)

    if results:
        sim.print_results(results)

        # Save
        output_file = f'../../data/simulation_txcost_{threshold_version}_24-25.json'
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"Results saved to: {output_file}")
    else:
        print("\nSimulation failed.")
        sys.exit(1)
