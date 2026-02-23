#!/usr/bin/env python3
"""
Smart Arbitrage Strategy - Versioned (Multi-Threshold Support)

Can run simulations with different threshold versions:
- Original (0.55): should_bet_*_now
- Threshold 0.50: should_bet_*_now_t050
- Threshold 0.45: should_bet_*_now_t045
- etc.

This allows comparing different thresholds without losing original data.
"""

import sqlite3
import pandas as pd
import numpy as np
import json


class VersionedArbitrageSimulator:
    """Arbitrage simulator that supports multiple threshold versions."""

    def __init__(self, db_path, threshold_version='original'):
        """
        Initialize simulator.

        Args:
            db_path: Path to database
            threshold_version: Which threshold labels to use
                - 'original' or '055': Use should_bet_*_now (threshold 0.55)
                - '050': Use should_bet_*_now_t050 (threshold 0.50)
                - '045': Use should_bet_*_now_t045 (threshold 0.45)
                - etc.
        """
        self.db_path = db_path
        self.threshold_version = threshold_version

        # Determine column suffix
        if threshold_version in ['original', '055', '0.55']:
            self.column_suffix = ''  # Original columns
            self.threshold_value = 0.55
        else:
            # Convert to suffix (e.g., '050' or '0.50' -> 't050')
            if threshold_version.startswith('t'):
                self.column_suffix = f'_{threshold_version}'
                self.threshold_value = float(threshold_version[1:]) / 100
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

    def run_simulation(self, season, initial_bankroll=10000, bet_amount=100):
        """Run arbitrage simulation with specified threshold version."""
        conn = sqlite3.connect(self.db_path)

        print(f"\n{'='*70}")
        print(f"VERSIONED ARBITRAGE SIMULATION - SEASON {season}")
        print(f"{'='*70}")
        print(f"Threshold Version: {self.threshold_version} ({self.threshold_value:.3f})")
        print(f"Column Suffix: '{self.column_suffix}'")
        print(f"Initial Bankroll: £{initial_bankroll:,.2f}")
        print(f"Bet Amount: £{bet_amount:,.2f}")
        print(f"{'='*70}\n")

        # Check if columns exist
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(ml_features)")
        existing_columns = [row[1] for row in cursor.fetchall()]

        required_cols = [
            f'should_bet_home_now{self.column_suffix}',
            f'should_bet_draw_now{self.column_suffix}',
            f'should_bet_away_now{self.column_suffix}'
        ]

        missing_cols = [col for col in required_cols if col not in existing_columns]

        if missing_cols:
            print(f"ERROR: Required columns not found in database:")
            for col in missing_cols:
                print(f"  - {col}")
            print()
            print(f"Please run regenerate_timing_labels.py to create these columns.")
            conn.close()
            return None

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

            print(f"\n📊 {home_team} vs {away_team}")
            print(f"   First: {first_row['snapshot_time']} ({first_row['days_before_match']:.1f} days before)")

            # Check odds available
            if pd.isna(first_row['home_odds_current']) or pd.isna(first_row['draw_odds_current']) or pd.isna(first_row['away_odds_current']):
                print(f"   ✗ Missing odds")
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

            print(f"   Odds: Home {odds_map['home']:.2f}, Draw {odds_map['draw']:.2f}, Away {odds_map['away']:.2f}")
            print(f"   ➤ Bet {bet1_outcome} ({bet1_odds:.2f}) + {bet2_outcome} ({bet2_odds:.2f})")
            print(f"   ⏳ Wait for {bet3_outcome} signal...")

            # STEP 3: Wait for timing signal on 3rd outcome (using versioned column)
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
                    print(f"   ✓ Signal! Bet {bet3_outcome} at {bet3_odds_final:.2f}")
                    print(f"     Time: {bet3_snapshot_time} ({bet3_days_before:.1f} days before)")
                    break

            # STEP 4: Fallback - last snapshot
            if not bet3_placed:
                last_row = match_df.iloc[-1]
                bet3_snapshot_time = last_row['snapshot_time']
                bet3_days_before = last_row['days_before_match']

                odds_column = f'{bet3_outcome}_odds_current'
                if pd.notna(last_row[odds_column]):
                    bet3_odds_final = last_row[odds_column]

                print(f"   ⏰ No signal - bet at last snapshot: {bet3_odds_final:.2f}")

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
                print(f"   ✗ No arbitrage with final odds")
                continue

            if bankroll < bet_amount:
                print(f"   ✗ Insufficient funds")
                break

            # Complete the bet
            bankroll -= stakes_result['total_stake']
            bankroll += stakes_result['guaranteed_return']

            completed_bets.append({
                'match_id': match_id,
                'home_team': home_team,
                'away_team': away_team,
                'days_before_start': first_row['days_before_match'],
                'days_before_complete': bet3_days_before,
                'home_odds': final_odds_list['home'],
                'draw_odds': final_odds_list['draw'],
                'away_odds': final_odds_list['away'],
                'bet1_outcome': bet1_outcome,
                'bet2_outcome': bet2_outcome,
                'bet3_outcome': bet3_outcome,
                'bet3_odds_initial': bet3_odds,
                'bet3_odds_final': bet3_odds_final,
                'odds_change_pct': ((bet3_odds_final - bet3_odds) / bet3_odds * 100),
                'total_stake': stakes_result['total_stake'],
                'profit': stakes_result['guaranteed_profit'],
                'profit_pct': stakes_result['profit_percent'],
                'signal_fired': bet3_placed
            })

            print(f"   💰 Profit: £{stakes_result['guaranteed_profit']:.2f} ({stakes_result['profit_percent']:.2f}%)")
            print(f"   💵 Bankroll: £{bankroll:,.2f}")

        # Results
        total_profit = sum(b['profit'] for b in completed_bets)
        roi = ((bankroll - initial_bankroll) / initial_bankroll) * 100

        # Analyze signal coverage
        signals_fired = sum(1 for b in completed_bets if b['signal_fired'])
        no_signals = len(completed_bets) - signals_fired

        results = {
            'season': season,
            'threshold_version': self.threshold_version,
            'threshold_value': self.threshold_value,
            'initial_bankroll': initial_bankroll,
            'final_bankroll': bankroll,
            'total_profit': total_profit,
            'roi': roi,
            'total_matches': len(matches),
            'skipped_no_future_arb': skipped_no_arb,
            'skipped_no_odds': skipped_no_odds,
            'arbitrages_completed': len(completed_bets),
            'signals_fired': signals_fired,
            'no_signals': no_signals,
            'signal_coverage': (signals_fired / len(completed_bets) * 100) if completed_bets else 0,
            'coverage': (len(completed_bets) / len(matches)) * 100,
            'avg_profit': total_profit / len(completed_bets) if completed_bets else 0,
            'avg_profit_pct': np.mean([b['profit_pct'] for b in completed_bets]) if completed_bets else 0,
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

        print("MATCHES:")
        print(f"  Total:              {results['total_matches']:>8,}")
        print(f"  No future arb:      {results['skipped_no_future_arb']:>8,}")
        print(f"  Completed arbs:     {results['arbitrages_completed']:>8,}")
        print(f"  Coverage:           {results['coverage']:>7.1f}%\n")

        print("TIMING SIGNALS:")
        print(f"  Signals fired:      {results['signals_fired']:>8,} ({results['signal_coverage']:.1f}%)")
        print(f"  No signals:         {results['no_signals']:>8,} ({100-results['signal_coverage']:.1f}%)")
        print(f"  Avg Profit/Bet:     £{results['avg_profit']:>7,.2f}")
        print(f"  Avg Profit %:       {results['avg_profit_pct']:>7.2f}%\n")

        print(f"{'='*70}\n")


if __name__ == "__main__":
    import sys

    db_path = "../../data/raw/epl_arbitrage.db"

    # Default to original
    threshold_version = 'original'

    # Allow command-line argument
    if len(sys.argv) > 1:
        threshold_version = sys.argv[1]

    print(f"\nRunning simulation with threshold version: {threshold_version}\n")

    sim = VersionedArbitrageSimulator(db_path, threshold_version)
    results = sim.run_simulation('21/22', initial_bankroll=10000, bet_amount=100)

    if results:
        sim.print_results(results)

        # Save
        output_file = f'../../data/simulation_{threshold_version}_21-22.json'
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"Results saved to: {output_file}")
    else:
        print("\nSimulation failed - check error messages above.")
        sys.exit(1)
