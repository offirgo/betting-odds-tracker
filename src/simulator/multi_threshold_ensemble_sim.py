#!/usr/bin/env python3
"""
Optimization #4: Multi-Threshold Ensemble

Use signals from BOTH threshold 0.55 and 0.50:
- If EITHER threshold signals to bet, place the bet
- Combines conservative (0.55) precision with aggressive (0.50) coverage
- Should increase signal coverage while maintaining quality

Expected improvement: +5-8% profit
"""

import sqlite3
import pandas as pd
import numpy as np
import json
import sys


class MultiThresholdEnsembleSimulator:
    """Arbitrage simulator using ensemble of multiple thresholds."""

    def __init__(self, db_path):
        """
        Initialize simulator.

        Args:
            db_path: Path to database
        """
        self.db_path = db_path

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
        """Run arbitrage simulation with multi-threshold ensemble."""
        conn = sqlite3.connect(self.db_path)

        print(f"\n{'='*70}")
        print(f"MULTI-THRESHOLD ENSEMBLE SIMULATION - SEASON {season}")
        print(f"{'='*70}")
        print(f"Using thresholds: 0.55 (original) + 0.50 (aggressive)")
        print(f"Strategy: Bet if EITHER threshold signals")
        print(f"Initial Bankroll: £{initial_bankroll:,.2f}")
        print(f"Bet Amount: £{bet_amount:,.2f}")
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

        # Track which threshold triggered each bet
        threshold_055_only = 0
        threshold_050_only = 0
        both_thresholds = 0
        no_signal = 0

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

            # STEP 3: Wait for timing signal from EITHER threshold
            bet3_placed = False
            bet3_odds_final = bet3_odds
            bet3_snapshot_time = None
            bet3_days_before = None
            triggered_by = None

            # Column names for both thresholds
            timing_col_055 = f'should_bet_{bet3_outcome}_now'
            timing_col_050 = f'should_bet_{bet3_outcome}_now_t050'

            for idx, row in match_df.iterrows():
                if row['snapshot_time'] == first_row['snapshot_time']:
                    continue

                # Check both thresholds
                signal_055 = row.get(timing_col_055, 0) == 1
                signal_050 = row.get(timing_col_050, 0) == 1

                # Update current odds
                odds_column = f'{bet3_outcome}_odds_current'
                if pd.notna(row[odds_column]):
                    bet3_odds_final = row[odds_column]

                # Bet if EITHER threshold signals
                if (signal_055 or signal_050) and not bet3_placed:
                    bet3_placed = True
                    bet3_snapshot_time = row['snapshot_time']
                    bet3_days_before = row['days_before_match']

                    # Track which threshold(s) triggered
                    if signal_055 and signal_050:
                        triggered_by = 'both'
                        both_thresholds += 1
                    elif signal_055:
                        triggered_by = '0.55_only'
                        threshold_055_only += 1
                    else:
                        triggered_by = '0.50_only'
                        threshold_050_only += 1

                    break

            # STEP 4: Fallback - last snapshot
            if not bet3_placed:
                last_row = match_df.iloc[-1]
                bet3_snapshot_time = last_row['snapshot_time']
                bet3_days_before = last_row['days_before_match']
                triggered_by = 'fallback'
                no_signal += 1

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
                'days_before': bet3_days_before,
                'triggered_by': triggered_by,
                'total_stake': stakes_result['total_stake'],
                'profit': stakes_result['guaranteed_profit'],
                'profit_pct': stakes_result['profit_percent']
            })

        # Results
        total_profit = sum(b['profit'] for b in completed_bets)
        roi = ((bankroll - initial_bankroll) / initial_bankroll) * 100

        # Calculate signal breakdown
        signals_fired = len(completed_bets) - no_signal

        results = {
            'season': season,
            'initial_bankroll': initial_bankroll,
            'final_bankroll': bankroll,
            'total_profit': total_profit,
            'roi': roi,
            'total_matches': len(matches),
            'skipped_no_future_arb': skipped_no_arb,
            'skipped_no_odds': skipped_no_odds,
            'arbitrages_completed': len(completed_bets),
            'signals_fired': signals_fired,
            'no_signal': no_signal,
            'signal_coverage': (signals_fired / len(completed_bets) * 100) if completed_bets else 0,
            'threshold_055_only': threshold_055_only,
            'threshold_050_only': threshold_050_only,
            'both_thresholds': both_thresholds,
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
        print(f"Multi-Threshold Ensemble (0.55 + 0.50)")
        print(f"{'='*70}\n")

        print("FINANCIAL:")
        print(f"  Initial: £{results['initial_bankroll']:>12,.2f}")
        print(f"  Final:   £{results['final_bankroll']:>12,.2f}")
        print(f"  Profit:  £{results['total_profit']:>12,.2f}")
        print(f"  ROI:     {results['roi']:>13.2f}%\n")

        print("ENSEMBLE SIGNAL BREAKDOWN:")
        total_with_signals = results['signals_fired']
        print(f"  Threshold 0.55 only: {results['threshold_055_only']:>7,} ({results['threshold_055_only']/total_with_signals*100 if total_with_signals > 0 else 0:>5.1f}%)")
        print(f"  Threshold 0.50 only: {results['threshold_050_only']:>7,} ({results['threshold_050_only']/total_with_signals*100 if total_with_signals > 0 else 0:>5.1f}%)")
        print(f"  Both thresholds:     {results['both_thresholds']:>7,} ({results['both_thresholds']/total_with_signals*100 if total_with_signals > 0 else 0:>5.1f}%)")
        print(f"  Total signals fired: {results['signals_fired']:>7,}")
        print(f"  No signal (fallback):{results['no_signal']:>7,}\n")

        print("MATCHES:")
        print(f"  Total:              {results['total_matches']:>8,}")
        print(f"  Completed arbs:     {results['arbitrages_completed']:>8,}")
        print(f"  Coverage:           {results['coverage']:>7.1f}%")
        print(f"  Signal coverage:    {results['signal_coverage']:>7.1f}%")
        print(f"  Avg Profit/Bet:     £{results['avg_profit']:>7,.2f}")
        print(f"  Avg Profit %:       {results['avg_profit_pct']:>7.2f}%\n")

        print(f"{'='*70}\n")


if __name__ == "__main__":
    db_path = "../../data/raw/epl_arbitrage.db"

    print(f"\nRunning multi-threshold ensemble simulation\n")

    sim = MultiThresholdEnsembleSimulator(db_path)
    results = sim.run_simulation('24/25', initial_bankroll=10000, bet_amount=100)

    if results:
        sim.print_results(results)

        # Save
        output_file = f'../../data/simulation_ensemble_24-25.json'
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"Results saved to: {output_file}")
    else:
        print("\nSimulation failed.")
        sys.exit(1)
