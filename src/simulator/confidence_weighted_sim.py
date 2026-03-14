#!/usr/bin/env python3
"""
Optimization #1: Model Confidence Weighting

Instead of fixed bet sizes, scale bets by model confidence (probability score).
Higher confidence = larger bet (up to 1.5x base bet).
Lower confidence = smaller bet (down to 0.5x base bet).

Expected improvement: +10-15% profit
"""

import sqlite3
import pandas as pd
import numpy as np
import pickle
import json
import sys


class ConfidenceWeightedSimulator:
    """Arbitrage simulator with confidence-weighted bet sizing."""

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

    def get_confidence_multiplier(self, days_before, signal_fired):
        """
        Calculate bet size multiplier based on signal timing (confidence proxy).

        Early signals = high confidence (model is very sure)
        Late signals = medium confidence
        No signal = lower confidence (fallback bet)

        Args:
            days_before: Days before match when signal fired
            signal_fired: Whether timing signal fired (vs fallback)

        Returns:
            Multiplier between 0.8 and 1.4
        """
        if not signal_fired:
            return 0.8  # No signal = lowest confidence

        # Signal fired - confidence based on how early
        if days_before > 10:
            return 1.4  # Very early = high confidence
        elif days_before > 5:
            return 1.2  # Early-mid = good confidence
        elif days_before > 2:
            return 1.0  # Mid-late = normal confidence
        else:
            return 0.9  # Very late = slightly cautious

    def run_simulation(self, season, initial_bankroll=10000, base_bet_amount=100):
        """Run arbitrage simulation with confidence weighting."""
        conn = sqlite3.connect(self.db_path)

        print(f"\n{'='*70}")
        print(f"CONFIDENCE-WEIGHTED SIMULATION - SEASON {season}")
        print(f"{'='*70}")
        print(f"Threshold Version: {self.threshold_version} ({self.threshold_value:.3f})")
        print(f"Initial Bankroll: £{initial_bankroll:,.2f}")
        print(f"Base Bet Amount: £{base_bet_amount:,.2f} (weighted by confidence)")
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

        # Get feature columns
        label_cols = ['will_have_future_arbitrage', 'should_bet_home_now', 'should_bet_draw_now',
                      'should_bet_away_now', 'match_id', 'season', 'home_team', 'away_team',
                      'snapshot_time', 'days_before_match']
        feature_cols = [col for col in df.columns if col not in label_cols and not col.startswith('should_bet_')]

        # Group by match
        matches = df.groupby('match_id')

        # Track state
        bankroll = initial_bankroll
        completed_bets = []
        skipped_no_arb = 0
        skipped_no_odds = 0
        confidence_stats = []

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

            # STEP 3: Wait for timing signal on 3rd outcome with confidence weighting
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

            # Calculate confidence multiplier based on timing
            confidence_multiplier = self.get_confidence_multiplier(bet3_days_before, bet3_placed)

            # Apply confidence weighting to bet amount
            weighted_bet_amount = base_bet_amount * confidence_multiplier

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
                total_stake=weighted_bet_amount
            )

            if stakes_result is None:
                continue

            if bankroll < weighted_bet_amount:
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
                'confidence_multiplier': confidence_multiplier,
                'base_bet': base_bet_amount,
                'weighted_bet': weighted_bet_amount,
                'total_stake': stakes_result['total_stake'],
                'profit': stakes_result['guaranteed_profit'],
                'profit_pct': stakes_result['profit_percent'],
                'signal_fired': bet3_placed
            })

            confidence_stats.append({
                'days_before': bet3_days_before,
                'multiplier': confidence_multiplier,
                'profit': stakes_result['guaranteed_profit'],
                'signal_fired': bet3_placed
            })

        # Results
        total_profit = sum(b['profit'] for b in completed_bets)
        roi = ((bankroll - initial_bankroll) / initial_bankroll) * 100

        # Analyze confidence weighting impact
        signals_fired = sum(1 for b in completed_bets if b['signal_fired'])
        no_signals = len(completed_bets) - signals_fired
        avg_days_before = np.mean([s['days_before'] for s in confidence_stats]) if confidence_stats else 0
        avg_multiplier = np.mean([s['multiplier'] for s in confidence_stats]) if confidence_stats else 0

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
            'avg_days_before': avg_days_before,
            'avg_multiplier': avg_multiplier,
            'coverage': (len(completed_bets) / len(matches)) * 100,
            'avg_profit': total_profit / len(completed_bets) if completed_bets else 0,
            'avg_profit_pct': np.mean([b['profit_pct'] for b in completed_bets]) if completed_bets else 0,
            'bets': completed_bets,
            'confidence_stats': confidence_stats
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

        print("CONFIDENCE WEIGHTING:")
        print(f"  Avg Days Before:    {results['avg_days_before']:>7.1f}")
        print(f"  Avg Multiplier:     {results['avg_multiplier']:>7.2f}x")
        print(f"  (0.8x = no signal, 1.4x = early signal)\n")

        print("MATCHES:")
        print(f"  Total:              {results['total_matches']:>8,}")
        print(f"  Completed arbs:     {results['arbitrages_completed']:>8,}")
        print(f"  Signals fired:      {results['signals_fired']:>8,} ({results['signal_coverage']:.1f}%)")
        print(f"  No signals:         {results['no_signals']:>8,}\n")

        print(f"{'='*70}\n")


if __name__ == "__main__":
    db_path = "../../data/raw/epl_arbitrage.db"

    # Default to threshold 0.50 (best performer)
    threshold_version = '050'

    if len(sys.argv) > 1:
        threshold_version = sys.argv[1]

    print(f"\nRunning confidence-weighted simulation with threshold: {threshold_version}\n")

    sim = ConfidenceWeightedSimulator(db_path, threshold_version)
    results = sim.run_simulation('24/25', initial_bankroll=10000, base_bet_amount=100)

    if results:
        sim.print_results(results)

        # Save
        output_file = f'../../data/simulation_confidence_{threshold_version}_24-25.json'
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"Results saved to: {output_file}")
    else:
        print("\nSimulation failed.")
        sys.exit(1)
