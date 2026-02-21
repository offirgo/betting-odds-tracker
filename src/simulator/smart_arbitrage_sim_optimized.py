#!/usr/bin/env python3
"""
Optimized Smart Arbitrage Strategy - Outcome Selection Optimization

OPTIMIZATION 1: Preferentially wait for outcomes that historically improve most
Based on analysis showing:
- Away outcomes: 9.99% avg improvement
- Home outcomes: 9.43% avg improvement
- Draw outcomes: Lower improvement

Strategy:
1. First snapshot: Decide if match is worth tracking (has future arbitrage potential)
2. Immediately bet on 2 outcomes
3. Wait for timing signal on the outcome most likely to improve (away > home > draw)
4. Fallback: Place 3rd bet at last snapshot if no signal
5. Calculate stakes for complete arbitrage
"""

import sqlite3
import pandas as pd
import numpy as np
import json


class OptimizedArbitrageSimulator:
    """Optimized arbitrage simulator with smart outcome selection."""

    def __init__(self, db_path):
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

    def select_outcomes_optimized(self, odds_map):
        """
        Select which outcome to wait for, prioritizing those that improve most.

        Priority for bet3 (wait for timing):
        1. Away (historically best improvement: 9.99%)
        2. Home (second best: 9.43%)
        3. Draw (lowest improvement)

        Returns: (bet1_outcome, bet2_outcome, bet3_outcome) with their odds
        """
        # Define preference order (best improvement last, since we wait for it)
        preference_order = ['draw', 'home', 'away']  # Draw least preferred, away most

        # Try to select away or home as bet3 (the one we wait for)
        for preferred_bet3 in reversed(preference_order):  # Start with away
            if preferred_bet3 in odds_map:
                bet3_outcome = preferred_bet3
                bet3_odds = odds_map[preferred_bet3]

                # Get the other two outcomes
                other_outcomes = [(k, v) for k, v in odds_map.items() if k != preferred_bet3]

                # Sort others by odds (highest first)
                other_outcomes.sort(key=lambda x: x[1], reverse=True)

                bet1_outcome, bet1_odds = other_outcomes[0]
                bet2_outcome, bet2_odds = other_outcomes[1]

                return (
                    (bet1_outcome, bet1_odds),
                    (bet2_outcome, bet2_odds),
                    (bet3_outcome, bet3_odds)
                )

        # Fallback (should never happen with home/draw/away)
        sorted_outcomes = sorted(odds_map.items(), key=lambda x: x[1], reverse=True)
        return (sorted_outcomes[0], sorted_outcomes[1], sorted_outcomes[2])

    def run_simulation(self, season, initial_bankroll=10000, bet_amount=100):
        """Run optimized arbitrage simulation."""
        conn = sqlite3.connect(self.db_path)

        print(f"\n{'='*70}")
        print(f"OPTIMIZED ARBITRAGE SIMULATION - SEASON {season}")
        print(f"{'='*70}")
        print(f"Strategy: Track high-profit matches, bet 2 outcomes immediately,")
        print(f"          wait for away/home timing signal (historically best)")
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

        # Process each match
        for match_id, match_df in matches:
            # Sort chronologically
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

            # STEP 2: OPTIMIZED - Select outcomes with preference for away/home as bet3
            odds_map = {
                'home': first_row['home_odds_current'],
                'draw': first_row['draw_odds_current'],
                'away': first_row['away_odds_current']
            }

            (bet1_outcome, bet1_odds), (bet2_outcome, bet2_odds), (bet3_outcome, bet3_odds) = \
                self.select_outcomes_optimized(odds_map)

            print(f"   Odds: Home {odds_map['home']:.2f}, Draw {odds_map['draw']:.2f}, Away {odds_map['away']:.2f}")
            print(f"   ➤ Bet {bet1_outcome} ({bet1_odds:.2f}) + {bet2_outcome} ({bet2_odds:.2f})")
            print(f"   ⏳ Wait for {bet3_outcome} signal (optimized selection)...")

            # STEP 3: Wait for timing signal on 3rd outcome
            bet3_placed = False
            bet3_odds_final = bet3_odds
            bet3_snapshot_time = None
            bet3_days_before = None

            for idx, row in match_df.iterrows():
                # Skip first
                if row['snapshot_time'] == first_row['snapshot_time']:
                    continue

                # Check timing signal
                timing_column = f'should_bet_{bet3_outcome}_now'
                should_bet_now = row.get(timing_column, 0) == 1

                # Update odds
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

            # Check bankroll
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
                'profit_pct': stakes_result['profit_percent']
            })

            print(f"   💰 Profit: £{stakes_result['guaranteed_profit']:.2f} ({stakes_result['profit_percent']:.2f}%)")
            print(f"   💵 Bankroll: £{bankroll:,.2f}")

        # Results
        total_profit = sum(b['profit'] for b in completed_bets)
        roi = ((bankroll - initial_bankroll) / initial_bankroll) * 100

        # Analyze odds changes by outcome
        odds_improved = [b for b in completed_bets if b['odds_change_pct'] > 0]
        odds_worsened = [b for b in completed_bets if b['odds_change_pct'] < 0]

        # Break down by outcome
        by_outcome = {}
        for outcome in ['home', 'draw', 'away']:
            outcome_bets = [b for b in completed_bets if b['bet3_outcome'] == outcome]
            if outcome_bets:
                by_outcome[outcome] = {
                    'count': len(outcome_bets),
                    'avg_improvement': np.mean([b['odds_change_pct'] for b in outcome_bets])
                }

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
            'coverage': (len(completed_bets) / len(matches)) * 100,
            'avg_profit': total_profit / len(completed_bets) if completed_bets else 0,
            'avg_profit_pct': np.mean([b['profit_pct'] for b in completed_bets]) if completed_bets else 0,
            'odds_improved_count': len(odds_improved),
            'odds_worsened_count': len(odds_worsened),
            'avg_odds_change': np.mean([b['odds_change_pct'] for b in completed_bets]) if completed_bets else 0,
            'by_outcome': by_outcome,
            'bets': completed_bets
        }

        return results

    def print_results(self, results):
        """Print results."""
        print(f"\n{'='*70}")
        print(f"FINAL RESULTS - SEASON {results['season']} (OPTIMIZED)")
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

        print("TIMING STRATEGY:")
        print(f"  Avg Profit/Bet:     £{results['avg_profit']:>7,.2f}")
        print(f"  Avg Profit %:       {results['avg_profit_pct']:>7.2f}%")
        print(f"  3rd Odds Improved:  {results['odds_improved_count']:>8,} times")
        print(f"  3rd Odds Worsened:  {results['odds_worsened_count']:>8,} times")
        print(f"  Avg Odds Change:    {results['avg_odds_change']:>7.2f}%\n")

        print("OUTCOME PREFERENCE (Optimized):")
        if 'by_outcome' in results:
            for outcome, stats in results['by_outcome'].items():
                print(f"  {outcome.capitalize():5}: {stats['count']:3} bets, avg improvement {stats['avg_improvement']:+6.2f}%")
        print()

        print(f"{'='*70}\n")


if __name__ == "__main__":
    db_path = "../../data/raw/epl_arbitrage.db"

    sim = OptimizedArbitrageSimulator(db_path)
    results = sim.run_simulation('21/22', initial_bankroll=10000, bet_amount=100)
    sim.print_results(results)

    # Save
    with open('../../data/optimized_simulation_21-22.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
    print("Results saved to: ../../data/optimized_simulation_21-22.json")
