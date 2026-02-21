#!/usr/bin/env python3
"""
Optimized Smart Arbitrage Strategy - Dynamic Bet Sizing

OPTIMIZATION #2: Dynamic bet sizing based on predicted profit potential

Instead of fixed £100 bets, this strategy:
1. Analyzes predicted profit % for each opportunity
2. Bets MORE on high-profit opportunities (>4% profit)
3. Bets LESS on low-profit opportunities (<2% profit)
4. SKIPS very low profit opportunities (<1% profit)

This concentrates capital on the best opportunities for better ROI.

Strategy:
1. Calculate expected profit % from odds
2. Size bet based on profit potential:
   - Very high profit (>5%): 2.0x base bet
   - High profit (>4%): 1.5x base bet
   - Medium profit (>3%): 1.0x base bet
   - Low profit (>2%): 0.5x base bet
   - Very low profit (>1%): 0.25x base bet
   - Minimal profit (<1%): SKIP
3. Wait for optimal timing on 3rd outcome (use Optimization #1)
4. Calculate stakes for complete arbitrage
"""

import sqlite3
import pandas as pd
import numpy as np
import json


class DynamicSizingSimulator:
    """Arbitrage simulator with dynamic bet sizing based on profit potential."""

    def __init__(self, db_path, base_bet_amount=100):
        self.db_path = db_path
        self.base_bet_amount = base_bet_amount

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

    def calculate_bet_size(self, profit_percent, expected_improvement=10.0):
        """
        Calculate bet size based on predicted profit percentage.

        Takes into account expected odds improvement from timing strategy.
        Historical data shows ~10% avg improvement on 3rd outcome.

        New approach: Bet on EVERYTHING but scale up for better opportunities.
        Base multiplier is 0.5x (£50), scaling up to 2.0x (£200) for best bets.

        Args:
            profit_percent (float): Initial profit percentage
            expected_improvement (float): Expected % odds improvement (default 10%)

        Returns:
            float: Bet amount
        """
        # Estimate final profit after timing optimization
        estimated_final_profit = profit_percent + (expected_improvement * 0.3)

        # Scale bet size based on estimated profit
        # Everyone gets at least 0.5x base, best get 2.0x
        if estimated_final_profit >= 5.0:
            multiplier = 2.0  # Exceptional
        elif estimated_final_profit >= 4.0:
            multiplier = 1.5  # Very good
        elif estimated_final_profit >= 3.0:
            multiplier = 1.0  # Good (standard)
        elif estimated_final_profit >= 2.0:
            multiplier = 0.75  # Average
        else:
            multiplier = 0.5  # Below average (but still worth it)

        return self.base_bet_amount * multiplier

    def select_outcomes_optimized(self, odds_map):
        """
        Select which outcome to wait for, prioritizing away/home (Optimization #1).
        """
        preference_order = ['draw', 'home', 'away']

        for preferred_bet3 in reversed(preference_order):
            if preferred_bet3 in odds_map:
                bet3_outcome = preferred_bet3
                bet3_odds = odds_map[preferred_bet3]

                other_outcomes = [(k, v) for k, v in odds_map.items() if k != preferred_bet3]
                other_outcomes.sort(key=lambda x: x[1], reverse=True)

                bet1_outcome, bet1_odds = other_outcomes[0]
                bet2_outcome, bet2_odds = other_outcomes[1]

                return (
                    (bet1_outcome, bet1_odds),
                    (bet2_outcome, bet2_odds),
                    (bet3_outcome, bet3_odds)
                )

        sorted_outcomes = sorted(odds_map.items(), key=lambda x: x[1], reverse=True)
        return (sorted_outcomes[0], sorted_outcomes[1], sorted_outcomes[2])

    def run_simulation(self, season, initial_bankroll=10000):
        """Run dynamic sizing arbitrage simulation."""
        conn = sqlite3.connect(self.db_path)

        print(f"\n{'='*70}")
        print(f"DYNAMIC BET SIZING SIMULATION - SEASON {season}")
        print(f"{'='*70}")
        print(f"Strategy: Dynamic bet sizing based on profit potential")
        print(f"          + Optimized outcome selection (wait for away/home)")
        print(f"Initial Bankroll: £{initial_bankroll:,.2f}")
        print(f"Base Bet Amount: £{self.base_bet_amount:,.2f}")
        print(f"{'='*70}\n")

        print("Bet Sizing Rules (estimated profit after timing):")
        print("  >5% estimated: 2.0x base (£200) - Exceptional")
        print("  >4% estimated: 1.5x base (£150) - Very good")
        print("  >3% estimated: 1.0x base (£100) - Good (standard)")
        print("  >2% estimated: 0.75x base (£75) - Average")
        print("  <2% estimated: 0.5x base (£50) - Below average")
        print("  Note: ALL opportunities taken, scaled by profit potential\n")

        # Load all snapshots
        query = """
            SELECT * FROM ml_features
            WHERE season = ?
            ORDER BY match_id, snapshot_time DESC
        """
        df = pd.read_sql(query, conn, params=(season,))
        conn.close()

        print(f"Loaded {len(df):,} snapshots for {df['match_id'].nunique()} matches\n")

        matches = df.groupby('match_id')

        bankroll = initial_bankroll
        completed_bets = []
        skipped_no_arb = 0
        skipped_no_odds = 0
        skipped_low_profit = 0
        skipped_insufficient_funds = 0

        # Track sizing decisions
        sizing_stats = {
            'very_high': 0,  # >5%
            'high': 0,       # >4%
            'medium': 0,     # >3%
            'low': 0,        # >2%
            'very_low': 0,   # >1%
            'skipped': 0     # <1%
        }

        for match_id, match_df in matches:
            match_df = match_df.sort_values('snapshot_time')

            first_row = match_df.iloc[0]
            home_team = first_row['home_team']
            away_team = first_row['away_team']

            if first_row['will_have_future_arbitrage'] != 1:
                skipped_no_arb += 1
                continue

            if pd.isna(first_row['home_odds_current']) or pd.isna(first_row['draw_odds_current']) or pd.isna(first_row['away_odds_current']):
                skipped_no_odds += 1
                continue

            # Optimized outcome selection
            odds_map = {
                'home': first_row['home_odds_current'],
                'draw': first_row['draw_odds_current'],
                'away': first_row['away_odds_current']
            }

            (bet1_outcome, bet1_odds), (bet2_outcome, bet2_odds), (bet3_outcome, bet3_odds) = \
                self.select_outcomes_optimized(odds_map)

            # Calculate INITIAL profit estimate (before waiting for better odds on bet3)
            initial_stakes = self.calculate_three_way_stakes(bet1_odds, bet2_odds, bet3_odds, self.base_bet_amount)

            if initial_stakes is None:
                continue

            initial_profit_pct = initial_stakes['profit_percent']

            # Determine bet size based on initial profit + expected improvement
            bet_amount = self.calculate_bet_size(initial_profit_pct, expected_improvement=10.0)

            # Calculate estimated final profit for tracking
            estimated_final_profit = initial_profit_pct + (10.0 * 0.3)

            # Track sizing decision based on estimated final profit
            if estimated_final_profit >= 5.0:
                sizing_stats['very_high'] += 1  # Exceptional
            elif estimated_final_profit >= 4.0:
                sizing_stats['high'] += 1  # Very good
            elif estimated_final_profit >= 3.0:
                sizing_stats['medium'] += 1  # Good
            elif estimated_final_profit >= 2.0:
                sizing_stats['low'] += 1  # Average
            else:
                sizing_stats['very_low'] += 1  # Below average

            print(f"\n📊 {home_team} vs {away_team}")
            print(f"   Initial: {initial_profit_pct:.2f}%, Estimated: {estimated_final_profit:.2f}% → Bet £{bet_amount:.2f}")

            # Wait for timing signal on 3rd outcome
            bet3_placed = False
            bet3_odds_final = bet3_odds
            bet3_snapshot_time = None
            bet3_days_before = None

            for idx, row in match_df.iterrows():
                if row['snapshot_time'] == first_row['snapshot_time']:
                    continue

                timing_column = f'should_bet_{bet3_outcome}_now'
                should_bet_now = row.get(timing_column, 0) == 1

                odds_column = f'{bet3_outcome}_odds_current'
                if pd.notna(row[odds_column]):
                    bet3_odds_final = row[odds_column]

                if should_bet_now and not bet3_placed:
                    bet3_placed = True
                    bet3_snapshot_time = row['snapshot_time']
                    bet3_days_before = row['days_before_match']
                    print(f"   ✓ Signal! Bet {bet3_outcome} at {bet3_odds_final:.2f}")
                    break

            if not bet3_placed:
                last_row = match_df.iloc[-1]
                bet3_snapshot_time = last_row['snapshot_time']
                bet3_days_before = last_row['days_before_match']

                odds_column = f'{bet3_outcome}_odds_current'
                if pd.notna(last_row[odds_column]):
                    bet3_odds_final = last_row[odds_column]

                print(f"   ⏰ No signal - bet at last snapshot: {bet3_odds_final:.2f}")

            # Calculate final arbitrage with actual bet size
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

            if bankroll < stakes_result['total_stake']:
                print(f"   ✗ Insufficient funds (need £{stakes_result['total_stake']:.2f}, have £{bankroll:.2f})")
                skipped_insufficient_funds += 1
                continue

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
                'initial_profit_pct': initial_profit_pct,
                'bet_size_multiplier': bet_amount / self.base_bet_amount,
                'total_stake': stakes_result['total_stake'],
                'profit': stakes_result['guaranteed_profit'],
                'profit_pct': stakes_result['profit_percent']
            })

            print(f"   💰 Profit: £{stakes_result['guaranteed_profit']:.2f} ({stakes_result['profit_percent']:.2f}%)")
            print(f"   💵 Bankroll: £{bankroll:,.2f}")

        # Results
        total_profit = sum(b['profit'] for b in completed_bets)
        roi = ((bankroll - initial_bankroll) / initial_bankroll) * 100

        # Calculate weighted average profit
        total_stakes = sum(b['total_stake'] for b in completed_bets)

        results = {
            'season': season,
            'initial_bankroll': initial_bankroll,
            'final_bankroll': bankroll,
            'total_profit': total_profit,
            'roi': roi,
            'total_matches': len(matches),
            'skipped_no_future_arb': skipped_no_arb,
            'skipped_no_odds': skipped_no_odds,
            'skipped_low_profit': skipped_low_profit,
            'skipped_insufficient_funds': skipped_insufficient_funds,
            'arbitrages_completed': len(completed_bets),
            'coverage': (len(completed_bets) / len(matches)) * 100,
            'total_stakes': total_stakes,
            'avg_profit': total_profit / len(completed_bets) if completed_bets else 0,
            'avg_profit_pct': np.mean([b['profit_pct'] for b in completed_bets]) if completed_bets else 0,
            'avg_stake': total_stakes / len(completed_bets) if completed_bets else 0,
            'sizing_stats': sizing_stats,
            'bets': completed_bets
        }

        return results

    def print_results(self, results):
        """Print results."""
        print(f"\n{'='*70}")
        print(f"FINAL RESULTS - SEASON {results['season']} (DYNAMIC SIZING)")
        print(f"{'='*70}\n")

        print("FINANCIAL:")
        print(f"  Initial: £{results['initial_bankroll']:>12,.2f}")
        print(f"  Final:   £{results['final_bankroll']:>12,.2f}")
        print(f"  Profit:  £{results['total_profit']:>12,.2f}")
        print(f"  ROI:     {results['roi']:>13.2f}%\n")

        print("MATCHES:")
        print(f"  Total:              {results['total_matches']:>8,}")
        print(f"  No future arb:      {results['skipped_no_future_arb']:>8,}")
        print(f"  Skipped (low profit): {results['skipped_low_profit']:>6,}")
        print(f"  Completed arbs:     {results['arbitrages_completed']:>8,}")
        print(f"  Coverage:           {results['coverage']:>7.1f}%\n")

        print("BET SIZING:")
        print(f"  Total capital deployed: £{results['total_stakes']:>10,.2f}")
        print(f"  Avg stake per bet:      £{results['avg_stake']:>10,.2f}")
        print(f"  Avg profit per bet:     £{results['avg_profit']:>10,.2f}")
        print(f"  Avg profit %:           {results['avg_profit_pct']:>11.2f}%\n")

        print("SIZING DISTRIBUTION:")
        stats = results['sizing_stats']
        total_opps = sum(stats.values())
        print(f"  Exceptional (>5%):   {stats['very_high']:>4} ({stats['very_high']/total_opps*100:5.1f}%) - 2.0x bet (£200)")
        print(f"  Very good (>4%):     {stats['high']:>4} ({stats['high']/total_opps*100:5.1f}%) - 1.5x bet (£150)")
        print(f"  Good (>3%):          {stats['medium']:>4} ({stats['medium']/total_opps*100:5.1f}%) - 1.0x bet (£100)")
        print(f"  Average (>2%):       {stats['low']:>4} ({stats['low']/total_opps*100:5.1f}%) - 0.75x bet (£75)")
        print(f"  Below average (<2%): {stats['very_low']:>4} ({stats['very_low']/total_opps*100:5.1f}%) - 0.5x bet (£50)")
        if stats['skipped'] > 0:
            print(f"  Skipped:             {stats['skipped']:>4} ({stats['skipped']/total_opps*100:5.1f}%)")
        print()

        print(f"{'='*70}\n")


if __name__ == "__main__":
    db_path = "../../data/raw/epl_arbitrage.db"

    sim = DynamicSizingSimulator(db_path, base_bet_amount=100)
    results = sim.run_simulation('21/22', initial_bankroll=10000)
    sim.print_results(results)

    with open('../../data/dynamic_sizing_simulation_21-22.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
    print("Results saved to: ../../data/dynamic_sizing_simulation_21-22.json")
