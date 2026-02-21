#!/usr/bin/env python3
"""
Betting Economics Breakdown

Explains the relationship between individual bet profits and total season returns.
Answers key questions about risk exposure, capital requirements, and bet frequency.
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from smart_arbitrage_sim import SmartArbitrageSimulator


def analyze_betting_economics(season='21/22'):
    """Detailed breakdown of betting economics."""

    db_path = "../../data/raw/epl_arbitrage.db"
    sim = SmartArbitrageSimulator(db_path)

    result = sim.run_simulation(
        season=season,
        initial_bankroll=10000,
        bet_amount=100
    )

    bets = result['bets']

    print("\n" + "="*80)
    print(f"BETTING ECONOMICS BREAKDOWN - SEASON {season}")
    print("="*80 + "\n")

    # Basic stats
    total_bets = len(bets)
    total_profit = result['total_profit']
    avg_profit_per_bet = result['avg_profit']

    print("BASIC STATISTICS:")
    print(f"  Total bets placed: {total_bets}")
    print(f"  Total profit: £{total_profit:.2f}")
    print(f"  Average profit per bet: £{avg_profit_per_bet:.2f} ({result['avg_profit_pct']:.2f}%)")
    print(f"  ROI: {result['roi']:.2f}%\n")

    # Important: Understand capital flow in arbitrage
    print("CAPITAL FLOW (How Arbitrage Actually Works):")
    print("-" * 80)
    print("Each £100 arbitrage bet is split across 3 outcomes:")
    print(f"  Example: Home £33.33 @ 3.00, Draw £33.33 @ 3.00, Away £33.33 @ 3.00")
    print(f"  Total staked: £100")
    print(f"  Guaranteed return: £100 × (3.00) = £100 (using one of the stakes)")
    print(f"  Wait, that's £0 profit? No!")
    print()
    print("The MAGIC of arbitrage (combined inverse < 1.0):")
    print(f"  Example with 2.84% profit: Home £32.41, Draw £32.41, Away £35.18")
    print(f"  If Home wins: £32.41 × 3.08 = £99.82... Wait, that's less!")
    print()
    print("Let me show you a REAL example from the data:")

    # Show a real example
    if bets:
        example_bet = bets[10]  # Pick a middle bet
        print(f"\n  Match: {example_bet['home_team']} vs {example_bet['away_team']}")
        print(f"  Home odds: {example_bet['home_odds']:.2f}")
        print(f"  Draw odds: {example_bet['draw_odds']:.2f}")
        print(f"  Away odds: {example_bet['away_odds']:.2f}")

        # Calculate stakes (simplified calculation)
        total_stake = example_bet['total_stake']
        combined_inv = (1/example_bet['home_odds'] +
                       1/example_bet['draw_odds'] +
                       1/example_bet['away_odds'])

        home_stake = total_stake * (1/example_bet['home_odds']) / combined_inv
        draw_stake = total_stake * (1/example_bet['draw_odds']) / combined_inv
        away_stake = total_stake * (1/example_bet['away_odds']) / combined_inv

        print(f"\n  Stakes placed:")
        print(f"    Home: £{home_stake:.2f}")
        print(f"    Draw: £{draw_stake:.2f}")
        print(f"    Away: £{away_stake:.2f}")
        print(f"    Total: £{total_stake:.2f}")

        print(f"\n  Returns (whichever outcome wins):")
        print(f"    If Home wins: £{home_stake:.2f} × {example_bet['home_odds']:.2f} = £{home_stake * example_bet['home_odds']:.2f}")
        print(f"    If Draw: £{draw_stake:.2f} × {example_bet['draw_odds']:.2f} = £{draw_stake * example_bet['draw_odds']:.2f}")
        print(f"    If Away wins: £{away_stake:.2f} × {example_bet['away_odds']:.2f} = £{away_stake * example_bet['away_odds']:.2f}")

        guaranteed_return = home_stake * example_bet['home_odds']
        guaranteed_profit = guaranteed_return - total_stake

        print(f"\n  Guaranteed return: £{guaranteed_return:.2f} (regardless of outcome!)")
        print(f"  Guaranteed profit: £{guaranteed_profit:.2f} ({example_bet['profit_pct']:.2f}%)")

    print("\n" + "="*80)
    print("KEY INSIGHT: You get £100 back + profit IMMEDIATELY after match ends!")
    print("So your capital is tied up for 0-20 days, then returned.")
    print("="*80 + "\n")

    # Timeline analysis
    print("\nTIMELINE & RISK EXPOSURE:")
    print("-" * 80)

    # Convert to dataframe for time analysis
    bets_df = pd.DataFrame(bets)
    bets_df['first_snapshot'] = pd.to_datetime(bets_df['first_snapshot'])
    bets_df['bet3_snapshot'] = pd.to_datetime(bets_df['bet3_snapshot'])

    # Find season start and end
    season_start = bets_df['first_snapshot'].min()
    season_end = bets_df['bet3_snapshot'].max()
    season_duration_days = (season_end - season_start).days
    season_duration_weeks = season_duration_days / 7

    print(f"Season duration: {season_duration_days} days ({season_duration_weeks:.1f} weeks)")
    print(f"Total bets: {total_bets}")
    print(f"Bets per week: {total_bets / season_duration_weeks:.1f}")
    print(f"Bets per month: {total_bets / (season_duration_weeks / 4.33):.1f}")

    # Weekly breakdown
    bets_df['week'] = bets_df['first_snapshot'].dt.isocalendar().week
    bets_df['year'] = bets_df['first_snapshot'].dt.year
    bets_df['year_week'] = bets_df['year'].astype(str) + '-W' + bets_df['week'].astype(str).str.zfill(2)

    weekly_stats = bets_df.groupby('year_week').agg({
        'total_stake': 'sum',
        'profit': 'sum',
        'profit_pct': 'mean'
    }).reset_index()

    weekly_stats.columns = ['Week', 'Total Staked', 'Total Profit', 'Avg Profit %']

    print(f"\nWEEKLY ACTIVITY (showing first 10 weeks):")
    print(weekly_stats.head(10).to_string(index=False))

    print(f"\n\nWEEKLY SUMMARY STATISTICS:")
    print(f"  Average stake per week: £{weekly_stats['Total Staked'].mean():.2f}")
    print(f"  Max stake in one week: £{weekly_stats['Total Staked'].max():.2f}")
    print(f"  Min stake in one week: £{weekly_stats['Total Staked'].min():.2f}")
    print(f"  Average profit per week: £{weekly_stats['Total Profit'].mean():.2f}")

    # Capital requirements
    print(f"\n\nCAPITAL REQUIREMENTS:")
    print("-" * 80)
    print("This is the KEY question: How much money do you need?")
    print()
    print("Naive calculation (WRONG):")
    print(f"  {total_bets} bets × £100 = £{total_bets * 100:,} needed")
    print()
    print("Actual calculation (RIGHT):")
    print("  Bets overlap in time - money comes back after each match")
    print("  Need to calculate PEAK simultaneous exposure")
    print()

    # Calculate peak exposure (bets active at same time)
    active_bets_timeline = []

    for _, bet in bets_df.iterrows():
        # Bet is active from first_snapshot until days_before_complete = 0
        start = bet['first_snapshot']
        # Estimate match date
        match_date = start + timedelta(days=bet['days_before_start'])

        active_bets_timeline.append({
            'date': start,
            'change': +1,
            'amount': bet['total_stake']
        })
        active_bets_timeline.append({
            'date': match_date,
            'change': -1,
            'amount': bet['total_stake']
        })

    # Sort by date
    timeline_df = pd.DataFrame(active_bets_timeline).sort_values('date')
    timeline_df['cumulative_bets'] = timeline_df['change'].cumsum()
    timeline_df['cumulative_exposure'] = timeline_df.apply(
        lambda row: row['amount'] if row['change'] > 0 else -row['amount'], axis=1
    ).cumsum()

    peak_exposure = timeline_df['cumulative_exposure'].max()
    peak_concurrent_bets = timeline_df['cumulative_bets'].max()
    avg_exposure = timeline_df['cumulative_exposure'].mean()

    print(f"ACTUAL CAPITAL NEEDED:")
    print(f"  Peak exposure: £{peak_exposure:.2f}")
    print(f"  Peak concurrent bets: {int(peak_concurrent_bets)}")
    print(f"  Average exposure: £{avg_exposure:.2f}")
    print(f"  Recommended bankroll: £{peak_exposure * 1.2:.2f} (20% buffer)")

    print(f"\n\nTURNOVER vs PROFIT:")
    print("-" * 80)
    total_turnover = total_bets * 100
    print(f"  Total turnover: £{total_turnover:,} (sum of all stakes)")
    print(f"  Total profit: £{total_profit:.2f}")
    print(f"  Profit margin: {(total_profit / total_turnover) * 100:.2f}% of turnover")
    print(f"  ROI on capital: {result['roi']:.2f}% (profit / bankroll)")

    print(f"\n\n" + "="*80)
    print("SUMMARY:")
    print("="*80)
    print(f"• You place {total_bets / season_duration_weeks:.1f} bets per week")
    print(f"• Average stake per week: £{weekly_stats['Total Staked'].mean():.2f}")
    print(f"• Average profit per week: £{weekly_stats['Total Profit'].mean():.2f}")
    print(f"• Average profit per bet: £{avg_profit_per_bet:.2f}")
    print(f"• Peak capital needed: £{peak_exposure:.2f}")
    print(f"• Season total profit: £{total_profit:.2f}")
    print(f"• ROI: {result['roi']:.2f}%")
    print("="*80 + "\n")


if __name__ == "__main__":
    analyze_betting_economics('21/22')
