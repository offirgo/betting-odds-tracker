#!/usr/bin/env python3
"""
Compounding Strategy Analysis

Shows the dramatic difference between fixed betting and compounding strategies.
"""

from smart_arbitrage_sim import SmartArbitrageSimulator
import numpy as np

db_path = "../../data/raw/epl_arbitrage.db"
sim = SmartArbitrageSimulator(db_path)

# Get the bet sequence
result = sim.run_simulation('21/22', initial_bankroll=10000, bet_amount=100)
bets = result['bets']

print("\n" + "="*80)
print("COMPOUNDING STRATEGY ANALYSIS - SEASON 21/22")
print("="*80 + "\n")

# Strategy 1: Fixed £100 bets (current)
print("STRATEGY 1: Fixed £100 Bets (What We Did)")
print("-" * 80)
bankroll_fixed = 10000
for bet in bets:
    bankroll_fixed += bet['profit']

print(f"Starting bankroll: £10,000.00")
print(f"Final bankroll: £{bankroll_fixed:,.2f}")
print(f"Total profit: £{bankroll_fixed - 10000:.2f}")
print(f"ROI: {((bankroll_fixed - 10000) / 10000) * 100:.2f}%\n")

# Strategy 2: Bet 1% of current bankroll
print("STRATEGY 2: Bet 1% of Current Bankroll")
print("-" * 80)
bankroll_1pct = 10000
for bet in bets:
    bet_size = bankroll_1pct * 0.01
    profit = bet_size * (bet['profit_pct'] / 100)
    bankroll_1pct += profit

print(f"Starting bankroll: £10,000.00")
print(f"Final bankroll: £{bankroll_1pct:,.2f}")
print(f"Total profit: £{bankroll_1pct - 10000:.2f}")
print(f"ROI: {((bankroll_1pct - 10000) / 10000) * 100:.2f}%\n")

# Strategy 3: Bet 2.5% of current bankroll
print("STRATEGY 3: Bet 2.5% of Current Bankroll")
print("-" * 80)
bankroll_25pct = 10000
for bet in bets:
    bet_size = bankroll_25pct * 0.025
    profit = bet_size * (bet['profit_pct'] / 100)
    bankroll_25pct += profit

print(f"Starting bankroll: £10,000.00")
print(f"Final bankroll: £{bankroll_25pct:,.2f}")
print(f"Total profit: £{bankroll_25pct - 10000:.2f}")
print(f"ROI: {((bankroll_25pct - 10000) / 10000) * 100:.2f}%\n")

# Strategy 4: Bet 5% of current bankroll
print("STRATEGY 4: Bet 5% of Current Bankroll")
print("-" * 80)
bankroll_5pct = 10000
for bet in bets:
    bet_size = bankroll_5pct * 0.05
    profit = bet_size * (bet['profit_pct'] / 100)
    bankroll_5pct += profit

print(f"Starting bankroll: £10,000.00")
print(f"Final bankroll: £{bankroll_5pct:,.2f}")
print(f"Total profit: £{bankroll_5pct - 10000:.2f}")
print(f"ROI: {((bankroll_5pct - 10000) / 10000) * 100:.2f}%\n")

# Strategy 5: Bet 10% of current bankroll
print("STRATEGY 5: Bet 10% of Current Bankroll (AGGRESSIVE)")
print("-" * 80)
bankroll_10pct = 10000
for bet in bets:
    bet_size = bankroll_10pct * 0.10
    profit = bet_size * (bet['profit_pct'] / 100)
    bankroll_10pct += profit

print(f"Starting bankroll: £10,000.00")
print(f"Final bankroll: £{bankroll_10pct:,.2f}")
print(f"Total profit: £{bankroll_10pct - 10000:.2f}")
print(f"ROI: {((bankroll_10pct - 10000) / 10000) * 100:.2f}%\n")

# Strategy 6: Weekly compounding (divide bankroll by weekly bets)
print("STRATEGY 6: Full Weekly Compounding (Divide Bankroll by Weekly Bets)")
print("-" * 80)
print("This divides your current bankroll by ~5.3 bets/week")
print()

# Simulate week by week
bankroll_weekly = 10000
weekly_bets = []
current_week_bets = []

# Group bets by week (approximate)
bets_per_week = len(bets) / 38  # 38 weeks in season

for i, bet in enumerate(bets):
    current_week_bets.append(bet)

    # Every ~5 bets = 1 week
    if len(current_week_bets) >= bets_per_week or i == len(bets) - 1:
        # Calculate bet size for this week
        bet_size_per_bet = bankroll_weekly / len(current_week_bets)

        # Process all bets in the week
        weekly_profit = 0
        for week_bet in current_week_bets:
            profit = bet_size_per_bet * (week_bet['profit_pct'] / 100)
            weekly_profit += profit

        bankroll_weekly += weekly_profit
        current_week_bets = []

print(f"Starting bankroll: £10,000.00")
print(f"Final bankroll: £{bankroll_weekly:,.2f}")
print(f"Total profit: £{bankroll_weekly - 10000:.2f}")
print(f"ROI: {((bankroll_weekly - 10000) / 10000) * 100:.2f}%\n")

# Strategy 7: MAXIMUM COMPOUNDING - bet entire bankroll on each bet
print("STRATEGY 7: MAXIMUM COMPOUNDING (Bet Everything Each Time)")
print("-" * 80)
print("⚠️  ONLY POSSIBLE WITH ARBITRAGE (zero risk)!")
print()
bankroll_max = 10000
for bet in bets:
    profit = bankroll_max * (bet['profit_pct'] / 100)
    bankroll_max += profit

print(f"Starting bankroll: £10,000.00")
print(f"Final bankroll: £{bankroll_max:,.2f}")
print(f"Total profit: £{bankroll_max - 10000:,.2f}")
print(f"ROI: {((bankroll_max - 10000) / 10000) * 100:.2f}%\n")

# Comparison table
print("\n" + "="*80)
print("STRATEGY COMPARISON")
print("="*80 + "\n")

strategies = [
    ("Fixed £100", bankroll_fixed),
    ("1% of Bankroll", bankroll_1pct),
    ("2.5% of Bankroll", bankroll_25pct),
    ("5% of Bankroll", bankroll_5pct),
    ("10% of Bankroll", bankroll_10pct),
    ("Weekly Compound", bankroll_weekly),
    ("Max Compound", bankroll_max),
]

print(f"{'Strategy':<20} {'Final Bankroll':>15} {'Profit':>12} {'ROI':>8}")
print("-" * 80)
for name, final in strategies:
    profit = final - 10000
    roi = ((final - 10000) / 10000) * 100
    print(f"{name:<20} £{final:>14,.2f} £{profit:>10,.2f} {roi:>7.2f}%")

print("\n" + "="*80)
print("KEY INSIGHTS:")
print("="*80)
print(f"• Fixed betting (£100): {((bankroll_fixed - 10000) / 10000) * 100:.2f}% ROI")
print(f"• 10% compounding: {((bankroll_10pct - 10000) / 10000) * 100:.2f}% ROI ({((bankroll_10pct - bankroll_fixed) / (bankroll_fixed - 10000)) * 100:.0f}% better!)")
print(f"• Max compounding: {((bankroll_max - 10000) / 10000) * 100:.2f}% ROI ({((bankroll_max - bankroll_fixed) / (bankroll_fixed - 10000)) * 100:.0f}% better!)")
print()
print("⚠️  IMPORTANT: Max compounding only works with arbitrage (zero risk)")
print("    With normal betting, you'd go bankrupt trying this!")
print("="*80 + "\n")

# Annual projection
print("="*80)
print("WHAT IF YOU DID THIS FOR 4 SEASONS? (Compounding Across Years)")
print("="*80 + "\n")

# Run all 4 seasons with max compounding
seasons = ['21/22', '22/23', '23/24', '24/25']
bankroll_multi_season = 10000

print("Max Compounding Strategy (Bet 100% Each Time):")
print("-" * 80)
for season in seasons:
    result_season = sim.run_simulation(season, initial_bankroll=10000, bet_amount=100)

    # Apply compounding
    season_start = bankroll_multi_season
    for bet in result_season['bets']:
        profit = bankroll_multi_season * (bet['profit_pct'] / 100)
        bankroll_multi_season += profit

    season_profit = bankroll_multi_season - season_start
    season_roi = (season_profit / season_start) * 100

    print(f"Season {season}:")
    print(f"  Start: £{season_start:,.2f}")
    print(f"  End: £{bankroll_multi_season:,.2f}")
    print(f"  Profit: £{season_profit:,.2f} ({season_roi:.2f}%)")
    print()

total_roi = ((bankroll_multi_season - 10000) / 10000) * 100
print(f"{'='*80}")
print(f"4-Year Result:")
print(f"  Starting bankroll: £10,000.00")
print(f"  Final bankroll: £{bankroll_multi_season:,.2f}")
print(f"  Total profit: £{bankroll_multi_season - 10000:,.2f}")
print(f"  Total ROI: {total_roi:.2f}%")
print(f"  Turned £10k into £{bankroll_multi_season/1000:.0f}k!")
print(f"{'='*80}\n")
