#!/usr/bin/env python3
"""
Test Optimization #1: Outcome Selection

Compares baseline strategy vs optimized outcome selection.
"""

from smart_arbitrage_sim import SmartArbitrageSimulator
from smart_arbitrage_sim_optimized import OptimizedArbitrageSimulator

db_path = "../../data/raw/epl_arbitrage.db"

print("\n" + "="*80)
print("OPTIMIZATION TEST: Smart Outcome Selection")
print("="*80)
print()
print("Baseline: Wait for lowest odds (3rd highest)")
print("Optimized: Wait for away/home (historically better improvement)")
print()
print("="*80)

# Run baseline
print("\n[1/2] Running BASELINE strategy...")
baseline_sim = SmartArbitrageSimulator(db_path)
baseline = baseline_sim.run_simulation('21/22', 10000, 100)

# Run optimized
print("\n[2/2] Running OPTIMIZED strategy...")
optimized_sim = OptimizedArbitrageSimulator(db_path)
optimized = optimized_sim.run_simulation('21/22', 10000, 100)

# Compare
print("\n" + "="*80)
print("COMPARISON - SEASON 21/22")
print("="*80 + "\n")

print(f"{'Metric':<30} {'Baseline':>15} {'Optimized':>15} {'Improvement':>15}")
print("-" * 80)

metrics = [
    ('Total Profit', f"£{baseline['total_profit']:.2f}", f"£{optimized['total_profit']:.2f}",
     f"+£{optimized['total_profit'] - baseline['total_profit']:.2f}"),
    ('ROI', f"{baseline['roi']:.2f}%", f"{optimized['roi']:.2f}%",
     f"+{optimized['roi'] - baseline['roi']:.2f}%"),
    ('Avg Profit/Bet', f"£{baseline['avg_profit']:.2f}", f"£{optimized['avg_profit']:.2f}",
     f"+£{optimized['avg_profit'] - baseline['avg_profit']:.2f}"),
    ('Avg Profit %', f"{baseline['avg_profit_pct']:.2f}%", f"{optimized['avg_profit_pct']:.2f}%",
     f"+{optimized['avg_profit_pct'] - baseline['avg_profit_pct']:.2f}%"),
    ('Bets Completed', f"{baseline['arbitrages_completed']}", f"{optimized['arbitrages_completed']}",
     f"{optimized['arbitrages_completed'] - baseline['arbitrages_completed']:+d}"),
    ('Avg Odds Change', f"{baseline['avg_odds_change']:.2f}%", f"{optimized['avg_odds_change']:.2f}%",
     f"+{optimized['avg_odds_change'] - baseline['avg_odds_change']:.2f}%"),
]

for metric_name, baseline_val, optimized_val, improvement in metrics:
    print(f"{metric_name:<30} {baseline_val:>15} {optimized_val:>15} {improvement:>15}")

print("\n" + "="*80)
print("OUTCOME ANALYSIS")
print("="*80 + "\n")

# Baseline outcome distribution
baseline_outcomes = {}
for bet in baseline['bets']:
    outcome = bet['bet3_outcome']
    if outcome not in baseline_outcomes:
        baseline_outcomes[outcome] = []
    baseline_outcomes[outcome].append(bet['odds_change_pct'])

# Optimized outcome distribution
optimized_outcomes = {}
for bet in optimized['bets']:
    outcome = bet['bet3_outcome']
    if outcome not in optimized_outcomes:
        optimized_outcomes[outcome] = []
    optimized_outcomes[outcome].append(bet['odds_change_pct'])

print("BASELINE - Bets by outcome:")
for outcome in ['home', 'draw', 'away']:
    if outcome in baseline_outcomes:
        count = len(baseline_outcomes[outcome])
        avg_improvement = sum(baseline_outcomes[outcome]) / count
        print(f"  {outcome.capitalize():5}: {count:3} bets ({count/len(baseline['bets'])*100:5.1f}%), avg improvement {avg_improvement:+6.2f}%")

print("\nOPTIMIZED - Bets by outcome:")
for outcome in ['home', 'draw', 'away']:
    if outcome in optimized_outcomes:
        count = len(optimized_outcomes[outcome])
        avg_improvement = sum(optimized_outcomes[outcome]) / count
        print(f"  {outcome.capitalize():5}: {count:3} bets ({count/len(optimized['bets'])*100:5.1f}%), avg improvement {avg_improvement:+6.2f}%")

# Calculate percentage improvement
profit_improvement_pct = ((optimized['total_profit'] - baseline['total_profit']) / baseline['total_profit']) * 100
roi_improvement_pct = ((optimized['roi'] - baseline['roi']) / baseline['roi']) * 100

print("\n" + "="*80)
print("BOTTOM LINE")
print("="*80)
print(f"• Profit improved by {profit_improvement_pct:+.1f}%")
print(f"• ROI improved by {roi_improvement_pct:+.1f}%")
print(f"• Average profit per bet improved by {((optimized['avg_profit_pct'] - baseline['avg_profit_pct']) / baseline['avg_profit_pct']) * 100:+.1f}%")
print()

if profit_improvement_pct > 0:
    print(f"✓ Optimization #1 SUCCESSFUL!")
    print(f"  By waiting for away/home outcomes instead of always taking lowest odds,")
    print(f"  we improved profit by £{optimized['total_profit'] - baseline['total_profit']:.2f} ({profit_improvement_pct:.1f}%)")
else:
    print("✗ Optimization did not improve results as expected.")
    print("  This may indicate that the baseline already had good outcome selection")
    print("  or that the historical pattern doesn't hold for this season.")

print("="*80 + "\n")
