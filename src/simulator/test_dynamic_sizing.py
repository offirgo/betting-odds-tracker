#!/usr/bin/env python3
"""
Test Optimization #2: Dynamic Bet Sizing

Compares three strategies:
1. Baseline (fixed £100 bets)
2. Optimization #1 (outcome selection)
3. Optimization #2 (dynamic sizing + outcome selection)
"""

from smart_arbitrage_sim import SmartArbitrageSimulator
from smart_arbitrage_sim_optimized import OptimizedArbitrageSimulator
from smart_arbitrage_sim_dynamic_sizing import DynamicSizingSimulator

db_path = "../../data/raw/epl_arbitrage.db"

print("\n" + "="*80)
print("OPTIMIZATION TEST: Dynamic Bet Sizing")
print("="*80)
print()
print("Three strategies to compare:")
print("  1. Baseline: Fixed £100 bets, wait for lowest odds")
print("  2. Opt #1: Fixed £100 bets, wait for away/home (outcome selection)")
print("  3. Opt #2: Dynamic sizing + outcome selection")
print()
print("="*80)

# Run baseline
print("\n[1/3] Running BASELINE strategy...")
baseline_sim = SmartArbitrageSimulator(db_path)
baseline = baseline_sim.run_simulation('21/22', 10000, 100)

# Run optimization #1
print("\n[2/3] Running OPTIMIZATION #1 (outcome selection)...")
opt1_sim = OptimizedArbitrageSimulator(db_path)
opt1 = opt1_sim.run_simulation('21/22', 10000, 100)

# Run optimization #2
print("\n[3/3] Running OPTIMIZATION #2 (dynamic sizing)...")
opt2_sim = DynamicSizingSimulator(db_path, base_bet_amount=100)
opt2 = opt2_sim.run_simulation('21/22', 10000)

# Compare
print("\n" + "="*80)
print("COMPARISON - SEASON 21/22")
print("="*80 + "\n")

print(f"{'Metric':<30} {'Baseline':>15} {'Opt #1':>15} {'Opt #2':>15}")
print("-" * 80)

metrics = [
    ('Total Profit', f"£{baseline['total_profit']:.2f}",
     f"£{opt1['total_profit']:.2f}",
     f"£{opt2['total_profit']:.2f}"),
    ('ROI', f"{baseline['roi']:.2f}%",
     f"{opt1['roi']:.2f}%",
     f"{opt2['roi']:.2f}%"),
    ('Bets Completed', f"{baseline['arbitrages_completed']}",
     f"{opt1['arbitrages_completed']}",
     f"{opt2['arbitrages_completed']}"),
    ('Avg Profit/Bet', f"£{baseline['avg_profit']:.2f}",
     f"£{opt1['avg_profit']:.2f}",
     f"£{opt2['avg_profit']:.2f}"),
    ('Avg Profit %', f"{baseline['avg_profit_pct']:.2f}%",
     f"{opt1['avg_profit_pct']:.2f}%",
     f"{opt2['avg_profit_pct']:.2f}%"),
]

for metric_name, baseline_val, opt1_val, opt2_val in metrics:
    print(f"{metric_name:<30} {baseline_val:>15} {opt1_val:>15} {opt2_val:>15}")

# Additional metrics for dynamic sizing
print("\n" + "="*80)
print("DYNAMIC SIZING DETAILS (Opt #2)")
print("="*80 + "\n")

if 'avg_stake' in opt2:
    print(f"Total capital deployed: £{opt2['total_stakes']:,.2f}")
    print(f"Average stake per bet: £{opt2['avg_stake']:.2f}")
    print(f"  vs. Baseline: £100.00 (fixed)")
    print()

if 'sizing_stats' in opt2:
    stats = opt2['sizing_stats']
    total = sum(stats.values())
    print("Sizing distribution:")
    print(f"  Exceptional (>5%):   {stats['very_high']:>3} ({stats['very_high']/total*100:5.1f}%) - 2.0x bet (£200)")
    print(f"  Very good (>4%):     {stats['high']:>3} ({stats['high']/total*100:5.1f}%) - 1.5x bet (£150)")
    print(f"  Good (>3%):          {stats['medium']:>3} ({stats['medium']/total*100:5.1f}%) - 1.0x bet (£100)")
    print(f"  Average (>2%):       {stats['low']:>3} ({stats['low']/total*100:5.1f}%) - 0.75x bet (£75)")
    print(f"  Below average (<2%): {stats['very_low']:>3} ({stats['very_low']/total*100:5.1f}%) - 0.5x bet (£50)")
    if stats['skipped'] > 0:
        print(f"  Skipped:             {stats['skipped']:>3} ({stats['skipped']/total*100:5.1f}%)")
    print()

# Calculate improvements
print("="*80)
print("IMPROVEMENT ANALYSIS")
print("="*80 + "\n")

baseline_profit = baseline['total_profit']
opt1_profit = opt1['total_profit']
opt2_profit = opt2['total_profit']

baseline_roi = baseline['roi']
opt1_roi = opt1['roi']
opt2_roi = opt2['roi']

print("vs. BASELINE:")
print(f"  Opt #1 profit: +£{opt1_profit - baseline_profit:.2f} ({(opt1_profit - baseline_profit)/baseline_profit*100:+.1f}%)")
print(f"  Opt #1 ROI: {opt1_roi - baseline_roi:+.2f}% ({(opt1_roi - baseline_roi)/baseline_roi*100:+.1f}%)")
print()
print(f"  Opt #2 profit: +£{opt2_profit - baseline_profit:.2f} ({(opt2_profit - baseline_profit)/baseline_profit*100:+.1f}%)")
print(f"  Opt #2 ROI: {opt2_roi - baseline_roi:+.2f}% ({(opt2_roi - baseline_roi)/baseline_roi*100:+.1f}%)")
print()

print("Opt #2 vs. Opt #1:")
print(f"  Additional profit: +£{opt2_profit - opt1_profit:.2f} ({(opt2_profit - opt1_profit)/opt1_profit*100:+.1f}%)")
print(f"  Additional ROI: {opt2_roi - opt1_roi:+.2f}% ({(opt2_roi - opt1_roi)/opt1_roi*100:+.1f}%)")
print()

# Efficiency analysis
print("="*80)
print("CAPITAL EFFICIENCY")
print("="*80 + "\n")

baseline_turnover = baseline['arbitrages_completed'] * 100
opt1_turnover = opt1['arbitrages_completed'] * 100
opt2_turnover = opt2.get('total_stakes', opt2['arbitrages_completed'] * 100)

print(f"{'Strategy':<20} {'Turnover':>15} {'Profit':>15} {'Margin':>10}")
print("-" * 65)
print(f"{'Baseline':<20} £{baseline_turnover:>13,.2f} £{baseline_profit:>13,.2f} {baseline_profit/baseline_turnover*100:>9.2f}%")
print(f"{'Opt #1':<20} £{opt1_turnover:>13,.2f} £{opt1_profit:>13,.2f} {opt1_profit/opt1_turnover*100:>9.2f}%")
print(f"{'Opt #2':<20} £{opt2_turnover:>13,.2f} £{opt2_profit:>13,.2f} {opt2_profit/opt2_turnover*100:>9.2f}%")
print()

print("="*80)
print("BOTTOM LINE")
print("="*80)

if opt2_roi > baseline_roi:
    roi_improvement = ((opt2_roi - baseline_roi) / baseline_roi) * 100
    print(f"✓ Optimization #2 SUCCESSFUL!")
    print(f"  ROI improved from {baseline_roi:.2f}% to {opt2_roi:.2f}% (+{roi_improvement:.1f}%)")
    print(f"  Total profit increased by £{opt2_profit - baseline_profit:.2f}")
    print()

    if 'avg_stake' in opt2:
        print(f"  Key insight: By betting MORE on high-profit opportunities and")
        print(f"  LESS on low-profit ones, we improved ROI while managing risk.")
        print(f"  Average stake: £{opt2['avg_stake']:.2f} (vs £100 fixed)")
else:
    print("✗ Optimization did not improve results as expected.")

print("="*80 + "\n")
