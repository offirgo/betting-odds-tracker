#!/usr/bin/env python3
"""
Compare Different Threshold Versions

Runs simulations with different thresholds and compares results side-by-side.

Usage:
    python compare_thresholds.py         # Compare original (0.55) vs new (0.50)
    python compare_thresholds.py 21/22   # Specify season
"""

import sys
from smart_arbitrage_sim_versioned import VersionedArbitrageSimulator

db_path = "../../data/raw/epl_arbitrage.db"

# Get season from command line or use default
season = sys.argv[1] if len(sys.argv) > 1 else '21/22'

print("\n" + "="*80)
print(f"THRESHOLD COMPARISON - SEASON {season}")
print("="*80)
print()
print("Comparing:")
print("  Version 1: Original (threshold 0.55)")
print("  Version 2: New (threshold 0.50)")
print()
print("="*80 + "\n")

# Run original (0.55)
print("[1/2] Running with ORIGINAL threshold (0.55)...\n")
sim_original = VersionedArbitrageSimulator(db_path, 'original')
result_original = sim_original.run_simulation(season, 10000, 100)

if not result_original:
    print("\n✗ Original simulation failed")
    sys.exit(1)

# Run new (0.50)
print("\n[2/2] Running with NEW threshold (0.50)...\n")
sim_new = VersionedArbitrageSimulator(db_path, '050')
result_new = sim_new.run_simulation(season, 10000, 100)

if not result_new:
    print("\n✗ New simulation failed")
    print("\nDid you run regenerate_timing_labels.py?")
    print("  python regenerate_timing_labels.py 0.50")
    sys.exit(1)

# Compare results
print("\n" + "="*80)
print(f"COMPARISON RESULTS - SEASON {season}")
print("="*80 + "\n")

print(f"{'Metric':<30} {'Original (0.55)':>18} {'New (0.50)':>18} {'Change':>18}")
print("-" * 90)

metrics = [
    ('Total Profit', f"£{result_original['total_profit']:.2f}",
     f"£{result_new['total_profit']:.2f}",
     f"+£{result_new['total_profit'] - result_original['total_profit']:.2f}"),

    ('ROI', f"{result_original['roi']:.2f}%",
     f"{result_new['roi']:.2f}%",
     f"{result_new['roi'] - result_original['roi']:+.2f}%"),

    ('Bets Completed', f"{result_original['arbitrages_completed']}",
     f"{result_new['arbitrages_completed']}",
     f"{result_new['arbitrages_completed'] - result_original['arbitrages_completed']:+d}"),

    ('Signals Fired', f"{result_original['signals_fired']}",
     f"{result_new['signals_fired']}",
     f"{result_new['signals_fired'] - result_original['signals_fired']:+d}"),

    ('No Signals', f"{result_original['no_signals']}",
     f"{result_new['no_signals']}",
     f"{result_new['no_signals'] - result_original['no_signals']:+d}"),

    ('Signal Coverage', f"{result_original['signal_coverage']:.1f}%",
     f"{result_new['signal_coverage']:.1f}%",
     f"{result_new['signal_coverage'] - result_original['signal_coverage']:+.1f}%"),

    ('Avg Profit/Bet', f"{result_original['avg_profit_pct']:.2f}%",
     f"{result_new['avg_profit_pct']:.2f}%",
     f"{result_new['avg_profit_pct'] - result_original['avg_profit_pct']:+.2f}%"),
]

for metric_name, orig_val, new_val, change in metrics:
    print(f"{metric_name:<30} {orig_val:>18} {new_val:>18} {change:>18}")

print()

# Calculate percentage improvements
profit_improvement = ((result_new['total_profit'] - result_original['total_profit']) /
                      result_original['total_profit'] * 100) if result_original['total_profit'] > 0 else 0

bets_improvement = ((result_new['arbitrages_completed'] - result_original['arbitrages_completed']) /
                    result_original['arbitrages_completed'] * 100) if result_original['arbitrages_completed'] > 0 else 0

signals_improvement = ((result_new['signals_fired'] - result_original['signals_fired']) /
                       result_original['signals_fired'] * 100) if result_original['signals_fired'] > 0 else 0

# Assessment
print("="*80)
print("ASSESSMENT")
print("="*80 + "\n")

print(f"Profit change: {profit_improvement:+.1f}%")
print(f"Bets change: {bets_improvement:+.1f}%")
print(f"Signal coverage change: {signals_improvement:+.1f}%")
print()

if profit_improvement >= 5:
    print("✅ STRONG IMPROVEMENT - Recommend deploying new threshold")
    print(f"   Profit improved by {profit_improvement:.1f}% (>{5}% threshold)")
elif profit_improvement >= 2:
    print("✓ MODERATE IMPROVEMENT - Worth considering")
    print(f"   Profit improved by {profit_improvement:.1f}%")
elif profit_improvement >= 0:
    print("⚠ MINOR IMPROVEMENT - May not be worth the complexity")
    print(f"   Profit improved by {profit_improvement:.1f}% (<{2}%)")
else:
    print("❌ REGRESSION - Do NOT deploy new threshold")
    print(f"   Profit decreased by {profit_improvement:.1f}%")

print()

# Recommendation
print("="*80)
print("RECOMMENDATION")
print("="*80)
print()

if profit_improvement >= 5:
    print("Deploy new threshold (0.50):")
    print("  1. Validated on hold-out (24/25)")
    print("  2. Improves profit by >5%")
    print("  3. Increases signal coverage significantly")
    print()
    print("Action: Update production to use threshold 0.50")

elif profit_improvement >= 0:
    print("Test on other seasons:")
    print(f"  1. Current improvement: {profit_improvement:.1f}%")
    print("  2. Test on 22/23, 23/24, 24/25")
    print("  3. If consistent across all seasons, deploy")
    print()
    print("Action: Run comparison on all seasons")

else:
    print("Keep original threshold (0.55):")
    print(f"  1. New threshold performs worse (-{abs(profit_improvement):.1f}%)")
    print("  2. Stick with current (validated) approach")
    print()
    print("Action: No changes to production")

print()
print("="*80 + "\n")
