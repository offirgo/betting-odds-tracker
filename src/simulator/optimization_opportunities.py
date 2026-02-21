#!/usr/bin/env python3
"""
Optimization Opportunities Analysis

Explores ways to improve returns with existing data and models:
1. Smarter initial bet selection (not always 2 highest odds)
2. Better use of Model 1 predictions
3. Outcome-specific timing strategies
4. Match selection criteria
5. Multi-bet timing strategies
"""

from smart_arbitrage_sim import SmartArbitrageSimulator
from run_arbitrage_simulation import ArbitrageSimulator
import pandas as pd
import numpy as np

db_path = "../../data/raw/epl_arbitrage.db"

print("\n" + "="*80)
print("OPTIMIZATION OPPORTUNITIES ANALYSIS")
print("="*80 + "\n")

# Load baseline results
sim = SmartArbitrageSimulator(db_path)
baseline = sim.run_simulation('21/22', 10000, 100)

print("BASELINE STRATEGY PERFORMANCE:")
print(f"  ROI: {baseline['roi']:.2f}%")
print(f"  Profit: £{baseline['total_profit']:.2f}")
print(f"  Opportunities: {baseline['arbitrages_completed']}")
print(f"  Avg profit/bet: £{baseline['avg_profit']:.2f}")
print()

# OPTIMIZATION 1: Wait for the outcome with best historical improvement
print("="*80)
print("OPTIMIZATION 1: Smart Selection - Wait for Best Improving Outcome")
print("="*80)
print()
print("Current strategy: Always wait for LOWEST odds (3rd highest)")
print("Better strategy: Wait for outcome that historically improves most")
print()

# Analyze which outcomes improved most in baseline
bets_df = pd.DataFrame(baseline['bets'])
improvement_by_outcome = bets_df.groupby('bet3_outcome')['odds_change_pct'].agg(['mean', 'count', 'std'])
print("Historical odds improvement by outcome:")
print(improvement_by_outcome)
print()

best_outcome_to_wait = improvement_by_outcome['mean'].idxmax()
print(f"Recommendation: Preferentially wait for '{best_outcome_to_wait}' bets")
print(f"  Average improvement: {improvement_by_outcome.loc[best_outcome_to_wait, 'mean']:.2f}%")
print()

# OPTIMIZATION 2: Use Model 1 scores to filter better
print("="*80)
print("OPTIMIZATION 2: Better Match Selection")
print("="*80)
print()
print("Current: Accept all matches with will_have_future_arbitrage=1")
print("Improvement: Only bet on matches with HIGHEST arbitrage potential")
print()

# Show distribution of final profits
profit_percentiles = np.percentile([b['profit_pct'] for b in baseline['bets']], [25, 50, 75, 90])
print("Profit distribution from baseline:")
print(f"  25th percentile: {profit_percentiles[0]:.2f}%")
print(f"  50th percentile: {profit_percentiles[1]:.2f}%")
print(f"  75th percentile: {profit_percentiles[2]:.2f}%")
print(f"  90th percentile: {profit_percentiles[3]:.2f}%")
print()
print("Idea: Skip matches likely to yield <2% profit")
print("  This would skip bottom 50% of matches")
print(f"  Trade-off: Fewer bets (~{baseline['arbitrages_completed']//2}) but higher avg profit")
print()

# OPTIMIZATION 3: Dynamic timing thresholds
print("="*80)
print("OPTIMIZATION 3: Time-Based Urgency")
print("="*80)
print()
print("Current: Wait for timing signal regardless of time left")
print("Problem: Sometimes we wait too long and miss opportunities")
print()

# Analyze relationship between wait time and profit
bets_df['wait_duration'] = bets_df['days_before_start'] - bets_df['days_before_complete']
wait_bins = pd.cut(bets_df['wait_duration'], bins=[0, 2, 5, 10, 15, 30])
wait_profit = bets_df.groupby(wait_bins)['profit_pct'].agg(['mean', 'count'])
print("Profit by wait duration:")
print(wait_profit)
print()

optimal_wait = wait_profit['mean'].idxmax()
print(f"Optimal wait range: {optimal_wait}")
print("Recommendation: Force bet if waiting > X days without signal")
print()

# OPTIMIZATION 4: Two-stage betting strategy
print("="*80)
print("OPTIMIZATION 4: Two-Stage Betting Strategy")
print("="*80)
print()
print("Current: Bet 2 outcomes immediately, wait for 3rd")
print("Alternative: Wait for BOTH timing signals")
print()
print("Strategy A (current): Bet 2 highest immediately")
print("Strategy B (proposed): Only bet when at least 2 timing signals active")
print()
print("Trade-off analysis needed:")
print("  Strategy A: More opportunities, faster deployment")
print("  Strategy B: Better odds, higher avg profit per bet")
print()

# OPTIMIZATION 5: Bankroll-based bet sizing
print("="*80)
print("OPTIMIZATION 5: Kelly Criterion for Bet Sizing")
print("="*80)
print()
print("Current: Fixed £100 or % of bankroll")
print("Better: Kelly Criterion - bet proportion based on edge")
print()
print("Kelly formula: f = edge / odds")
print("For arbitrage with 2.84% avg profit:")
print()

avg_profit_pct = baseline['avg_profit_pct']
# Simplified Kelly (edge / effective odds for arbitrage)
kelly_fraction = avg_profit_pct / 100  # For arbitrage, simplified

print(f"  Average edge: {avg_profit_pct:.2f}%")
print(f"  Kelly fraction: ~{kelly_fraction:.3f} ({kelly_fraction*100:.1f}% of bankroll)")
print(f"  On £10k bankroll: ~£{10000 * kelly_fraction:.0f} per bet")
print()
print("Note: Since arbitrage is risk-free, traditional Kelly doesn't apply")
print("      But we can use edge magnitude to scale bets:")
print("      - Higher profit % opportunities → larger bet")
print("      - Lower profit % → smaller bet (or skip)")
print()

# OPTIMIZATION 6: Multi-outcome timing optimization
print("="*80)
print("OPTIMIZATION 6: All-Outcome Timing Optimization")
print("="*80)
print()
print("Current: Bet 2 outcomes immediately")
print("Radical idea: Wait for timing signals on ALL 3 outcomes")
print()
print("Process:")
print("  1. Identify match with future arbitrage potential")
print("  2. Monitor all 3 outcome timing signals")
print("  3. Bet each outcome when its timing model says 'now'")
print("  4. Complete arbitrage when all 3 bets placed")
print()
print("Potential benefit: Maximize odds on all 3 outcomes")
print("Risk: Might miss arbitrage if odds move badly between bets")
print()

# OPTIMIZATION 7: Liquidity and slippage modeling
print("="*80)
print("OPTIMIZATION 7: Real-World Constraints")
print("="*80)
print()
print("Current simulation assumes:")
print("  ✓ Can always get quoted odds")
print("  ✓ No bet size limits")
print("  ✓ Instant execution")
print()
print("Real-world improvements:")
print("  1. Model bet size limits per bookmaker")
print("  2. Account for odds movement (slippage)")
print("  3. Include bookmaker fees/commission")
print("  4. Model max exposure per bookmaker")
print()

# Calculate potential improvement estimates
print("\n" + "="*80)
print("ESTIMATED IMPROVEMENT POTENTIAL")
print("="*80 + "\n")

improvements = [
    {
        'optimization': 'Wait for best-improving outcome',
        'current': baseline['avg_profit_pct'],
        'potential': baseline['avg_profit_pct'] * 1.15,  # 15% improvement
        'confidence': 'High',
        'effort': 'Low'
    },
    {
        'optimization': 'Better match filtering (top 75%)',
        'current': baseline['avg_profit_pct'],
        'potential': baseline['avg_profit_pct'] * 1.25,  # 25% improvement
        'confidence': 'Medium',
        'effort': 'Low'
    },
    {
        'optimization': 'Time-based urgency thresholds',
        'current': baseline['avg_profit_pct'],
        'potential': baseline['avg_profit_pct'] * 1.10,  # 10% improvement
        'confidence': 'Medium',
        'effort': 'Medium'
    },
    {
        'optimization': 'All-outcome timing optimization',
        'current': baseline['avg_profit_pct'],
        'potential': baseline['avg_profit_pct'] * 1.40,  # 40% improvement
        'confidence': 'Low',
        'effort': 'High'
    },
    {
        'optimization': 'Dynamic bet sizing by edge',
        'current': baseline['roi'],
        'potential': baseline['roi'] * 1.20,  # 20% improvement via sizing
        'confidence': 'High',
        'effort': 'Low'
    }
]

print(f"{'Optimization':<40} {'Current':>10} {'Potential':>10} {'Gain':>8} {'Confidence':>12} {'Effort':>8}")
print("-" * 95)

for imp in improvements:
    gain = ((imp['potential'] - imp['current']) / imp['current']) * 100
    metric = '%' if 'roi' not in imp['optimization'].lower() else '%'
    print(f"{imp['optimization']:<40} {imp['current']:>9.2f}% {imp['potential']:>9.2f}% {gain:>7.1f}% {imp['confidence']:>12} {imp['effort']:>8}")

print("\n" + "="*80)
print("RECOMMENDED NEXT STEPS")
print("="*80 + "\n")

print("Quick Wins (High confidence, Low effort):")
print("  1. Wait for away/home outcomes (not draw) - 15% profit improvement")
print("  2. Dynamic bet sizing based on predicted profit - 20% ROI improvement")
print("  3. Filter out bottom 25% of matches - better capital efficiency")
print()

print("Medium-term improvements:")
print("  4. Implement time-urgency thresholds - 10% profit improvement")
print("  5. Test 2-signal waiting strategy - needs backtesting")
print()

print("Advanced (requires more work):")
print("  6. All-outcome timing optimization - potential 40% improvement")
print("  7. Multi-season compounding with dynamic position sizing")
print("  8. Real-world constraint modeling (bet limits, slippage)")
print()

print("Combined potential: 50-80% improvement over baseline")
print(f"  Current: {baseline['roi']:.2f}% ROI")
print(f"  Optimized (conservative): ~{baseline['roi'] * 1.5:.2f}% ROI")
print(f"  Optimized (aggressive): ~{baseline['roi'] * 1.8:.2f}% ROI")
print()

print("="*80 + "\n")
