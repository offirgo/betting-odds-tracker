# Optimization #2: Dynamic Bet Sizing - FAILED

## Summary

**Attempted Strategy**: Scale bet sizes based on predicted profit potential (0.5x to 2.0x base bet).

**Result**: ❌ FAILED - Only 27 bets completed vs 235 baseline, £132 profit vs £577.

## Performance (Season 21/22)

| Metric | Baseline | Opt #1 | Opt #2 (Dynamic) | Result |
|--------|----------|--------|------------------|--------|
| Total Profit | £570.00 | £576.70 | £132.31 | **-76.8%** ❌ |
| ROI | 5.70% | 5.77% | 1.32% | **-76.8%** ❌ |
| Bets Completed | 201 | 235 | 27 | **-86.6%** ❌ |
| Avg Profit/Bet | 2.84% | 2.45% | 4.90% | **+72.5%** ✓ |
| Capital Deployed | £20,100 | £23,500 | £3,250 | **-83.8%** |

## Why It Failed

### Root Cause: Arbitrage Develops Over Time

The strategy has a fundamental flaw:

1. **Dynamic sizing requires initial profit estimates**
   - We calculate bet size based on profit at FIRST snapshot
   - But most matches don't have arbitrage initially!

2. **Filtering eliminates 90% of opportunities**
   - If `combined_inverse >= 1.0` at first snapshot → skip match
   - This removes matches where arbitrage develops later
   - Baseline/Opt#1 bet on these because they WAIT for arbitrage to develop

3. **Only 30 opportunities evaluated vs 235 for Opt#1**
   - 10% exceptional (3 matches)
   - 23% very good (7 matches)
   - 67% good (20 matches)
   - But missing 205 opportunities that develop arbitrage later!

### The Paradox

- **Per-bet efficiency improved**: 3.94% avg profit vs 2.84% baseline (+38.7%)
- **Total profit collapsed**: £132 vs £570 baseline (-76.8%)
- **Capital underutilized**: Only £3,250 deployed vs £20,100 baseline

We picked BETTER bets but missed MOST opportunities.

## Sizing Distribution

Of the 30 opportunities that HAD initial arbitrage:

| Category | Count | % | Bet Size |
|----------|-------|---|----------|
| Exceptional (>5%) | 3 | 10% | £200 (2.0x) |
| Very good (>4%) | 7 | 23% | £150 (1.5x) |
| Good (>3%) | 20 | 67% | £100 (1.0x) |
| Average (>2%) | 0 | 0% | £75 (0.75x) |
| Below average (<2%) | 0 | 0% | £50 (0.5x) |

The sizing logic worked correctly on matches it evaluated, but it didn't see enough matches.

## Lessons Learned

### 1. **Timing-Based Strategies Can't Use Initial Profit**

Our core strategy is:
1. Identify matches with FUTURE arbitrage potential (Model 1)
2. Wait for optimal timing (Model 2/3/4)
3. Complete arbitrage when odds improve

Dynamic sizing needs profit estimates BEFORE placing bets, but:
- Initial profit is often negative or 0% (no arbitrage yet)
- Final profit depends on how much odds improve (unknown initially)
- We can't size bets based on unknown future values

### 2. **Historical Averages Don't Help Enough**

We tried estimating final profit as:
```python
estimated_final_profit = initial_profit + (expected_improvement * 0.3)
# e.g., 0.5% initial + (10% * 0.3) = 3.5% estimated
```

Problems:
- Still filtered out matches with NO initial arbitrage
- Estimates were inaccurate (too aggressive or conservative)
- Didn't account for match-specific factors

### 3. **Volume Matters More Than Efficiency**

In arbitrage betting (zero risk):
- **201 bets @ 2.84% avg = £570** (baseline)
- **27 bets @ 4.90% avg = £132** (dynamic sizing)

More opportunities at lower margins beats fewer opportunities at higher margins when there's no risk.

### 4. **Dynamic Sizing Needs Different Context**

Dynamic sizing works well when:
- ✓ You have MANY opportunities to choose from
- ✓ You can evaluate profit BEFORE committing
- ✓ Risk varies (bet more on safer bets)

Our context:
- ✗ Limited opportunities (~200-235/season)
- ✗ Profit unknown initially (develops over time)
- ✗ Risk is ZERO (arbitrage)

## Alternative Approaches

### Option A: Post-Signal Dynamic Sizing (Not Tested)

Instead of sizing at first snapshot, size AFTER timing signal:
1. Wait for timing signal on 3rd outcome
2. Calculate ACTUAL final profit
3. Size bet accordingly

Problems:
- Complex to implement (need to track partial positions)
- May miss opportunities if bankroll tied up
- Adds execution risk

### Option B: Match Quality Filtering (Better)

Instead of dynamic sizing, SKIP low-quality matches:
- Use Model 1 scores to filter (already done)
- Add: Skip matches with <X% predicted profit
- Bet fixed amount on remaining high-quality matches

This is simpler and aligns with our strategy.

### Option C: Compounding (Already Analyzed)

Instead of per-bet sizing, use percentage of bankroll:
- Bet 1-10% of current bankroll
- Automatically scales as bankroll grows
- Works with our timing strategy

Analysis shows 10% compounding → 76.56% ROI vs 5.70% fixed.

## Recommendation

**DO NOT use dynamic bet sizing for this strategy.**

Instead:
1. ✅ Use Optimization #1 (outcome selection) - proven +1.2% improvement
2. ✅ Implement compounding (estimated +1,246% improvement)
3. ✅ Consider match quality filtering (skip bottom 25%)

Dynamic sizing is the WRONG tool for a timing-based, arbitrage strategy.

## Technical Files

- `smart_arbitrage_sim_dynamic_sizing.py` - Implementation (failed)
- `test_dynamic_sizing.py` - Comparison test
- `optimization_2_results.md` - This document

## Conclusion

This optimization teaches us an important lesson: **not all optimizations work in all contexts**.

Dynamic sizing is a powerful technique, but it requires:
- Known profit at decision time
- Many opportunities to select from
- Variable risk to manage

Our strategy has:
- Unknown profit initially (timing-based)
- Limited opportunities (~200/season)
- Zero risk (arbitrage)

The right optimization for us is **compounding** or **match filtering**, not dynamic sizing.

---

💡 **Key Takeaway**: Failed optimizations are valuable - they teach us what NOT to do and why. This analysis saves future effort by documenting why dynamic sizing doesn't fit our strategy.
