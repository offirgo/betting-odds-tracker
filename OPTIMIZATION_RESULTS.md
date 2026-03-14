# Arbitrage Optimization Results

This document tracks results from testing various optimization strategies.

## Optimization #1: Model Confidence Weighting

**Concept**: Scale bet sizes by model confidence, using signal timing as a proxy.
- Early signals (>10 days before): 1.4x multiplier (high confidence)
- Mid signals (5-10 days): 1.2x multiplier
- Late signals (2-5 days): 1.0x multiplier
- Very late (<2 days): 0.9x multiplier
- No signal (fallback): 0.8x multiplier

**Test Season**: 24/25 (hold-out validation)
**Base Bet**: £100

### Results

#### Threshold 0.55 (Original)
| Metric | Baseline | Confidence-Weighted | Change |
|--------|----------|---------------------|--------|
| Profit | £584.62 | £562.64 | **-£22.00 (-3.8%)** |
| ROI | 5.85% | 5.63% | -0.22% |
| Completed Bets | 160 | 160 | 0 |
| Signals Fired | 122 (76.2%) | 122 (76.2%) | 0 |
| Avg Multiplier | N/A | 1.00x | - |
| Avg Days Before | N/A | 3.4 | - |

**Assessment**: ❌ **Small regression** - Confidence weighting slightly reduces profit with conservative threshold

#### Threshold 0.50
| Metric | Baseline | Confidence-Weighted | Change |
|--------|----------|---------------------|--------|
| Profit | £197.53 | £211.33 | **+£13.80 (+7.0%)** |
| ROI | 1.98% | 2.11% | +0.13% |
| Completed Bets | 86 | 86 | 0 |
| Signals Fired | 86 (100%) | 86 (100%) | 0 |
| Avg Multiplier | N/A | 1.11x | - |
| Avg Days Before | N/A | 5.6 | - |

**Assessment**: ✓ **Moderate improvement** - Confidence weighting improves profit by 7%

### Overall Conclusion

**Mixed Results**:
- Improves profit for aggressive thresholds (0.50): +7.0%
- Slight regression for conservative thresholds (0.55): -3.8%
- Average multipliers close to 1.0x suggest limited differentiation
- Timing-based confidence proxy may not capture true model confidence well

**Recommendation**:
- Consider keeping for threshold 0.50 specifically
- May not be worth the added complexity for threshold 0.55
- Future improvement: Use actual model probability scores instead of timing proxy

**Implementation**: `src/simulator/confidence_weighted_sim.py`

---

## Optimization #2: Match Filtering by Characteristics

**Concept**: Filter out matches with unfavorable characteristics to focus on high-quality opportunities.

Filters applied:
1. Heavy favorites (min odds < 1.4)
2. Too balanced (all odds 2.5-3.5)
3. Extreme underdogs (max odds > 15.0)
4. Low volatility (odds range < 2.0)

**Test Season**: 24/25 (hold-out validation)
**Base Bet**: £100

### Results

#### Threshold 0.55 (Original)
| Metric | Baseline | With Filtering | Change |
|--------|----------|----------------|--------|
| Profit | £584.62 | £283.62 | **-£301.00 (-51.5%)** |
| ROI | 5.85% | 2.84% | -3.01% |
| Completed Bets | 160 | 78 | -82 (-51.2%) |
| Matches Filtered | 0 | 218 (56.5%) | +218 |
| Avg Profit/Bet | £3.65 | £3.64 | -0.01% |

**Filtering breakdown**:
- Heavy favorites: 57 matches (14.8%)
- Too balanced: 27 matches (7.0%)
- Low volatility: 134 matches (34.7%)
- Extreme underdogs: 0 matches (0.0%)

#### Threshold 0.50
| Metric | Baseline | With Filtering | Change |
|--------|----------|----------------|--------|
| Profit | £197.53 | £88.70 | **-£108.83 (-55.1%)** |
| ROI | 1.98% | 0.89% | -1.09% |
| Completed Bets | 86 | 40 | -46 (-53.5%) |
| Matches Filtered | 0 | 218 (56.5%) | +218 |
| Avg Profit/Bet | £2.30 | £2.22 | -0.08% |

### Overall Conclusion

**Failed Optimization**:
- Threshold 0.55: -51.5% profit (£584.62 → £283.62)
- Threshold 0.50: -55.1% profit (£197.53 → £88.70)
- Filters out 56.5% of matches (218 of 386)
- Reduces completed bets by ~50%
- Profit per bet remains similar, but volume drops drastically

**Why it failed**:
- Filters are too aggressive - removing profitable opportunities
- Low volatility filter (34.7% of matches) removes many good arbitrage opportunities
- The "unfavorable" matches were actually profitable
- Arbitrage doesn't require market efficiency - it exploits pricing mismatches

**Recommendation**: ❌ **Do NOT use match filtering**
- Filtering reduces opportunities without improving quality
- Keep all matches that show future arbitrage potential

**Implementation**: `src/simulator/match_filtering_sim.py` (for reference only)

---

## Optimization #3: Early Exit Strategy

**Concept**: Exit arbitrage early when odds improve favorably, rather than always waiting for timing signal.

Strategy:
- Monitor third outcome odds continuously
- Exit early if odds create arbitrage opportunity ≥ 1.0% profit
- Otherwise wait for timing signal or fallback to last snapshot

**Test Season**: 24/25 (hold-out validation)
**Base Bet**: £100
**Min Profit for Early Exit**: 1.0%

### Results

#### Threshold 0.55 (Original)
| Metric | Baseline | Early Exit | Change |
|--------|----------|------------|--------|
| Profit | £584.62 | £265.86 | **-£318.76 (-54.5%)** |
| ROI | 5.85% | 2.66% | -3.19% |
| Completed Bets | 160 | 160 | 0 |
| Early Exits | N/A | 129 (80.6%) | - |
| Timing Signals | 122 | - | - |
| Avg Profit/Bet | £3.65 | £1.66 | -54.5% |

#### Threshold 0.50
| Metric | Baseline | Early Exit | Change |
|--------|----------|------------|--------|
| Profit | £197.53 | £151.05 | **-£46.48 (-23.5%)** |
| ROI | 1.98% | 1.51% | -0.47% |
| Completed Bets | 86 | 93 | +7 |
| Early Exits | N/A | 76 (81.7%) | - |
| Timing Signals | 86 | - | - |
| Avg Profit/Bet | £2.30 | £1.62 | -29.6% |

### Overall Conclusion

**Failed Optimization**:
- Threshold 0.55: -54.5% profit (£584.62 → £265.86)
- Threshold 0.50: -23.5% profit (£197.53 → £151.05)
- 80%+ of bets exit early with minimal profit
- Average profit per bet drops significantly

**Why it failed**:
- 1.0% minimum profit threshold is too low
- Exits prematurely before odds fully improve
- Timing models already capture optimal entry points
- "Greed is good" - waiting for better odds pays off
- Early exit sacrifices potential profit for certainty

**Recommendation**: ❌ **Do NOT use early exit strategy**
- Timing models are better at predicting optimal bet timing
- Patience yields higher profits than premature exits
- If odds improve early, they often improve more later

**Implementation**: `src/simulator/early_exit_sim.py` (for reference only)

---

## Optimization #4: Multi-Threshold Ensemble

**Concept**: Combine signals from both threshold 0.55 and 0.50 - bet if EITHER threshold signals.

Strategy:
- Monitor both `should_bet_*_now` (0.55) and `should_bet_*_now_t050` (0.50)
- Place bet if either threshold signals
- Aims to combine conservative precision with aggressive coverage

**Test Season**: 24/25 (hold-out validation)
**Base Bet**: £100

### Results

| Metric | Threshold 0.55 | Threshold 0.50 | Ensemble | vs Best |
|--------|----------------|----------------|----------|---------|
| Profit | £584.62 | £197.53 | £250.01 | **-£334.61 (-57.2%)** |
| ROI | 5.85% | 1.98% | 2.50% | -3.35% |
| Completed Bets | 160 | 86 | 98 | -62 |
| Avg Profit/Bet | £3.65 | £2.30 | £2.55 | -£1.10 |

**Signal breakdown (ensemble)**:
- Threshold 0.55 only: 36 (36.7%)
- Threshold 0.50 only: 56 (57.1%)
- Both thresholds: 6 (6.1%)

### Overall Conclusion

**Failed Optimization**:
- Performs worse than best individual threshold (0.55)
- Gets fewer bets than expected (98 vs 160 from 0.55 alone)
- Average profit per bet (£2.55) between the two thresholds
- No synergistic benefit from combining signals

**Why it failed**:
- Different thresholds trigger at different times
- By the time second threshold signals, odds may have changed
- Conservative threshold (0.55) already provides best results
- Adding aggressive signals dilutes quality without adding enough volume

**Recommendation**: ❌ **Do NOT use ensemble**
- Stick with single best threshold (0.55 original)
- Ensemble adds complexity without benefit

**Implementation**: `src/simulator/multi_threshold_ensemble_sim.py` (for reference only)

---

## Optimization #5: Odds Movement Prediction

**Status**: Pending

**Expected Impact**: +15-25% profit

---

## Optimization #6: Bankroll Management (Kelly Criterion)

**Status**: Pending

**Expected Impact**: +20-40% long-term profit

---

## Optimization #7: Transaction Cost Awareness

**Status**: Pending

**Expected Impact**: +2-3% profit
