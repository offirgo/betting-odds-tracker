# Threshold Versioning System

This system allows you to test different ML model thresholds without losing your original data or results.

## Overview

**Problem**: We want to test if lowering timing model thresholds from 0.55 to 0.50 improves results.

**Solution**: Create new columns in the database for each threshold version, so you can run simulations with either threshold at any time.

## Database Structure

### Original (Threshold 0.55)
```
should_bet_home_now
should_bet_draw_now
should_bet_away_now
```

### New Version (Threshold 0.50)
```
should_bet_home_now_t050
should_bet_draw_now_t050
should_bet_away_now_t050
```

### Future Versions (if needed)
```
should_bet_*_now_t045  (threshold 0.45)
should_bet_*_now_t060  (threshold 0.60)
etc.
```

## Step-by-Step Usage

### Step 1: Generate New Labels

Run the regeneration script with desired threshold:

```bash
cd src/models
python regenerate_timing_labels.py 0.50
```

This will:
- Load the trained models
- Re-run predictions with threshold 0.50
- Create new columns: `should_bet_*_now_t050`
- Keep original columns unchanged

**Time**: ~5-10 minutes depending on data size

### Step 2: Run Comparison

Compare original vs. new threshold:

```bash
cd ../simulator
python compare_thresholds.py 21/22
```

This will run both versions and show side-by-side comparison.

### Step 3: Test on All Seasons

```bash
python compare_thresholds.py 21/22
python compare_thresholds.py 22/23
python compare_thresholds.py 23/24
python compare_thresholds.py 24/25  # Hold-out validation
```

### Step 4: Decide

Based on results:
- **If profit improves >5%**: Deploy new threshold
- **If profit improves 0-5%**: Test more, maybe deploy
- **If profit decreases**: Keep original

## Manual Testing

You can also run simulations manually:

```bash
# Original threshold (0.55)
python smart_arbitrage_sim_versioned.py original

# New threshold (0.50)
python smart_arbitrage_sim_versioned.py 050

# Or specify in code:
from smart_arbitrage_sim_versioned import VersionedArbitrageSimulator

sim = VersionedArbitrageSimulator(db_path, 'original')  # or '050'
result = sim.run_simulation('21/22', 10000, 100)
```

## Adding More Thresholds

Want to test threshold 0.45?

```bash
python regenerate_timing_labels.py 0.45
python compare_thresholds.py 21/22  # Edit to compare 0.55 vs 0.45
```

## Advantages

1. **Non-destructive**: Original data never lost
2. **A/B testing**: Compare multiple versions easily
3. **Rollback**: Can always go back to original
4. **Production safe**: Test thoroughly before deploying

## Files

- `regenerate_timing_labels.py` - Generates new threshold labels
- `smart_arbitrage_sim_versioned.py` - Simulator supporting multiple versions
- `compare_thresholds.py` - Side-by-side comparison tool
- `THRESHOLD_VERSIONING.md` - This file

## Expected Results (Based on Validation)

**Hold-out validation (24/25) suggested**:
- Threshold 0.50 should catch 14-26% more signals
- Expected profit improvement: +5-10%
- Signal coverage: 73-86% → 85-95%

**But**: Must test to confirm!

## Troubleshooting

### "ERROR: Required columns not found"
→ Run `regenerate_timing_labels.py` first

### Model loading fails
→ Check Python version compatibility with pickled models
→ May need to retrain models in current environment

### Takes too long
→ Normal for first run (~5-10 min for 50k rows)
→ Subsequent runs faster (updates only)

## Production Deployment

Once you've validated the new threshold:

1. Update threshold files:
```bash
echo "0.5" > models/winners/threshold_home_timing_strong_precision.txt
echo "0.5" > models/winners/threshold_draw_timing_strong_lower_false_alarms.txt
echo "0.5" > models/winners/threshold_away_timing_strong_precision.txt
```

2. Update production code to use new columns:
```python
# Change from:
timing_col = 'should_bet_home_now'

# To:
timing_col = 'should_bet_home_now_t050'
```

3. Or regenerate entire database with new threshold as default

## Important Notes

- Keep both versions during testing phase
- Only deploy after validating on ALL seasons
- Hold-out (24/25) is the final validation
- If uncertain, stay with original (conservative approach)
