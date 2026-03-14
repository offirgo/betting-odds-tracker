# Live Arbitrage Dashboard

Real-time betting arbitrage monitoring system with ML-powered timing predictions.

## Features

- 📊 **Real-time Odds**: Fetches live EPL odds from The Odds API
- 🤖 **ML Predictions**: Uses trained models to predict optimal bet timing
- 🎯 **Arbitrage Detection**: Automatically identifies risk-free betting opportunities
- 📈 **Web Dashboard**: Beautiful, real-time web interface
- 💰 **Performance Tracking**: Monitor your betting history and ROI

## Quick Start

### 1. Get API Key

Get a free API key from [The Odds API](https://the-odds-api.com/):
- Sign up for free account
- Get API key (500 requests/month free tier)

### 2. Install Dependencies

```bash
cd src/live
pip install -r requirements.txt
```

### 3. Set API Key

```bash
export ODDS_API_KEY='your_api_key_here'
```

### 4. Run Dashboard

```bash
python dashboard_server.py
```

Then open in browser: **http://localhost:5000**

## How It Works

### The Strategy

1. **Fetch Live Odds**: Dashboard fetches current EPL match odds from multiple bookmakers
2. **Detect Arbitrage**: Checks if combined inverse odds < 1.0 (guaranteed profit exists)
3. **ML Timing Prediction**: Runs 3 timing models (Home/Draw/Away) to predict optimal bet timing
4. **Recommendation**: Suggests when to bet based on arbitrage + timing signals

### ML Models

The system uses 3 Random Forest models trained on historical odds data:

- **Home Timing Model** (threshold: 0.55)
- **Draw Timing Model** (threshold: 0.575)
- **Away Timing Model** (threshold: 0.55)

Each model predicts: "Should we bet on this outcome NOW?"

### Betting Logic

**Standard 3-Way Arbitrage:**

1. Identify match with arbitrage opportunity
2. Bet on 2 highest odds outcomes immediately
3. **Wait for ML timing signal** for 3rd (lowest odds) outcome
4. When signal fires → complete the arbitrage
5. Lock in guaranteed profit

**Example:**
```
Match: Arsenal vs Chelsea
Odds: H 2.10 | D 3.40 | A 3.80
Arbitrage: Yes (2.4% profit)

Action:
✓ Bet Arsenal (2.10) NOW
✓ Bet Draw (3.40) NOW
⏳ Wait for Away signal...
   ML Prediction: 0.48 (threshold: 0.55)
   → WAIT
```

## Dashboard Features

### Live Opportunities

- See all upcoming EPL matches
- Real-time odds from best bookmakers
- Arbitrage detection with profit %
- ML timing signals for each outcome
- Clear BET NOW / PREPARE / WAIT recommendations

### Statistics

- Active arbitrage opportunities
- Average profit percentage
- API quota remaining
- Bets placed (tracked)

### Predictions

Each match shows:
- Current best odds (Home/Draw/Away)
- ML model probability for each outcome
- Whether timing signal has fired
- Recommended action

## API Limits

**Free Tier (The Odds API):**
- 500 requests/month
- ~16 requests/day
- Fetch odds 3-4 times per day maximum

**Tips:**
- Don't enable auto-fetch on short intervals
- Manually refresh when needed
- Consider paid tier for serious use

## File Structure

```
src/live/
├── odds_fetcher.py       # Fetches odds from The Odds API
├── live_predictor.py     # Runs ML models on live data
├── dashboard_server.py   # Flask web server
├── templates/
│   └── dashboard.html    # Web UI
├── requirements.txt      # Dependencies
└── README.md            # This file
```

## Performance Expectations

Based on hold-out validation (Season 24/25):

**Fixed Betting (£100/bet):**
- Profit: £584.62 per season
- ROI: 5.85%
- Avg profit/bet: £3.65
- Coverage: 160 matches/season (41.5%)

**Kelly Criterion (Recommended):**
- Profit: £956.86 per season
- ROI: 9.57%
- Avg profit/bet: £5.98
- +64% improvement over fixed

**With 2% Commission:**
- Profit: £315.39 per season
- ROI: 3.15%
- Avg profit/bet: £3.09
- ~46% reduction due to costs

## Important Notes

⚠️ **This is for educational/testing purposes**

- Dashboard tracks bets but doesn't place real bets
- You must place bets manually on betting sites
- Always verify odds before betting
- Consider transaction costs (2-5% commission typical)
- Past performance doesn't guarantee future results

## Troubleshooting

**"ODDS_API_KEY not set"**
```bash
export ODDS_API_KEY='your_key_here'
```

**"Failed to load ML models"**
- Check models exist in `../../models/winners/`
- Models must be trained first (run training scripts)

**"No matches loaded"**
- Click "Refresh Odds" button
- Check API quota (must have requests remaining)
- EPL season must be active (Aug-May typically)

**"API quota exceeded"**
- Wait until next month (free tier resets monthly)
- Consider paid tier
- Reduce fetch frequency

## Next Steps

1. **Test with Paper Trading**: Track predictions without real money
2. **Monitor Performance**: Record actual vs predicted outcomes
3. **Optimize**: Test different thresholds, Kelly fractions
4. **Scale**: Consider paid API tier for more frequent updates

## Support

For issues or questions:
- Check OPTIMIZATION_RESULTS.md for model performance
- See THRESHOLD_VERSIONING.md for threshold testing
- Review simulator code in src/simulator/ for logic

---

Built with:
- Flask (web framework)
- The Odds API (odds data)
- Scikit-learn (ML models)
- Pandas/NumPy (data processing)
