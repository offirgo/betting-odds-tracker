#!/usr/bin/env python3
"""
Live Dashboard Web Server

Flask server that provides:
1. Real-time odds monitoring
2. ML predictions for betting opportunities
3. Performance tracking
4. Web-based dashboard UI

Run: python dashboard_server.py
Then open: http://localhost:5000
"""

from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import json
import os
from datetime import datetime, timezone
import threading
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(dotenv_path='../../.env')

from odds_fetcher import OddsFetcher
from live_predictor_fixed import LivePredictor


app = Flask(__name__,
            template_folder='templates',
            static_folder='static')
CORS(app)

# Global state
current_matches = []
predictions = []
performance_stats = {
    'total_opportunities': 0,
    'total_profit': 0,
    'bets_placed': 0,
    'win_rate': 0,
    'avg_profit_pct': 0
}
bet_history = []
last_update = None

# Initialize components
odds_fetcher = None
predictor = None
auto_fetch_enabled = False
fetch_interval = 300  # 5 minutes default


def initialize():
    """Initialize odds fetcher and predictor."""
    global odds_fetcher, predictor

    api_key = os.getenv('ODDS_API_KEY')
    if not api_key:
        print("WARNING: ODDS_API_KEY not set. Live data fetching disabled.")
        print("Set with: export ODDS_API_KEY='your_key'")

    odds_fetcher = OddsFetcher(api_key) if api_key else None

    try:
        predictor = LivePredictor()
        print("✓ ML models loaded successfully")
    except Exception as e:
        print(f"✗ Error loading ML models: {e}")
        predictor = None


@app.route('/')
def index():
    """Serve the dashboard HTML."""
    return render_template('dashboard.html')


@app.route('/api/status')
def get_status():
    """Get system status."""
    return jsonify({
        'odds_fetcher_ready': odds_fetcher is not None,
        'predictor_ready': predictor is not None,
        'auto_fetch_enabled': auto_fetch_enabled,
        'fetch_interval': fetch_interval,
        'last_update': last_update,
        'api_quota': odds_fetcher.get_remaining_requests() if odds_fetcher else None
    })


@app.route('/api/matches')
def get_matches():
    """Get current matches with odds and predictions."""
    return jsonify({
        'matches': current_matches,
        'predictions': predictions,
        'last_update': last_update
    })


@app.route('/api/fetch_odds', methods=['POST'])
def fetch_odds():
    """Manually trigger odds fetch."""
    global current_matches, predictions, last_update

    if not odds_fetcher:
        return jsonify({'error': 'Odds fetcher not initialized. Set ODDS_API_KEY.'}), 400

    try:
        matches = odds_fetcher.fetch_live_odds()

        if matches:
            current_matches = matches
            last_update = datetime.now(timezone.utc).isoformat()

            # Run predictions if predictor is available
            if predictor:
                predictions = []
                for match in matches:
                    try:
                        analysis = predictor.analyze_match(match)
                        predictions.append(analysis)
                    except Exception as e:
                        print(f"Error analyzing match: {e}")

                # Save tracking state after predictions
                try:
                    predictor.save_tracking_state()
                except Exception as e:
                    print(f"Error saving tracking state: {e}")

            return jsonify({
                'success': True,
                'matches_count': len(matches),
                'predictions_count': len(predictions),
                'last_update': last_update
            })
        else:
            return jsonify({'error': 'Failed to fetch odds'}), 500

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/performance')
def get_performance():
    """Get performance statistics."""
    return jsonify(performance_stats)


@app.route('/api/bet_history')
def get_bet_history():
    """Get betting history."""
    return jsonify({
        'history': bet_history,
        'total': len(bet_history)
    })


@app.route('/api/place_bet', methods=['POST'])
def place_bet():
    """
    Record a bet placement (for tracking).

    This doesn't actually place real bets - just tracks what you would bet.
    """
    global bet_history, performance_stats

    data = request.json

    bet = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'match_id': data.get('match_id'),
        'home_team': data.get('home_team'),
        'away_team': data.get('away_team'),
        'stake': data.get('stake', 100),
        'bet_type': data.get('bet_type', 'arbitrage'),
        'odds': data.get('odds', {}),
        'expected_profit': data.get('expected_profit', 0),
        'status': 'pending'
    }

    bet_history.append(bet)

    # Update stats
    performance_stats['bets_placed'] += 1
    performance_stats['total_opportunities'] += 1

    # Save to file
    save_bet_history()

    return jsonify({
        'success': True,
        'bet_id': len(bet_history) - 1
    })


@app.route('/api/settings', methods=['GET', 'POST'])
def settings():
    """Get/update dashboard settings."""
    global auto_fetch_enabled, fetch_interval

    if request.method == 'POST':
        data = request.json
        auto_fetch_enabled = data.get('auto_fetch_enabled', auto_fetch_enabled)
        fetch_interval = data.get('fetch_interval', fetch_interval)

        return jsonify({
            'success': True,
            'settings': {
                'auto_fetch_enabled': auto_fetch_enabled,
                'fetch_interval': fetch_interval
            }
        })
    else:
        return jsonify({
            'auto_fetch_enabled': auto_fetch_enabled,
            'fetch_interval': fetch_interval
        })


def save_bet_history():
    """Save bet history to JSON file."""
    try:
        with open('../../data/live_bet_history.json', 'w') as f:
            json.dump(bet_history, f, indent=2)
    except Exception as e:
        print(f"Error saving bet history: {e}")


def load_bet_history():
    """Load bet history from JSON file."""
    global bet_history, performance_stats

    try:
        if os.path.exists('../../data/live_bet_history.json'):
            with open('../../data/live_bet_history.json', 'r') as f:
                bet_history = json.load(f)

            # Recalculate stats
            performance_stats['bets_placed'] = len(bet_history)
            performance_stats['total_profit'] = sum(b.get('actual_profit', 0) for b in bet_history)
            wins = sum(1 for b in bet_history if b.get('status') == 'won')
            performance_stats['win_rate'] = (wins / len(bet_history) * 100) if bet_history else 0

    except Exception as e:
        print(f"Error loading bet history: {e}")


def auto_fetch_loop():
    """Background thread that auto-fetches odds at regular intervals."""
    global current_matches, predictions, last_update

    while True:
        if auto_fetch_enabled and odds_fetcher:
            try:
                print(f"Auto-fetching odds... ({datetime.now()})")
                matches = odds_fetcher.fetch_live_odds()

                if matches:
                    current_matches = matches
                    last_update = datetime.now(timezone.utc).isoformat()

                    if predictor:
                        predictions = []
                        for match in matches:
                            try:
                                analysis = predictor.analyze_match(match)
                                predictions.append(analysis)
                            except Exception as e:
                                print(f"Error analyzing match: {e}")

                        # Save tracking state
                        try:
                            predictor.save_tracking_state()
                        except Exception as e:
                            print(f"Error saving tracking state: {e}")

            except Exception as e:
                print(f"Error in auto-fetch: {e}")

        time.sleep(fetch_interval)


if __name__ == '__main__':
    print("=" * 70)
    print("LIVE ARBITRAGE DASHBOARD")
    print("=" * 70)
    print()

    # Initialize
    initialize()
    load_bet_history()

    # Start auto-fetch thread
    fetch_thread = threading.Thread(target=auto_fetch_loop, daemon=True)
    fetch_thread.start()

    print()
    print("✓ Dashboard server starting...")
    print()
    print("Open in browser: http://localhost:5001")
    print()
    print("Press Ctrl+C to stop")
    print("=" * 70)
    print()

    # Run Flask app
    app.run(debug=True, host='0.0.0.0', port=5001, use_reloader=False)
