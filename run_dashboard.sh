#!/bin/bash
# Quick start script for the live dashboard

echo "🎯 Live Arbitrage Dashboard - Quick Start"
echo "=========================================="
echo ""

# Activate venv
echo "Activating virtual environment..."
source .venv/bin/activate

# Check if requirements are installed
if ! python -c "import flask" 2>/dev/null; then
    echo ""
    echo "Installing dependencies..."
    cd src/live
    pip install -r requirements.txt
    cd ../..
fi

# Check for .env file
if [ ! -f .env ]; then
    echo ""
    echo "⚠️  WARNING: .env file not found!"
    echo "Create one with:"
    echo "  echo 'ODDS_API_KEY=your_key_here' > .env"
    echo ""
    exit 1
fi

# Run dashboard
echo ""
echo "✓ Starting dashboard server..."
echo "✓ Open in browser: http://localhost:5000"
echo ""
echo "Press Ctrl+C to stop"
echo "=========================================="
echo ""

cd src/live
python dashboard_server.py
