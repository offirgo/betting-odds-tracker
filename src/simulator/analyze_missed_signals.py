#!/usr/bin/env python3
"""
Missed Signals Deep Dive

Analyzes the 11-19% of matches where timing models DON'T fire signals.
These are our biggest opportunity for model improvement.

Questions:
1. What makes these matches different?
2. What features distinguish "signal fires" vs "no signal"?
3. Can we retrain or adjust thresholds to catch these?
