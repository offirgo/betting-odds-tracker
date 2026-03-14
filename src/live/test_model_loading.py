#!/usr/bin/env python3
"""Test different methods to load the pre-trained models."""

import pickle
import sys
import os

model_path = '../../models/winners/model_home_timing_strong_precision.pkl'

print("="*70)
print("TESTING MODEL LOADING METHODS")
print("="*70)
print(f"\nPython version: {sys.version}")
print(f"Model path: {model_path}")
print()

# Method 1: Standard pickle
print("Method 1: Standard pickle.load()...")
try:
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    print(f"✓ SUCCESS! Model type: {type(model)}")
    print(f"  Model: {model}")
except Exception as e:
    print(f"✗ FAILED: {e}")
    print()

# Method 2: Pickle with encoding
print("Method 2: pickle.load() with encoding='latin1'...")
try:
    with open(model_path, 'rb') as f:
        model = pickle.load(f, encoding='latin1')
    print(f"✓ SUCCESS! Model type: {type(model)}")
    print(f"  Model: {model}")
except Exception as e:
    print(f"✗ FAILED: {e}")
    print()

# Method 3: Pickle with different protocol
print("Method 3: pickle.load() with fix_imports=True...")
try:
    with open(model_path, 'rb') as f:
        model = pickle.load(f, fix_imports=True, encoding='ASCII')
    print(f"✓ SUCCESS! Model type: {type(model)}")
    print(f"  Model: {model}")
except Exception as e:
    print(f"✗ FAILED: {e}")
    print()

# Method 4: Try joblib
print("Method 4: Try joblib.load()...")
try:
    import joblib
    model = joblib.load(model_path)
    print(f"✓ SUCCESS! Model type: {type(model)}")
    print(f"  Model: {model}")
except Exception as e:
    print(f"✗ FAILED: {e}")
    print()

# Method 5: Check pickle protocol
print("Method 5: Checking pickle protocol...")
try:
    with open(model_path, 'rb') as f:
        # Read first few bytes to check protocol
        data = f.read(20)
        protocol = data[1] if len(data) > 1 else None
        print(f"  Pickle protocol byte: {protocol}")
        print(f"  First 20 bytes: {data}")
except Exception as e:
    print(f"✗ FAILED: {e}")

print()
print("="*70)
