#!/usr/bin/env python3
"""
Step 7: Train Model 5 - Away Betting Timing (Precision-Focused)
This model predicts: "Should I bet on Away team NOW?"
Answer: True/False (Yes/No)

This tells you the OPTIMAL moment to place your Away bet to get best odds.

OPTIMIZED FOR PRECISION: We prioritize that when model says "bet now", it's correct,
even if that means missing many opportunities.
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix, roc_auc_score, precision_score, recall_score
from sklearn.model_selection import train_test_split
import joblib
import os


def load_prepared_data(data_dir='../../data/prepared'):
    """Load the CSV files for away betting timing"""

    print("=" * 60)
    print("STEP 7: Train Away Betting Timing Model (PRECISION-FOCUSED)")
    print("=" * 60)

    print("\n📂 Loading prepared data...")

    # Load training data
    X_train = pd.read_csv(f'{data_dir}/features_train.csv')
    y_train = pd.read_csv(f'{data_dir}/target_train_bet_away_timing.csv')['target']

    # Load test data
    X_test = pd.read_csv(f'{data_dir}/features_test.csv')
    y_test = pd.read_csv(f'{data_dir}/target_test_bet_away_timing.csv')['target']

    # Split training data to have a validation set for threshold calibration
    X_train, X_val, y_train, y_val = train_test_split(
        X_train, y_train, test_size=0.2, random_state=42, stratify=y_train
    )

    print(f"✓ Training data: {len(X_train)} rows, {X_train.shape[1]} features")
    print(f"✓ Validation data: {len(X_val)} rows, {X_val.shape[1]} features")
    print(f"✓ Test data: {len(X_test)} rows, {X_test.shape[1]} features")

    return X_train, X_val, X_test, y_train, y_val