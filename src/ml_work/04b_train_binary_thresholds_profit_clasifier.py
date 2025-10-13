#!/usr/bin/env python3
"""
Step 4: Train Model 2 - High Profit Classification
This model predicts: "Will this match have profit > X%?"
Answer: True/False (Yes/No)
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix, roc_auc_score, \
    precision_recall_curve
import joblib
import os
import argparse


def load_prepared_data(threshold, data_dir='../../data/prepared'):
    """
    Load the CSV files for a specific profit threshold

    Args:
        threshold: Profit percentage threshold (e.g., 3.0, 3.5, 4.0, 5.0, 7.0)
    """

    print("=" * 60)
    print(f"STEP 4: Train High Profit Classifier (>{threshold}%)")
    print("=" * 60)

    print(f"\n📂 Loading prepared data for {threshold}% threshold...")

    # Handle decimal thresholds in filename
    threshold_label = f"{threshold:.1f}".replace('.', '_')

    # Load training data
    X_train = pd.read_csv(f'{data_dir}/features_train.csv')
    y_train = pd.read_csv(f'{data_dir}/target_train_profit_gt{threshold_label}pct.csv')['target']

    # Load test data
    X_test = pd.read_csv(f'{data_dir}/features_test.csv')
    y_test = pd.read_csv(f'{data_dir}/target_test_profit_gt{threshold_label}pct.csv')['target']

    print(f"✓ Training data: {len(X_train)} rows, {X_train.shape[1]} features")
    print(f"✓ Test data: {len(X_test)} rows, {X_test.shape[1]} features")

    return X_train, X_test, y_train, y_test


def check_data_balance(y_train, y_test, threshold):
    """
    Check if our data is balanced
    """

    print("\n" + "=" * 60)
    print("DATA BALANCE CHECK")
    print("=" * 60)

    train_counts = y_train.value_counts()
    test_counts = y_test.value_counts()

    print(f"\n📊 Training Set:")
    print(
        f"   Profit > {threshold}% (1): {train_counts.get(1, 0):,} ({100 * train_counts.get(1, 0) / len(y_train):.1f}%)")
    print(
        f"   Profit ≤ {threshold}% (0): {train_counts.get(0, 0):,} ({100 * train_counts.get(0, 0) / len(y_train):.1f}%)")

    print(f"\n📊 Test Set:")
    print(f"   Profit > {threshold}% (1): {test_counts.get(1, 0):,} ({100 * test_counts.get(1, 0) / len(y_test):.1f}%)")
    print(f"   Profit ≤ {threshold}% (0): {test_counts.get(0, 0):,} ({100 * test_counts.get(0, 0) / len(y_test):.1f}%)")

    # Evaluate balance
    train_positive_pct = 100 * train_counts.get(1, 0) / len(y_train)

    print(f"\n💡 Class Balance Assessment:")
    if 40 <= train_positive_pct <= 60:
        print(f"   ✅ EXCELLENT balance ({train_positive_pct:.1f}% positive)")
        print("   Model should learn both classes well")
        is_imbalanced = False
    elif 30 <= train_positive_pct <= 70:
        print(f"   ✓ GOOD balance ({train_positive_pct:.1f}% positive)")
        print("   Will use class_weight='balanced' to help")
        is_imbalanced = True
    elif 20 <= train_positive_pct <= 80:
        print(f"   ⚠️  MODERATE imbalance ({train_positive_pct:.1f}% positive)")
        print("   Will use class_weight='balanced' - model might be biased")
        is_imbalanced = True
    else:
        print(f"   ❌ SEVERE imbalance ({train_positive_pct:.1f}% positive)")
        print("   Model will struggle with minority class")
        is_imbalanced = True

    return is_imbalanced


def train_model(X_train, y_train, is_imbalanced=False):
    """
    Train a Random Forest Classifier
    """

    print("\n" + "=" * 60)
    print("TRAINING THE MODEL")
    print("=" * 60)

    print("\n🌲 Creating Random Forest Classifier...")
    print("   Parameters:")
    print("   - n_estimators=100 (we'll build 100 decision trees)")
    print("   - max_depth=20 (each tree can be up to 20 levels deep)")
    print("   - min_samples_split=10 (need at least 10 examples to split a node)")
    print("   - random_state=42 (for reproducible results)")
    if is_imbalanced:
        print("   - class_weight='balanced' (to handle imbalanced data)")

    # Create the model
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=20,
        min_samples_split=10,
        random_state=42,
        class_weight='balanced' if is_imbalanced else None,
        n_jobs=-1
    )

    print("\n🎓 Training the model...")
    print("   This might take 30-60 seconds...")

    model.fit(X_train, y_train)

    print("✓ Training complete!")

    return model


def evaluate_model(model, X_train, X_test, y_train, y_test, threshold):
    """
    Test how well the model works
    """

    print("\n" + "=" * 60)
    print("MODEL EVALUATION")
    print("=" * 60)

    # Make predictions on training data
    print("\n📈 Testing on training data...")
    train_predictions = model.predict(X_train)
    train_proba = model.predict_proba(X_train)[:, 1]
    train_accuracy = accuracy_score(y_train, train_predictions)
    print(f"   Training Accuracy: {train_accuracy * 100:.2f}%")

    # Make predictions on test data
    print("\n📈 Testing on test data (new, unseen data)...")
    test_predictions = model.predict(X_test)
    test_proba = model.predict_proba(X_test)[:, 1]
    test_accuracy = accuracy_score(y_test, test_predictions)
    print(f"   Test Accuracy: {test_accuracy * 100:.2f}%")

    # ROC AUC Score
    try:
        train_auc = roc_auc_score(y_train, train_proba)
        test_auc = roc_auc_score(y_test, test_proba)
        print(f"\n📊 ROC AUC Scores:")
        print(f"   Training AUC: {train_auc:.3f}")
        print(f"   Test AUC:     {test_auc:.3f}")

        if test_auc > 0.85:
            print(f"   🎉 Excellent discrimination ability!")
        elif test_auc > 0.75:
            print(f"   ✓ Good discrimination ability")
        elif test_auc > 0.65:
            print(f"   ⚠️  Moderate discrimination ability")
        else:
            print(f"   ❌ Poor discrimination ability")
    except:
        test_auc = None
        print("\n⚠️  Could not calculate AUC (might be single class in data)")

    # Interpret the results
    print("\n💡 What does this mean?")
    if test_accuracy > 0.85:
        print("   🎉 Excellent! The model is very accurate.")
    elif test_accuracy > 0.75:
        print("   ✓ Good accuracy. The model is working well.")
    elif test_accuracy > 0.65:
        print("   ⚠️  Moderate accuracy. May need improvement.")
    else:
        print("   ❌ Low accuracy. Model needs work.")

    # Check for overfitting
    accuracy_gap = train_accuracy - test_accuracy
    if accuracy_gap > 0.1:
        print(f"\n⚠️  Warning: Training accuracy is {accuracy_gap * 100:.1f}% higher than test accuracy")
        print("   This suggests overfitting (model memorized training data)")
    else:
        print(f"\n✓ No significant overfitting detected (gap: {accuracy_gap * 100:.1f}%)")

    # Detailed breakdown
    print("\n" + "=" * 60)
    print("DETAILED METRICS")
    print("=" * 60)

    print("\n📊 Classification Report (Test Set):")
    print("\nWhat these metrics mean:")
    print("  - Precision: When model says 'Yes high profit', how often is it right?")
    print("  - Recall: Of all actual high-profit cases, how many did we catch?")
    print("  - F1-score: Balance between precision and recall (higher is better)")
    print()

    report = classification_report(y_test, test_predictions,
                                   target_names=[f'Profit ≤ {threshold}%', f'Profit > {threshold}%'])
    print(report)

    # Confusion Matrix
    print("\n📊 Confusion Matrix:")
    print("\nWhat this shows:")
    print("  Rows = Actual values, Columns = Predicted values")
    print()
    cm = confusion_matrix(y_test, test_predictions)
    print(f"                    Predicted Low    Predicted High")
    print(f"  Actually Low:        {cm[0, 0]:6d}         {cm[0, 1]:6d}")
    print(f"  Actually High:       {cm[1, 0]:6d}         {cm[1, 1]:6d}")

    # Explain the confusion matrix
    true_negatives = cm[0, 0]
    false_positives = cm[0, 1]
    false_negatives = cm[1, 0]
    true_positives = cm[1, 1]

    print("\n💡 In plain English:")
    print(f"   ✓ Correctly identified {true_positives:,} high-profit opportunities")
    print(f"   ✓ Correctly identified {true_negatives:,} low-profit cases")
    print(f"   ✗ Missed {false_negatives:,} high-profit opportunities (false negatives)")
    print(f"   ✗ False alarms: {false_positives:,} times (predicted high profit when it wasn't)")

    # Calculate business metrics
    if true_positives + false_negatives > 0:
        recall = true_positives / (true_positives + false_negatives)
        print(f"\n📊 Business Impact:")
        print(f"   Capture Rate: {recall * 100:.1f}% of high-profit opportunities found")
        if recall > 0.8:
            print(f"   🎉 Excellent! You'll catch most valuable opportunities")
        elif recall > 0.6:
            print(f"   ✓ Good capture rate")
        else:
            print(f"   ⚠️  You're missing many high-profit opportunities")

    return test_accuracy, test_auc, test_predictions


def show_feature_importance(model, feature_names):
    """
    Show which features the model finds most important
    """

    print("\n" + "=" * 60)
    print("FEATURE IMPORTANCE")
    print("=" * 60)

    print("\n🔍 Which features matter most for predictions?")

    importances = model.feature_importances_
    feature_importance = pd.DataFrame({
        'feature': feature_names,
        'importance': importances
    }).sort_values('importance', ascending=False)

    print("\n📊 Top 10 Most Important Features:")
    for i, row in feature_importance.head(10).iterrows():
        print(f"   {row['feature']:35s} {row['importance']:.4f}")

    print("\n💡 What this means:")
    print("   Higher scores = model relies on these features more")
    print("   These are the patterns the model learned to identify high-profit matches")


def save_model(model, threshold, output_dir='../../models'):
    """
    Save the trained model
    """

    print("\n" + "=" * 60)
    print("SAVING MODEL")
    print("=" * 60)

    os.makedirs(output_dir, exist_ok=True)
    threshold_label = f"{threshold:.1f}".replace('.', '_')
    model_path = f'{output_dir}/model_high_profit_gt{threshold_label}pct.pkl'

    joblib.dump(model, model_path)

    print(f"\n✓ Model saved to: {model_path}")
    print(f"   You can load it later with: model = joblib.load('{model_path}')")

    return model_path


def main():
    """Main execution"""

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Train high profit classifier')
    parser.add_argument('--threshold', type=float, default=5.0,
                        help='Profit threshold percentage (e.g., 3.0, 3.5, 4.0, 5.0, 7.0)')
    args = parser.parse_args()

    threshold = args.threshold

    # Step 1: Load data
    X_train, X_test, y_train, y_test = load_prepared_data(threshold)

    # Step 2: Check balance
    is_imbalanced = check_data_balance(y_train, y_test, threshold)

    # Step 3: Train the model
    model = train_model(X_train, y_train, is_imbalanced)

    # Step 4: Evaluate performance
    test_accuracy, test_auc, predictions = evaluate_model(
        model, X_train, X_test, y_train, y_test, threshold
    )

    # Step 5: Show feature importance
    show_feature_importance(model, X_train.columns)

    # Step 6: Save the model
    model_path = save_model(model, threshold)

    print("\n" + "=" * 60)
    print("STEP 4 COMPLETE!")
    print("=" * 60)
    print(f"\n✓ Model trained for >{threshold}% profit threshold")
    print(f"   - Test Accuracy: {test_accuracy * 100:.1f}%")
    if test_auc:
        print(f"   - Test AUC:      {test_auc:.3f}")
    print(f"✓ Model saved to: {model_path}")
    print("\n💡 Next steps:")
    print("   1. Train models for other thresholds to compare")
    print("   2. Move on to Models 3-5: Betting Timing")
    print(f"\n✨ You now have a classifier that identifies matches with >{threshold}% profit!")


if __name__ == "__main__":
    main()