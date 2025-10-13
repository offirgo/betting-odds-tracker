#!/usr/bin/env python3
"""
Step 5: Train Model 3 - Home Betting Timing
This model predicts: "Should I bet on Home team NOW?"
Answer: True/False (Yes/No)

This tells you the OPTIMAL moment to place your Home bet to get best odds.
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix, roc_auc_score
import joblib
import os


def load_prepared_data(data_dir='../../data/prepared'):
    """Load the CSV files for home betting timing"""

    print("=" * 60)
    print("STEP 5: Train Home Betting Timing Model")
    print("=" * 60)

    print("\n📂 Loading prepared data...")

    # Load training data
    X_train = pd.read_csv(f'{data_dir}/features_train.csv')
    y_train = pd.read_csv(f'{data_dir}/target_train_bet_home_timing.csv')['target']

    # Load test data
    X_test = pd.read_csv(f'{data_dir}/features_test.csv')
    y_test = pd.read_csv(f'{data_dir}/target_test_bet_home_timing.csv')['target']

    print(f"✓ Training data: {len(X_train)} rows, {X_train.shape[1]} features")
    print(f"✓ Test data: {len(X_test)} rows, {X_test.shape[1]} features")

    return X_train, X_test, y_train, y_test


def check_data_balance(y_train, y_test):
    """Check class balance for betting timing"""

    print("\n" + "=" * 60)
    print("DATA BALANCE CHECK")
    print("=" * 60)

    train_counts = y_train.value_counts()
    test_counts = y_test.value_counts()

    train_yes_pct = 100 * train_counts.get(1, 0) / len(y_train)
    test_yes_pct = 100 * test_counts.get(1, 0) / len(y_test)

    print(f"\n📊 Training Set:")
    print(f"   Bet Home NOW (1):     {train_counts.get(1, 0):,} ({train_yes_pct:.1f}%)")
    print(f"   Don't bet yet (0):    {train_counts.get(0, 0):,} ({100 - train_yes_pct:.1f}%)")

    print(f"\n📊 Test Set:")
    print(f"   Bet Home NOW (1):     {test_counts.get(1, 0):,} ({test_yes_pct:.1f}%)")
    print(f"   Don't bet yet (0):    {test_counts.get(0, 0):,} ({100 - test_yes_pct:.1f}%)")

    print(f"\n💡 Class Balance Assessment:")
    if 40 <= train_yes_pct <= 60:
        print(f"   ✅ EXCELLENT balance ({train_yes_pct:.1f}% positive)")
        is_imbalanced = False
    elif 25 <= train_yes_pct <= 75:
        print(f"   ✓ GOOD balance ({train_yes_pct:.1f}% positive)")
        print("   Will use class_weight='balanced' to help")
        is_imbalanced = True
    else:
        print(f"   ⚠️  IMBALANCED ({train_yes_pct:.1f}% positive)")
        print("   Will use class_weight='balanced'")
        is_imbalanced = True

    return is_imbalanced


def train_regularized_models(X_train, y_train):
    """
    Train multiple regularization strategies
    Using lessons learned from Model 2 (Strong regularization worked best)
    """

    print("\n" + "=" * 60)
    print("TRAINING REGULARIZED MODELS")
    print("=" * 60)

    print("\n💡 Based on Model 2 results, we know:")
    print("   - Strong regularization (max_depth=8) worked best")
    print("   - Prevented overfitting while maintaining good capture rate")
    print("   - Will test similar strategies here")

    models = {}

    # Strategy 1: Strong regularization (what worked for Model 2)
    print("\n1️⃣  STRONG Regularization (recommended):")
    models['strong'] = RandomForestClassifier(
        n_estimators=200,
        max_depth=8,
        min_samples_split=40,
        min_samples_leaf=20,
        max_features='sqrt',
        random_state=42,
        class_weight='balanced',
        n_jobs=-1
    )
    models['strong'].fit(X_train, y_train)
    print("   ✓ Trained")

    # Strategy 2: Moderate regularization
    print("\n2️⃣  MODERATE Regularization:")
    models['moderate'] = RandomForestClassifier(
        n_estimators=150,
        max_depth=12,
        min_samples_split=25,
        min_samples_leaf=10,
        max_features='sqrt',
        random_state=42,
        class_weight='balanced',
        n_jobs=-1
    )
    models['moderate'].fit(X_train, y_train)
    print("   ✓ Trained")

    # Strategy 3: Light regularization
    print("\n3️⃣  LIGHT Regularization:")
    models['light'] = RandomForestClassifier(
        n_estimators=120,
        max_depth=15,
        min_samples_split=20,
        min_samples_leaf=8,
        max_features='sqrt',
        random_state=42,
        class_weight='balanced',
        n_jobs=-1
    )
    models['light'].fit(X_train, y_train)
    print("   ✓ Trained")

    return models


def evaluate_model(model, X_train, X_test, y_train, y_test, model_name="Model"):
    """Evaluate a single model"""

    # Training performance
    train_pred = model.predict(X_train)
    train_proba = model.predict_proba(X_train)[:, 1]
    train_acc = accuracy_score(y_train, train_pred)
    train_auc = roc_auc_score(y_train, train_proba)

    # Test performance
    test_pred = model.predict(X_test)
    test_proba = model.predict_proba(X_test)[:, 1]
    test_acc = accuracy_score(y_test, test_pred)
    test_auc = roc_auc_score(y_test, test_proba)

    # Confusion matrix
    cm = confusion_matrix(y_test, test_pred)

    # Calculate metrics
    if len(cm) > 1:
        # True positives: correctly identified optimal betting times
        true_positives = cm[1, 1]
        # False negatives: missed optimal betting times
        false_negatives = cm[1, 0]
        # Capture rate: % of optimal times we caught
        capture_rate = true_positives / (true_positives + false_negatives) if (
                                                                                          true_positives + false_negatives) > 0 else 0

        # False positives: bet too early/late when we shouldn't
        false_positives = cm[0, 1]
        # Precision: when we say "bet now", how often are we right?
        precision = true_positives / (true_positives + false_positives) if (true_positives + false_positives) > 0 else 0
    else:
        capture_rate = 0
        precision = 0

    return {
        'name': model_name,
        'train_acc': train_acc,
        'test_acc': test_acc,
        'train_auc': train_auc,
        'test_auc': test_auc,
        'overfitting_gap': train_acc - test_acc,
        'capture_rate': capture_rate,
        'precision': precision,
        'confusion_matrix': cm
    }


def compare_all_models(X_train, X_test, y_train, y_test):
    """Train and compare all regularization strategies"""

    print("\n" + "=" * 60)
    print("COMPARING ALL MODELS")
    print("=" * 60)

    models = train_regularized_models(X_train, y_train)

    print("\n" + "=" * 60)
    print("RESULTS COMPARISON")
    print("=" * 60)

    results = []
    for name, model in models.items():
        result = evaluate_model(model, X_train, X_test, y_train, y_test, name)
        results.append(result)

    # Display comparison table
    print("\n📊 Performance Comparison:")
    print("\n" + "=" * 90)
    print(
        f"{'Model':<15} {'Train Acc':<11} {'Test Acc':<11} {'Overfit Gap':<13} {'Test AUC':<10} {'Capture':<10} {'Precision':<10}")
    print("=" * 90)

    for r in results:
        print(
            f"{r['name']:<15} {r['train_acc'] * 100:>9.2f}%  {r['test_acc'] * 100:>9.2f}%  {r['overfitting_gap'] * 100:>11.2f}%  {r['test_auc']:>8.3f}  {r['capture_rate'] * 100:>8.1f}%  {r['precision'] * 100:>8.1f}%")

    print("=" * 90)

    # Analysis
    print("\n" + "=" * 60)
    print("ANALYSIS")
    print("=" * 60)

    best_overfit = min(results, key=lambda x: x['overfitting_gap'])
    print(f"\n✅ Least Overfitting: {best_overfit['name']} (gap: {best_overfit['overfitting_gap'] * 100:.1f}%)")

    best_acc = max(results, key=lambda x: x['test_acc'])
    print(f"✅ Best Test Accuracy: {best_acc['name']} ({best_acc['test_acc'] * 100:.1f}%)")

    best_capture = max(results, key=lambda x: x['capture_rate'])
    print(f"✅ Best Capture Rate: {best_capture['name']} ({best_capture['capture_rate'] * 100:.1f}%)")

    best_auc = max(results, key=lambda x: x['test_auc'])
    print(f"✅ Best AUC: {best_auc['name']} ({best_auc['test_auc']:.3f})")

    # Recommendation
    print("\n" + "=" * 60)
    print("RECOMMENDATION")
    print("=" * 60)

    # Weighted scoring: capture rate is most important for timing
    for r in results:
        overfit_score = max(0, 1 - r['overfitting_gap'] / 0.4)
        capture_score = r['capture_rate']
        precision_score = r['precision']

        # For timing: 50% capture, 30% precision, 20% overfit control
        r['combined_score'] = (0.5 * capture_score + 0.3 * precision_score + 0.2 * overfit_score)

    best_overall = max(results, key=lambda x: x['combined_score'])

    print(f"\n🏆 RECOMMENDED MODEL: {best_overall['name'].upper()}")
    print(f"\n   Why this model:")
    print(f"   - Test Accuracy: {best_overall['test_acc'] * 100:.1f}%")
    print(f"   - Overfitting Gap: {best_overall['overfitting_gap'] * 100:.1f}%")
    print(f"   - Captures {best_overall['capture_rate'] * 100:.1f}% of optimal betting times")
    print(f"   - Precision: {best_overall['precision'] * 100:.1f}% (when it says 'bet now', it's right this often)")
    print(f"   - AUC: {best_overall['test_auc']:.3f}")

    print(f"\n💡 What this means in practice:")
    cm = best_overall['confusion_matrix']
    if len(cm) > 1:
        optimal_times = cm[1, 0] + cm[1, 1]
        caught = cm[1, 1]
        missed = cm[1, 0]
        false_alarms = cm[0, 1]

        print(f"   - Out of {optimal_times:,} optimal betting moments:")
        print(f"     ✓ Will catch {caught:,} ({best_overall['capture_rate'] * 100:.1f}%)")
        print(f"     ✗ Will miss {missed:,} ({100 - best_overall['capture_rate'] * 100:.1f}%)")
        print(f"   - False alarms: {false_alarms:,} times (bet at wrong time)")

    # Show confusion matrix
    print(f"\n📊 Confusion Matrix (Recommended Model):")
    print(f"                    Predicted Wait    Predicted Bet Now")
    print(f"  Actually Wait:        {cm[0, 0]:6d}         {cm[0, 1]:6d}")
    print(f"  Actually Bet Now:     {cm[1, 0]:6d}         {cm[1, 1]:6d}")

    return models[best_overall['name']], best_overall['name'], results


def show_feature_importance(model, feature_names):
    """Show which features matter most for timing decisions"""

    print("\n" + "=" * 60)
    print("FEATURE IMPORTANCE")
    print("=" * 60)

    print("\n🔍 Which features matter most for timing decisions?")

    importances = model.feature_importances_
    feature_importance = pd.DataFrame({
        'feature': feature_names,
        'importance': importances
    }).sort_values('importance', ascending=False)

    print("\n📊 Top 10 Most Important Features:")
    for i, row in feature_importance.head(10).iterrows():
        print(f"   {row['feature']:35s} {row['importance']:.4f}")

    print("\n💡 What this means:")
    print("   These features tell the model WHEN to bet on Home team")
    print("   Higher scores = model relies on these patterns more")


def save_model(model, model_name, output_dir='../../models'):
    """Save the trained model"""

    print("\n" + "=" * 60)
    print("SAVING MODEL")
    print("=" * 60)

    os.makedirs(output_dir, exist_ok=True)
    model_path = f'{output_dir}/model_home_timing_{model_name}.pkl'

    joblib.dump(model, model_path)

    print(f"\n✓ Model saved to: {model_path}")

    return model_path


def main():
    """Main execution"""

    # Step 1: Load data
    X_train, X_test, y_train, y_test = load_prepared_data()

    # Step 2: Check balance
    is_imbalanced = check_data_balance(y_train, y_test)

    # Step 3: Train and compare all models
    best_model, best_name, all_results = compare_all_models(X_train, X_test, y_train, y_test)

    # Step 4: Show feature importance
    show_feature_importance(best_model, X_train.columns)

    # Step 5: Save best model
    model_path = save_model(best_model, best_name)

    print("\n" + "=" * 60)
    print("STEP 5 COMPLETE!")
    print("=" * 60)

    best_result = [r for r in all_results if r['name'] == best_name][0]

    print(f"\n✓ Model 3: Home Betting Timing trained!")
    print(f"   - Best Strategy: {best_name.upper()}")
    print(f"   - Test Accuracy: {best_result['test_acc'] * 100:.1f}%")
    print(f"   - Captures {best_result['capture_rate'] * 100:.1f}% of optimal betting times")
    print(f"   - Precision: {best_result['precision'] * 100:.1f}%")
    print(f"✓ Model saved to: {model_path}")

    print("\n💡 Next steps:")
    print("   1. Train Model 4: Draw Betting Timing")
    print("   2. Train Model 5: Away Betting Timing")
    print("   3. Combine all models for complete arbitrage system!")

    print("\n✨ You now know WHEN to bet on Home team!")


if __name__ == "__main__":
    main()