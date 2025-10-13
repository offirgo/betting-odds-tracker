#!/usr/bin/env python3
"""
Step 4b: Train IMPROVED High Profit Classifier
Reduces overfitting through regularization and model tuning
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix, roc_auc_score
import joblib
import os


def load_prepared_data(threshold=3.0, data_dir='../../data/prepared'):
    """Load data for specified threshold"""

    print("=" * 60)
    print(f"IMPROVED MODEL: High Profit Classifier (>{threshold}%)")
    print("=" * 60)

    print(f"\n📂 Loading prepared data for {threshold}% threshold...")

    threshold_label = f"{threshold:.1f}".replace('.', '_')

    X_train = pd.read_csv(f'{data_dir}/features_train.csv')
    y_train = pd.read_csv(f'{data_dir}/target_train_profit_gt{threshold_label}pct.csv')['target']
    X_test = pd.read_csv(f'{data_dir}/features_test.csv')
    y_test = pd.read_csv(f'{data_dir}/target_test_profit_gt{threshold_label}pct.csv')['target']

    print(f"✓ Training data: {len(X_train)} rows, {X_train.shape[1]} features")
    print(f"✓ Test data: {len(X_test)} rows, {X_test.shape[1]} features")

    return X_train, X_test, y_train, y_test


def check_data_balance(y_train, y_test, threshold):
    """Check class balance"""

    print("\n" + "=" * 60)
    print("DATA BALANCE CHECK")
    print("=" * 60)

    train_counts = y_train.value_counts()
    test_counts = y_test.value_counts()

    train_positive_pct = 100 * train_counts.get(1, 0) / len(y_train)

    print(f"\n📊 Training Set:")
    print(f"   Profit > {threshold}% (1): {train_counts.get(1, 0):,} ({train_positive_pct:.1f}%)")
    print(f"   Profit ≤ {threshold}% (0): {train_counts.get(0, 0):,} ({100 - train_positive_pct:.1f}%)")

    test_positive_pct = 100 * test_counts.get(1, 0) / len(y_test)
    print(f"\n📊 Test Set:")
    print(f"   Profit > {threshold}% (1): {test_counts.get(1, 0):,} ({test_positive_pct:.1f}%)")
    print(f"   Profit ≤ {threshold}% (0): {test_counts.get(0, 0):,} ({100 - test_positive_pct:.1f}%)")


def train_regularized_model(X_train, y_train):
    """
    Train with REDUCED overfitting through regularization

    Key changes from original:
    1. max_depth: 20 → 10 (shallower trees)
    2. min_samples_split: 10 → 30 (more conservative splits)
    3. min_samples_leaf: ADDED = 15 (leaves need more examples)
    4. max_features: 'sqrt' (only consider subset of features per split)
    5. n_estimators: 100 → 150 (more trees to compensate for simplicity)
    """

    print("\n" + "=" * 60)
    print("TRAINING REGULARIZED MODEL")
    print("=" * 60)

    print("\n🌲 Creating Random Forest with Anti-Overfitting Parameters...")
    print("\n   ORIGINAL MODEL:")
    print("   - n_estimators=100")
    print("   - max_depth=20")
    print("   - min_samples_split=10")
    print("   - min_samples_leaf=None")
    print("   - max_features=None (all features)")

    print("\n   NEW REGULARIZED MODEL:")
    print("   - n_estimators=150 ⬆️  (more trees for stability)")
    print("   - max_depth=10 ⬇️  (shallower = simpler = less memorization)")
    print("   - min_samples_split=30 ⬆️  (need more examples to split)")
    print("   - min_samples_leaf=15 ✨ (each leaf needs 15+ examples)")
    print("   - max_features='sqrt' ✨ (only √27 ≈ 5 features per split)")
    print("   - class_weight='balanced' (handle class imbalance)")

    model = RandomForestClassifier(
        n_estimators=150,  # More trees for ensemble stability
        max_depth=10,  # Shallower trees (less overfitting)
        min_samples_split=30,  # Need more samples to split
        min_samples_leaf=15,  # Leaves must have 15+ samples
        max_features='sqrt',  # Only consider sqrt(n_features) per split
        random_state=42,
        class_weight='balanced',
        n_jobs=-1
    )

    print("\n🎓 Training the model...")
    print("   This might take slightly longer due to more trees...")

    model.fit(X_train, y_train)

    print("✓ Training complete!")

    return model


def train_comparison_models(X_train, y_train):
    """
    Train 3 different regularization strategies for comparison
    """

    print("\n" + "=" * 60)
    print("TRAINING 3 REGULARIZATION STRATEGIES")
    print("=" * 60)

    models = {}

    # Strategy 1: Moderate regularization
    print("\n1️⃣  MODERATE Regularization:")
    print("   - Balanced approach between complexity and simplicity")
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

    # Strategy 2: Strong regularization
    print("\n2️⃣  STRONG Regularization:")
    print("   - Very simple model, maximum regularization")
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

    # Strategy 3: Light regularization
    print("\n3️⃣  LIGHT Regularization:")
    print("   - Still some complexity, gentler constraints")
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

    # Calculate capture rate (recall for positive class)
    if len(cm) > 1 and cm[1, 0] + cm[1, 1] > 0:
        capture_rate = cm[1, 1] / (cm[1, 0] + cm[1, 1])
    else:
        capture_rate = 0

    # Calculate precision for positive class
    if len(cm) > 1 and cm[0, 1] + cm[1, 1] > 0:
        precision = cm[1, 1] / (cm[0, 1] + cm[1, 1])
    else:
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
    """Train and compare multiple regularization strategies"""

    print("\n" + "=" * 60)
    print("COMPARING ALL MODELS")
    print("=" * 60)

    models = train_comparison_models(X_train, y_train)

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

    # Find best model
    print("\n" + "=" * 60)
    print("ANALYSIS")
    print("=" * 60)

    # Best by overfitting
    best_overfit = min(results, key=lambda x: x['overfitting_gap'])
    print(f"\n✅ Least Overfitting: {best_overfit['name']} (gap: {best_overfit['overfitting_gap'] * 100:.1f}%)")

    # Best by test accuracy
    best_acc = max(results, key=lambda x: x['test_acc'])
    print(f"✅ Best Test Accuracy: {best_acc['name']} ({best_acc['test_acc'] * 100:.1f}%)")

    # Best by capture rate
    best_capture = max(results, key=lambda x: x['capture_rate'])
    print(f"✅ Best Capture Rate: {best_capture['name']} ({best_capture['capture_rate'] * 100:.1f}%)")

    # Best by AUC
    best_auc = max(results, key=lambda x: x['test_auc'])
    print(f"✅ Best AUC: {best_auc['name']} ({best_auc['test_auc']:.3f})")

    # Recommendation
    print("\n" + "=" * 60)
    print("RECOMMENDATION")
    print("=" * 60)

    # Score each model (weighted combination)
    for r in results:
        # Lower overfitting = better (invert and normalize)
        overfit_score = max(0, 1 - r['overfitting_gap'] / 0.4)  # 40% gap = 0 score
        # Higher capture rate = better
        capture_score = r['capture_rate']
        # Higher test accuracy = better (but less important)
        acc_score = r['test_acc']

        # Weighted score: 40% capture, 40% overfit, 20% accuracy
        r['combined_score'] = (0.4 * capture_score + 0.4 * overfit_score + 0.2 * acc_score)

    best_overall = max(results, key=lambda x: x['combined_score'])

    print(f"\n🏆 RECOMMENDED MODEL: {best_overall['name'].upper()}")
    print(f"\n   Why this model:")
    print(f"   - Test Accuracy: {best_overall['test_acc'] * 100:.1f}%")
    print(f"   - Overfitting Gap: {best_overall['overfitting_gap'] * 100:.1f}% (lower is better)")
    print(f"   - Captures {best_overall['capture_rate'] * 100:.1f}% of high-profit opportunities")
    print(f"   - Precision: {best_overall['precision'] * 100:.1f}% (when it says yes, it's right this often)")
    print(f"   - AUC: {best_overall['test_auc']:.3f}")

    # Show confusion matrix for best model
    cm = best_overall['confusion_matrix']
    print(f"\n📊 Confusion Matrix (Recommended Model):")
    print(f"                    Predicted Low    Predicted High")
    print(f"  Actually Low:        {cm[0, 0]:6d}         {cm[0, 1]:6d}")
    print(f"  Actually High:       {cm[1, 0]:6d}         {cm[1, 1]:6d}")

    return models[best_overall['name']], best_overall['name'], results


def show_feature_importance(model, feature_names):
    """Show feature importance"""

    print("\n" + "=" * 60)
    print("FEATURE IMPORTANCE")
    print("=" * 60)

    importances = model.feature_importances_
    feature_importance = pd.DataFrame({
        'feature': feature_names,
        'importance': importances
    }).sort_values('importance', ascending=False)

    print("\n📊 Top 10 Most Important Features:")
    for i, row in feature_importance.head(10).iterrows():
        print(f"   {row['feature']:35s} {row['importance']:.4f}")


def save_model(model, model_name, threshold, output_dir='../../models'):
    """Save the best model"""

    print("\n" + "=" * 60)
    print("SAVING MODEL")
    print("=" * 60)

    os.makedirs(output_dir, exist_ok=True)
    threshold_label = f"{threshold:.1f}".replace('.', '_')
    model_path = f'{output_dir}/model_high_profit_gt{threshold_label}pct_improved_{model_name}.pkl'

    joblib.dump(model, model_path)

    print(f"\n✓ Model saved to: {model_path}")

    return model_path


def main():
    """Main execution"""

    threshold = 3.0

    # Load data
    X_train, X_test, y_train, y_test = load_prepared_data(threshold)

    # Check balance
    check_data_balance(y_train, y_test, threshold)

    # Train and compare all models
    best_model, best_name, all_results = compare_all_models(X_train, X_test, y_train, y_test)

    # Show feature importance
    show_feature_importance(best_model, X_train.columns)

    # Save best model
    model_path = save_model(best_model, best_name, threshold)

    print("\n" + "=" * 60)
    print("IMPROVED MODEL COMPLETE!")
    print("=" * 60)

    best_result = [r for r in all_results if r['name'] == best_name][0]

    print(f"\n✓ Tested 3 regularization strategies")
    print(f"✓ Best model: {best_name.upper()}")
    print(f"   - Test Accuracy: {best_result['test_acc'] * 100:.1f}%")
    print(f"   - Overfitting Gap: {best_result['overfitting_gap'] * 100:.1f}%")
    print(f"   - Capture Rate: {best_result['capture_rate'] * 100:.1f}%")
    print(f"✓ Model saved to: {model_path}")

    print("\n💡 Compare with original model:")
    print("   Original: 65.7% accuracy, 33% overfitting, 60% capture")
    print(
        f"   Improved: {best_result['test_acc'] * 100:.1f}% accuracy, {best_result['overfitting_gap'] * 100:.1f}% overfitting, {best_result['capture_rate'] * 100:.1f}% capture")


if __name__ == "__main__":
    main()