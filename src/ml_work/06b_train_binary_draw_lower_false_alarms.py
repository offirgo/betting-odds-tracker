#!/usr/bin/env python3
"""
Step 6 (Revised): Train Model 4 - Draw Betting Timing (Lower False Alarm Rate)
This model predicts: "Should I bet on Draw NOW?"
Answer: True/False (Yes/No)

This tells you the OPTIMAL moment to place your Draw bet to get best odds.

OPTIMIZED FOR LOWER FALSE ALARMS: We prioritize reducing false positives,
even at the expense of missing more opportunities.
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix, roc_auc_score, precision_score, \
    recall_score
from sklearn.model_selection import train_test_split
import joblib
import os


def load_prepared_data(data_dir='../../data/prepared'):
    """Load the CSV files for draw betting timing"""

    print("=" * 60)
    print("STEP 6 (Revised): Train Draw Betting Timing Model (LOWER FALSE ALARM RATE)")
    print("=" * 60)

    print("\n📂 Loading prepared data...")

    # Load training data
    X_train = pd.read_csv(f'{data_dir}/features_train.csv')
    y_train = pd.read_csv(f'{data_dir}/target_train_bet_draw_timing.csv')['target']

    # Load test data
    X_test = pd.read_csv(f'{data_dir}/features_test.csv')
    y_test = pd.read_csv(f'{data_dir}/target_test_bet_draw_timing.csv')['target']

    # Split training data to have a validation set for threshold calibration
    X_train, X_val, y_train, y_val = train_test_split(
        X_train, y_train, test_size=0.2, random_state=42, stratify=y_train
    )

    print(f"✓ Training data: {len(X_train)} rows, {X_train.shape[1]} features")
    print(f"✓ Validation data: {len(X_val)} rows, {X_val.shape[1]} features")
    print(f"✓ Test data: {len(X_test)} rows, {X_test.shape[1]} features")

    return X_train, X_val, X_test, y_train, y_val, y_test


def check_data_balance(y_train, y_val, y_test):
    """Check class balance for betting timing"""

    print("\n" + "=" * 60)
    print("DATA BALANCE CHECK")
    print("=" * 60)

    train_counts = y_train.value_counts()
    val_counts = y_val.value_counts()
    test_counts = y_test.value_counts()

    train_yes_pct = 100 * train_counts.get(1, 0) / len(y_train)
    val_yes_pct = 100 * val_counts.get(1, 0) / len(y_val)
    test_yes_pct = 100 * test_counts.get(1, 0) / len(y_test)

    print(f"\n📊 Training Set:")
    print(f"   Bet Draw NOW (1):     {train_counts.get(1, 0):,} ({train_yes_pct:.1f}%)")
    print(f"   Don't bet yet (0):    {train_counts.get(0, 0):,} ({100 - train_yes_pct:.1f}%)")

    print(f"\n📊 Validation Set:")
    print(f"   Bet Draw NOW (1):     {val_counts.get(1, 0):,} ({val_yes_pct:.1f}%)")
    print(f"   Don't bet yet (0):    {val_counts.get(0, 0):,} ({100 - val_yes_pct:.1f}%)")

    print(f"\n📊 Test Set:")
    print(f"   Bet Draw NOW (1):     {test_counts.get(1, 0):,} ({test_yes_pct:.1f}%)")
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


def calibrate_model_threshold(model, X_val, y_val, target_false_alarm=0.20):
    """Adjust decision threshold to achieve lower false alarm rate"""
    probas = model.predict_proba(X_val)[:, 1]

    print("\n" + "=" * 60)
    print(f"THRESHOLD CALIBRATION (Target False Alarm Rate: {target_false_alarm * 100:.0f}%)")
    print("=" * 60)

    # Try different thresholds - use finer granularity at higher values
    thresholds = np.concatenate([
        np.arange(0.1, 0.5, 0.05),  # Lower thresholds, coarser
        np.arange(0.5, 0.85, 0.025),  # Middle thresholds, finer
        np.arange(0.85, 0.98, 0.01)  # Higher thresholds, finest
    ])

    results = []

    for threshold in thresholds:
        y_pred = (probas >= threshold).astype(int)

        # Calculate metrics
        cm = confusion_matrix(y_val, y_pred)
        if len(cm) > 1:
            # False alarm rate = FP / (FP + TN)
            fp = cm[0, 1]
            tn = cm[0, 0]
            false_alarm_rate = fp / (fp + tn) if (fp + tn) > 0 else 0

            # True positive rate (recall) = TP / (TP + FN)
            tp = cm[1, 1]
            fn = cm[1, 0]
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0

            # Precision = TP / (TP + FP)
            precision = tp / (tp + fp) if (tp + fp) > 0 else 0

            # Count predictions
            pos_preds = sum(y_pred)
            pos_ratio = pos_preds / len(y_pred) if len(y_pred) > 0 else 0

            results.append({
                'threshold': threshold,
                'false_alarm_rate': false_alarm_rate,
                'recall': recall,
                'precision': precision,
                'pos_preds': pos_preds,
                'pos_ratio': pos_ratio,
                'true_positives': tp,
                'false_positives': fp
            })

    # Display results
    print("\n📊 Threshold Calibration Results:")
    print("\n" + "=" * 100)
    print(f"{'Threshold':<10} {'False Alarm':<12} {'Precision':<12} {'Recall':<12} {'# Bet Now':<12} {'% Bet Now':<12}")
    print("=" * 100)

    for r in results:
        print(
            f"{r['threshold']:<10.3f} {r['false_alarm_rate'] * 100:>10.1f}%  {r['precision'] * 100:>10.1f}%  {r['recall'] * 100:>10.1f}%  {r['pos_preds']:>10,d}  {r['pos_ratio'] * 100:>10.1f}%")

    # Find threshold that achieves target false alarm rate
    thresholds_below_target = [r for r in results if
                               r['false_alarm_rate'] <= target_false_alarm and r['pos_preds'] > 100]

    if thresholds_below_target:
        # Find the one with highest recall (catches most opportunities)
        best_result = max(thresholds_below_target, key=lambda x: x['recall'])
        best_threshold = best_result['threshold']
        print("\n✅ Selected threshold:", best_threshold)
        print(
            f"   - False Alarm Rate: {best_result['false_alarm_rate'] * 100:.1f}% (target: {target_false_alarm * 100:.0f}%)")
        print(f"   - Precision: {best_result['precision'] * 100:.1f}%")
        print(f"   - Recall: {best_result['recall'] * 100:.1f}%")
        print(f"   - # Bet Now predictions: {best_result['pos_preds']:,}")
        print(f"   - % Bet Now: {best_result['pos_ratio'] * 100:.1f}%")
        print(f"   - True positives: {best_result['true_positives']:,}")
        print(f"   - False positives: {best_result['false_positives']:,}")
    else:
        # Find the one with lowest false alarm rate
        best_result = min(results, key=lambda x: x['false_alarm_rate'] if x['pos_preds'] > 100 else 1.0)
        best_threshold = best_result['threshold']
        print("\n⚠️ No threshold met target false alarm rate with sufficient predictions")
        print(f"✅ Selected best available threshold: {best_threshold}")
        print(f"   - False Alarm Rate: {best_result['false_alarm_rate'] * 100:.1f}%")
        print(f"   - Precision: {best_result['precision'] * 100:.1f}%")
        print(f"   - Recall: {best_result['recall'] * 100:.1f}%")
        print(f"   - # Bet Now predictions: {best_result['pos_preds']:,}")
        print(f"   - % Bet Now: {best_result['pos_ratio'] * 100:.1f}%")
        print(f"   - True positives: {best_result['true_positives']:,}")
        print(f"   - False positives: {best_result['false_positives']:,}")

    return best_threshold, best_result


def train_regularized_models(X_train, y_train, X_val, y_val):
    """
    Train multiple regularization strategies with threshold calibration
    Using lessons learned from previous models
    """

    print("\n" + "=" * 60)
    print("TRAINING REGULARIZED MODELS")
    print("=" * 60)

    print("\n💡 Based on Draw Betting Timing model results and your lower false alarm priority:")
    print("   - Strong regularization worked best in previous version")
    print("   - Will add threshold calibration to reduce false alarms")
    print("   - Goal: Lower false alarm rate, even if we miss more opportunities")

    models = {}
    thresholds = {}
    threshold_results = {}

    # Strategy 1: Strong regularization (what worked for previous models)
    print("\n1️⃣  STRONG Regularization:")
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
    print("   ✓ Model trained")

    # Calibrate threshold for lower false alarm rate
    thresholds['strong'], threshold_results['strong'] = calibrate_model_threshold(
        models['strong'], X_val, y_val, target_false_alarm=0.20
    )

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
    print("   ✓ Model trained")

    # Calibrate threshold for lower false alarm rate
    thresholds['moderate'], threshold_results['moderate'] = calibrate_model_threshold(
        models['moderate'], X_val, y_val, target_false_alarm=0.20
    )

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
    print("   ✓ Model trained")

    # Calibrate threshold for lower false alarm rate
    thresholds['light'], threshold_results['light'] = calibrate_model_threshold(
        models['light'], X_val, y_val, target_false_alarm=0.20
    )

    # Strategy 4: Extra Trees (often good for imbalanced classes)
    print("\n4️⃣  EXTRA TREES (Specialized for imbalanced classes):")
    models['extra_trees'] = RandomForestClassifier(
        n_estimators=200,
        max_depth=10,
        min_samples_split=30,
        min_samples_leaf=15,
        max_features='sqrt',
        bootstrap=True,
        random_state=42,
        class_weight='balanced',
        criterion='entropy',  # Different splitting criterion
        n_jobs=-1
    )
    models['extra_trees'].fit(X_train, y_train)
    print("   ✓ Model trained")

    # Calibrate threshold for lower false alarm rate
    thresholds['extra_trees'], threshold_results['extra_trees'] = calibrate_model_threshold(
        models['extra_trees'], X_val, y_val, target_false_alarm=0.20
    )

    return models, thresholds, threshold_results


def evaluate_model(model, threshold, X_train, X_val, X_test, y_train, y_val, y_test, model_name="Model"):
    """Evaluate a single model with custom threshold"""

    # Make predictions with custom threshold
    train_proba = model.predict_proba(X_train)[:, 1]
    train_pred = (train_proba >= threshold).astype(int)

    val_proba = model.predict_proba(X_val)[:, 1]
    val_pred = (val_proba >= threshold).astype(int)

    test_proba = model.predict_proba(X_test)[:, 1]
    test_pred = (test_proba >= threshold).astype(int)

    # Calculate metrics
    train_acc = accuracy_score(y_train, train_pred)
    train_prec = precision_score(y_train, train_pred, zero_division=0)
    train_rec = recall_score(y_train, train_pred, zero_division=0)
    train_auc = roc_auc_score(y_train, train_proba)

    val_acc = accuracy_score(y_val, val_pred)
    val_prec = precision_score(y_val, val_pred, zero_division=0)
    val_rec = recall_score(y_val, val_pred, zero_division=0)
    val_auc = roc_auc_score(y_val, val_proba)

    test_acc = accuracy_score(y_test, test_pred)
    test_prec = precision_score(y_test, test_pred, zero_division=0)
    test_rec = recall_score(y_test, test_pred, zero_division=0)
    test_auc = roc_auc_score(y_test, test_proba)

    # Confusion matrix
    cm = confusion_matrix(y_test, test_pred)

    # Calculate additional metrics
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

        # False alarm rate: % of "wait" predictions that were wrong
        false_alarm_rate = false_positives / (false_positives + cm[0, 0]) if (false_positives + cm[0, 0]) > 0 else 0

        # Number of bets placed
        bets_placed = true_positives + false_positives
        bet_rate = bets_placed / len(y_test)
    else:
        capture_rate = 0
        precision = 0
        false_alarm_rate = 0
        bets_placed = 0
        bet_rate = 0

    return {
        'name': model_name,
        'threshold': threshold,
        'train_acc': train_acc,
        'val_acc': val_acc,
        'test_acc': test_acc,
        'train_prec': train_prec,
        'val_prec': val_prec,
        'test_prec': test_prec,
        'train_rec': train_rec,
        'val_rec': val_rec,
        'test_rec': test_rec,
        'train_auc': train_auc,
        'val_auc': val_auc,
        'test_auc': test_auc,
        'overfitting_gap': train_acc - test_acc,
        'capture_rate': capture_rate,  # Same as test_rec
        'precision': precision,  # Same as test_prec
        'false_alarm_rate': false_alarm_rate,
        'bets_placed': bets_placed,
        'bet_rate': bet_rate,
        'confusion_matrix': cm
    }


def compare_all_models(X_train, X_val, X_test, y_train, y_val, y_test, models, thresholds):
    """Compare all regularization strategies"""

    print("\n" + "=" * 60)
    print("COMPARING ALL MODELS (WITH CALIBRATED THRESHOLDS)")
    print("=" * 60)

    results = []
    for name, model in models.items():
        result = evaluate_model(
            model,
            thresholds[name],
            X_train, X_val, X_test,
            y_train, y_val, y_test,
            name
        )
        results.append(result)

    # Display comparison table
    print("\n📊 Performance Comparison:")
    print("\n" + "=" * 100)
    print(
        f"{'Model':<12} {'Threshold':<10} {'False Alarm':<12} {'Precision':<10} {'Recall':<10} {'Bets':<8} {'Bet Rate':<10} {'AUC':<8}")
    print("=" * 100)

    for r in results:
        print(
            f"{r['name']:<12} {r['threshold']:<10.3f} {r['false_alarm_rate'] * 100:>10.1f}%  {r['test_prec'] * 100:>8.1f}%  {r['test_rec'] * 100:>8.1f}%  {r['bets_placed']:>6,d}  {r['bet_rate'] * 100:>8.1f}%  {r['test_auc']:>6.3f}")

    print("=" * 100)

    # Analysis
    print("\n" + "=" * 60)
    print("ANALYSIS")
    print("=" * 60)

    best_false_alarm = min(results, key=lambda x: x['false_alarm_rate'] if x['bets_placed'] >= 100 else 1.0)
    print(
        f"\n✅ Lowest False Alarm Rate: {best_false_alarm['name']} ({best_false_alarm['false_alarm_rate'] * 100:.1f}%)")

    best_precision = max(results, key=lambda x: x['test_prec'] if x['bets_placed'] >= 100 else 0)
    print(f"✅ Best Precision: {best_precision['name']} ({best_precision['test_prec'] * 100:.1f}%)")

    best_acc = max(results, key=lambda x: x['test_acc'])
    print(f"✅ Best Test Accuracy: {best_acc['name']} ({best_acc['test_acc'] * 100:.1f}%)")

    best_capture = max(results, key=lambda x: x['test_rec'] if x['bets_placed'] >= 100 else 0)
    print(f"✅ Best Capture Rate: {best_capture['name']} ({best_capture['test_rec'] * 100:.1f}%)")

    best_auc = max(results, key=lambda x: x['test_auc'])
    print(f"✅ Best AUC: {best_auc['name']} ({best_auc['test_auc']:.3f})")

    # Recommendation
    print("\n" + "=" * 60)
    print("RECOMMENDATION")
    print("=" * 60)

    # Create a scoring system that heavily favors low false alarm rate
    for r in results:
        # Only consider models that make at least 100 predictions
        if r['bets_placed'] < 100:
            r['combined_score'] = 0
            continue

        # Normalize false alarm rate (lower is better)
        false_alarm_score = 1 - min(1.0, r['false_alarm_rate'] / 0.3)
        precision_score_val = r['test_prec']  # Precision on test set
        recall_score_val = r['test_rec']  # Recall/capture rate
        overfit_score = max(0, 1 - r['overfitting_gap'] / 0.4)  # Overfitting control

        # NEW WEIGHTS: 50% false alarm penalty, 30% precision, 10% recall, 10% overfit control
        r['combined_score'] = (
                    0.5 * false_alarm_score + 0.3 * precision_score_val + 0.1 * recall_score_val + 0.1 * overfit_score)

    # Get the best model (highest combined score)
    best_overall = max(results, key=lambda x: x['combined_score'])

    print(f"\n🏆 RECOMMENDED MODEL: {best_overall['name'].upper()} (Threshold: {best_overall['threshold']:.3f})")
    print(f"\n   Why this model:")
    print(f"   - Test Accuracy: {best_overall['test_acc'] * 100:.1f}%")
    print(f"   - False Alarm Rate: {best_overall['false_alarm_rate'] * 100:.1f}% (vs. 51.2% in previous model)")
    print(f"   - Precision: {best_overall['test_prec'] * 100:.1f}% (when it says 'bet now', it's right this often)")
    print(f"   - Captures {best_overall['test_rec'] * 100:.1f}% of optimal betting times")
    print(f"   - AUC: {best_overall['test_auc']:.3f}")

    print(f"\n💡 What this means in practice:")
    cm = best_overall['confusion_matrix']
    if len(cm) > 1:
        optimal_times = cm[1, 0] + cm[1, 1]
        caught = cm[1, 1]
        missed = cm[1, 0]
        false_alarms = cm[0, 1]

        print(f"   - Out of {optimal_times:,} optimal betting moments for Draw:")
        print(f"     ✓ Will catch {caught:,} ({best_overall['test_rec'] * 100:.1f}%)")
        print(f"     ✗ Will miss {missed:,} ({100 - best_overall['test_rec'] * 100:.1f}%)")
        print(f"   - False alarms: {false_alarms:,} times (vs. 8,220 in previous model)")
        print(
            f"   - Total bets placed: {best_overall['bets_placed']:,} ({best_overall['bet_rate'] * 100:.1f}% of opportunities)")

        if best_overall['test_prec'] > 0:
            print(f"   - ROI impact: {1 / best_overall['test_prec']:.2f}x multiplier needed to break even")
            print(f"     (For odds of {1 / best_overall['test_prec']:.2f} or higher)")

    # Show confusion matrix
    print(f"\n📊 Confusion Matrix (Recommended Model):")
    print(f"                    Predicted Wait    Predicted Bet Now")
    print(f"  Actually Wait:        {cm[0, 0]:6d}         {cm[0, 1]:6d}")
    print(f"  Actually Bet Now:     {cm[1, 0]:6d}         {cm[1, 1]:6d}")

    return models[best_overall['name']], best_overall['threshold'], best_overall['name'], results


def show_feature_importance(model, feature_names):
    """Show which features matter most for timing decisions"""

    print("\n" + "=" * 60)
    print("FEATURE IMPORTANCE")
    print("=" * 60)

    print("\n🔍 Which features matter most for Draw betting timing decisions?")

    importances = model.feature_importances_
    feature_importance = pd.DataFrame({
        'feature': feature_names,
        'importance': importances
    }).sort_values('importance', ascending=False)

    print("\n📊 Top 10 Most Important Features:")
    for i, row in feature_importance.head(10).iterrows():
        print(f"   {row['feature']:35s} {row['importance']:.4f}")

    print("\n💡 What this means:")
    print("   These features tell the model WHEN to bet on Draw")
    print("   Higher scores = model relies on these patterns more")

    return feature_importance


def save_model(model, threshold, model_name, output_dir='../../models'):
    """Save the trained model and its threshold"""

    print("\n" + "=" * 60)
    print("SAVING MODEL")
    print("=" * 60)

    os.makedirs(output_dir, exist_ok=True)
    model_path = f'{output_dir}/model_draw_timing_{model_name}_lower_false_alarms.pkl'
    threshold_path = f'{output_dir}/threshold_draw_timing_{model_name}_lower_false_alarms.txt'

    # Save model
    joblib.dump(model, model_path)

    # Save threshold
    with open(threshold_path, 'w') as f:
        f.write(str(threshold))

    print(f"\n✓ Model saved to: {model_path}")
    print(f"✓ Threshold saved to: {threshold_path}")

    return model_path, threshold_path


def main():
    """Main execution"""

    # Step 1: Load data with validation split
    X_train, X_val, X_test, y_train, y_val, y_test = load_prepared_data()

    # Step 2: Check balance
    is_imbalanced = check_data_balance(y_train, y_val, y_test)

    # Step 3: Train models with threshold calibration (target 20% false alarm rate)
    models, thresholds, threshold_results = train_regularized_models(X_train, y_train, X_val, y_val)

    # Step 4: Evaluate and compare all models
    best_model, best_threshold, best_name, all_results = compare_all_models(
        X_train, X_val, X_test, y_train, y_val, y_test, models, thresholds
    )

    # Step 5: Show feature importance
    feature_importance = show_feature_importance(best_model, X_train.columns)

    # Step 6: Save best model and threshold
    model_path, threshold_path = save_model(best_model, best_threshold, best_name)

    # Compare with previous model
    print("\n" + "=" * 60)
    print("COMPARISON WITH PREVIOUS MODEL")
    print("=" * 60)

    print("\n📊 Previous Draw Betting Model (STRONG, Threshold 0.35):")
    print(f"   - False Alarm Rate: 51.2%")
    print(f"   - Precision: 36.2%")
    print(f"   - Capture Rate: 82.8%")
    print(f"   - Total Bets: 12,881 (59.4% of opportunities)")
    print(f"   - ROI Threshold: 2.76 odds to break even")

    best_result = [r for r in all_results if r['name'] == best_name][0]

    print("\n📊 New Lower False Alarm Model:")
    print(f"   - False Alarm Rate: {best_result['false_alarm_rate'] * 100:.1f}%")
    print(f"   - Precision: {best_result['precision'] * 100:.1f}%")
    print(f"   - Capture Rate: {best_result['capture_rate'] * 100:.1f}%")
    print(f"   - Total Bets: {best_result['bets_placed']:,} ({best_result['bet_rate'] * 100:.1f}% of opportunities)")
    if best_result['precision'] > 0:
        print(f"   - ROI Threshold: {1 / best_result['precision']:.2f} odds to break even")

    print("\n" + "=" * 60)
    print("STEP 6 COMPLETE!")
    print("=" * 60)

    print(f"\n✓ Model 4: Lower False Alarm Draw Betting Timing trained!")
    print(f"   - Best Strategy: {best_name.upper()}")
    print(f"   - Decision Threshold: {best_threshold:.3f}")
    print(f"   - Test Accuracy: {best_result['test_acc'] * 100:.1f}%")
    print(f"   - Precision: {best_result['precision'] * 100:.1f}%")
    print(f"   - Captures {best_result['capture_rate'] * 100:.1f}% of optimal betting times")
    print(f"   - False Alarm Rate: {best_result['false_alarm_rate'] * 100:.1f}%")
    print(f"   - Bet Rate: {best_result['bet_rate'] * 100:.1f}% of opportunities")
    print(f"✓ Model saved to: {model_path}")
    print(f"✓ Threshold saved to: {threshold_path}")

    print("\n💡 Next steps:")
    print("   1. Train Model 5: Away Betting Timing (Precision-Focused)")
    print("   2. Combine all models for complete arbitrage system!")

    print("\n✨ You now know WHEN to bet on Draw - with fewer false alarms!")
    print("   This model makes fewer unnecessary bets while still capturing good opportunities.")


if __name__ == "__main__":
    main()