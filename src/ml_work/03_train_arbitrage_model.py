#!/usr/bin/env python3
"""
Step 3: Train Model 1 - Arbitrage Detection
This model predicts: "Will there be an arbitrage opportunity for this match?"
Answer: True/False (Yes/No)
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import joblib
import os


def load_prepared_data(data_dir='../../data/prepared'):
    """
    Load the CSV files we prepared in Step 2

    What we're loading:
    - Features (X): The input data the model will use to make predictions
    - Target (y): What we want to predict (will arbitrage exist?)
    """

    print("=" * 60)
    print("STEP 3: Train Arbitrage Detection Model")
    print("=" * 60)

    print("\n📂 Loading prepared data from CSV files...")

    # Load training data
    X_train = pd.read_csv(f'{data_dir}/features_train.csv')
    y_train = pd.read_csv(f'{data_dir}/target_train_arbitrage_exists.csv')['target']

    # Load test data
    X_test = pd.read_csv(f'{data_dir}/features_test.csv')
    y_test = pd.read_csv(f'{data_dir}/target_test_arbitrage_exists.csv')['target']

    print(f"✓ Training data: {len(X_train)} rows, {X_train.shape[1]} features")
    print(f"✓ Test data: {len(X_test)} rows, {X_test.shape[1]} features")

    return X_train, X_test, y_train, y_test


def check_data_balance(y_train, y_test):
    """
    Check if our data is balanced (similar number of Yes and No examples)

    Why this matters:
    - If 95% of examples are "No arbitrage", the model might just learn to always say No
    - We want a good mix so the model learns both patterns
    """

    print("\n" + "=" * 60)
    print("DATA BALANCE CHECK")
    print("=" * 60)

    train_counts = y_train.value_counts()
    test_counts = y_test.value_counts()

    print("\n📊 Training Set:")
    print(
        f"   Will have arbitrage (1): {train_counts.get(1, 0):,} ({100 * train_counts.get(1, 0) / len(y_train):.1f}%)")
    print(f"   No arbitrage (0): {train_counts.get(0, 0):,} ({100 * train_counts.get(0, 0) / len(y_train):.1f}%)")

    print("\n📊 Test Set:")
    print(f"   Will have arbitrage (1): {test_counts.get(1, 0):,} ({100 * test_counts.get(1, 0) / len(y_test):.1f}%)")
    print(f"   No arbitrage (0): {test_counts.get(0, 0):,} ({100 * test_counts.get(0, 0) / len(y_test):.1f}%)")

    # Warning if severely imbalanced
    train_majority_pct = max(train_counts.values) / len(y_train)
    if train_majority_pct > 0.9:
        print("\n⚠️  WARNING: Data is highly imbalanced (>90% one class)")
        print("   The model might be biased. We'll use class_weight='balanced' to help.")
        return True
    else:
        print("\n✓ Data balance looks reasonable")
        return False


def train_model(X_train, y_train, is_imbalanced=False):
    """
    Train a Random Forest Classifier

    What is Random Forest?
    - It's like asking 100 experts for their opinion, then taking a vote
    - Each "tree" in the forest looks at the data slightly differently
    - Final prediction = majority vote from all trees

    Why Random Forest?
    - Works well out of the box (doesn't need much tuning)
    - Handles lots of features well
    - Can capture complex patterns
    - Not easily fooled by noise
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

    # Create the model
    # class_weight='balanced' helps when data is imbalanced
    model = RandomForestClassifier(
        n_estimators=100,  # Number of trees in the forest
        max_depth=20,  # Maximum depth of each tree
        min_samples_split=10,  # Minimum samples needed to split a node
        random_state=42,  # Seed for reproducibility
        class_weight='balanced' if is_imbalanced else None,  # Handle imbalanced data
        n_jobs=-1  # Use all CPU cores for faster training
    )

    print("\n🎓 Training the model...")
    print("   This might take 30-60 seconds depending on your data size...")

    # This is where the "learning" happens!
    # The model looks at X_train (features) and y_train (targets)
    # and learns patterns that predict when arbitrage will exist
    model.fit(X_train, y_train)

    print("✓ Training complete!")

    return model


def evaluate_model(model, X_train, X_test, y_train, y_test):
    """
    Test how well the model works

    We'll check:
    1. Training accuracy - how well it learned the training data
    2. Test accuracy - how well it predicts NEW data it hasn't seen
    3. Detailed metrics - precision, recall, F1-score
    """

    print("\n" + "=" * 60)
    print("MODEL EVALUATION")
    print("=" * 60)

    # Make predictions on training data
    print("\n📈 Testing on training data...")
    train_predictions = model.predict(X_train)
    train_accuracy = accuracy_score(y_train, train_predictions)
    print(f"   Training Accuracy: {train_accuracy * 100:.2f}%")

    # Make predictions on test data (the important one!)
    print("\n📈 Testing on test data (new, unseen data)...")
    test_predictions = model.predict(X_test)
    test_accuracy = accuracy_score(y_test, test_predictions)
    print(f"   Test Accuracy: {test_accuracy * 100:.2f}%")

    # Interpret the results
    print("\n💡 What does this mean?")
    if test_accuracy > 0.85:
        print("   🎉 Excellent! The model is very accurate.")
    elif test_accuracy > 0.75:
        print("   ✓ Good accuracy. The model is working well.")
    elif test_accuracy > 0.65:
        print("   ⚠ Moderate accuracy. May need improvement.")
    else:
        print("   ❌ Low accuracy. Model needs work.")

    # Check for overfitting
    accuracy_gap = train_accuracy - test_accuracy
    if accuracy_gap > 0.1:
        print(f"\n⚠️  Warning: Training accuracy is {accuracy_gap * 100:.1f}% higher than test accuracy")
        print("   This suggests overfitting (model memorized training data)")

    # Detailed breakdown
    print("\n" + "=" * 60)
    print("DETAILED METRICS")
    print("=" * 60)

    print("\n📊 Classification Report (Test Set):")
    print("\nWhat these metrics mean:")
    print("  - Precision: When model says 'Yes arbitrage', how often is it right?")
    print("  - Recall: Of all actual arbitrage cases, how many did we catch?")
    print("  - F1-score: Balance between precision and recall (higher is better)")
    print()

    # This shows performance for each class (0=No, 1=Yes)
    report = classification_report(y_test, test_predictions,
                                   target_names=['No Arbitrage', 'Has Arbitrage'])
    print(report)

    # Confusion Matrix
    print("\n📊 Confusion Matrix:")
    print("\nWhat this shows:")
    print("  Rows = Actual values, Columns = Predicted values")
    print()
    cm = confusion_matrix(y_test, test_predictions)
    print(f"                    Predicted No    Predicted Yes")
    print(f"  Actually No:        {cm[0, 0]:6d}         {cm[0, 1]:6d}")
    print(f"  Actually Yes:       {cm[1, 0]:6d}         {cm[1, 1]:6d}")

    # Explain the confusion matrix
    true_negatives = cm[0, 0]  # Correctly predicted No
    false_positives = cm[0, 1]  # Wrongly predicted Yes
    false_negatives = cm[1, 0]  # Wrongly predicted No (MISSED opportunities!)
    true_positives = cm[1, 1]  # Correctly predicted Yes

    print("\n💡 In plain English:")
    print(f"   ✓ Correctly identified {true_positives:,} arbitrage opportunities")
    print(f"   ✓ Correctly identified {true_negatives:,} non-arbitrage cases")
    print(f"   ✗ Missed {false_negatives:,} arbitrage opportunities (false negatives)")
    print(f"   ✗ False alarms: {false_positives:,} times (predicted arbitrage when there wasn't)")

    return test_accuracy, test_predictions


def show_feature_importance(model, feature_names):
    """
    Show which features the model finds most important

    This tells us: "What does the model pay attention to when making predictions?"
    """

    print("\n" + "=" * 60)
    print("FEATURE IMPORTANCE")
    print("=" * 60)

    print("\n🔍 Which features matter most for predictions?")

    # Get importance scores
    importances = model.feature_importances_

    # Create a dataframe and sort by importance
    feature_importance = pd.DataFrame({
        'feature': feature_names,
        'importance': importances
    }).sort_values('importance', ascending=False)

    print("\n📊 Top 10 Most Important Features:")
    for i, row in feature_importance.head(10).iterrows():
        print(f"   {row['feature']:35s} {row['importance']:.4f}")

    print("\n💡 What this means:")
    print("   Higher scores = model relies on these features more")
    print("   These are the patterns the model learned to look for")


def save_model(model, output_dir='../../models'):
    """
    Save the trained model so we can use it later without retraining
    """

    print("\n" + "=" * 60)
    print("SAVING MODEL")
    print("=" * 60)

    os.makedirs(output_dir, exist_ok=True)
    model_path = f'{output_dir}/model_arbitrage_detection.pkl'

    # Save using joblib (efficient for sklearn models)
    joblib.dump(model, model_path)

    print(f"\n✓ Model saved to: {model_path}")
    print("   You can load it later with: model = joblib.load('{model_path}')")

    return model_path


def main():
    """Main execution"""

    # Step 1: Load data
    X_train, X_test, y_train, y_test = load_prepared_data()

    # Step 2: Check balance
    is_imbalanced = check_data_balance(y_train, y_test)

    # Step 3: Train the model
    model = train_model(X_train, y_train, is_imbalanced)

    # Step 4: Evaluate performance
    test_accuracy, predictions = evaluate_model(model, X_train, X_test, y_train, y_test)

    # Step 5: Show feature importance
    show_feature_importance(model, X_train.columns)

    # Step 6: Save the model
    model_path = save_model(model)

    print("\n" + "=" * 60)
    print("STEP 3 COMPLETE!")
    print("=" * 60)
    print(f"\n✓ Model trained with {test_accuracy * 100:.1f}% test accuracy")
    print(f"✓ Model saved to: {model_path}")
    print("\nNext steps:")
    print("  1. Train Model 2: Profit Prediction (how much profit?)")
    print("  2. Train Models 3-5: Betting Timing (when to bet each outcome?)")
    print("\n💡 You now have a working arbitrage detector!")
    print("   You can use it to predict if a match will have arbitrage opportunities")


if __name__ == "__main__":
    main()