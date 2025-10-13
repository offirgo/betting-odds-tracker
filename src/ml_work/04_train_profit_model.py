#!/usr/bin/env python3
"""
Step 4: Train Model 2 - Profit Prediction
This model predicts: "What is the maximum arbitrage profit % for this match?"
Answer: A percentage (e.g., 2.5%, 4.8%, 0.3%)
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import joblib
import os


def load_prepared_data(data_dir='../../data/prepared'):
    """
    Load the CSV files we prepared in Step 2

    What we're loading:
    - Features (X): The input data the model will use to make predictions
    - Target (y): What we want to predict (maximum profit percentage)
    """

    print("=" * 60)
    print("STEP 4: Train Profit Prediction Model")
    print("=" * 60)

    print("\n📂 Loading prepared data from CSV files...")

    # Load training data
    X_train = pd.read_csv(f'{data_dir}/features_train.csv')
    y_train = pd.read_csv(f'{data_dir}/target_train_profit_percent.csv')['target']

    # Load test data
    X_test = pd.read_csv(f'{data_dir}/features_test.csv')
    y_test = pd.read_csv(f'{data_dir}/target_test_profit_percent.csv')['target']

    print(f"✓ Training data: {len(X_train)} rows, {X_train.shape[1]} features")
    print(f"✓ Test data: {len(X_test)} rows, {X_test.shape[1]} features")

    return X_train, X_test, y_train, y_test


def analyze_target_distribution(y_train, y_test):
    """
    Understand the distribution of profit percentages

    Why this matters:
    - Helps us understand what profit ranges are common
    - Identifies outliers or unusual patterns
    - Informs our expectations for model performance
    """

    print("\n" + "=" * 60)
    print("PROFIT DISTRIBUTION ANALYSIS")
    print("=" * 60)

    print("\n📊 Training Set Profit Statistics:")
    print(f"   Mean profit:        {y_train.mean():.2f}%")
    print(f"   Median profit:      {y_train.median():.2f}%")
    print(f"   Std deviation:      {y_train.std():.2f}%")
    print(f"   Min profit:         {y_train.min():.2f}%")
    print(f"   Max profit:         {y_train.max():.2f}%")

    print("\n📊 Test Set Profit Statistics:")
    print(f"   Mean profit:        {y_test.mean():.2f}%")
    print(f"   Median profit:      {y_test.median():.2f}%")
    print(f"   Std deviation:      {y_test.std():.2f}%")
    print(f"   Min profit:         {y_test.min():.2f}%")
    print(f"   Max profit:         {y_test.max():.2f}%")

    # Show profit distribution in buckets
    print("\n📊 Profit Distribution (Training Set):")
    bins = [0, 1, 2, 3, 5, 10, 100]
    labels = ['0-1%', '1-2%', '2-3%', '3-5%', '5-10%', '10%+']
    profit_buckets = pd.cut(y_train, bins=bins, labels=labels)
    bucket_counts = profit_buckets.value_counts().sort_index()

    for bucket, count in bucket_counts.items():
        pct = 100 * count / len(y_train)
        print(f"   {bucket:8s}: {count:6,} matches ({pct:5.1f}%)")

    print("\n💡 What this tells us:")
    if y_train.mean() < 2:
        print("   Most arbitrage opportunities are small (< 2% profit)")
        print("   High-profit opportunities are rare - these are the gems to find!")
    elif y_train.mean() < 5:
        print("   Moderate profit opportunities are common")
        print("   Focus on consistently identifying 3-5% opportunities")
    else:
        print("   High profit opportunities available!")
        print("   Model should focus on accuracy for these valuable predictions")


def train_model(X_train, y_train):
    """
    Train a Random Forest Regressor

    What is Random Forest Regressor?
    - Like the classifier, but predicts numbers instead of categories
    - Each tree predicts a profit percentage
    - Final prediction = average of all tree predictions

    Why Random Forest for regression?
    - Handles non-linear relationships well
    - Robust to outliers
    - Can capture complex patterns in profit dynamics
    - Works well with many features
    """

    print("\n" + "=" * 60)
    print("TRAINING THE MODEL")
    print("=" * 60)

    print("\n🌲 Creating Random Forest Regressor...")
    print("   Parameters:")
    print("   - n_estimators=100 (we'll build 100 decision trees)")
    print("   - max_depth=20 (each tree can be up to 20 levels deep)")
    print("   - min_samples_split=10 (need at least 10 examples to split a node)")
    print("   - min_samples_leaf=5 (each leaf must have at least 5 examples)")
    print("   - random_state=42 (for reproducible results)")

    # Create the model
    model = RandomForestRegressor(
        n_estimators=100,  # Number of trees in the forest
        max_depth=20,  # Maximum depth of each tree
        min_samples_split=10,  # Minimum samples needed to split a node
        min_samples_leaf=5,  # Minimum samples in a leaf node
        random_state=42,  # Seed for reproducibility
        n_jobs=-1  # Use all CPU cores for faster training
    )

    print("\n🎓 Training the model...")
    print("   This might take 30-60 seconds depending on your data size...")

    # Train the model
    # The model learns patterns that predict maximum profit percentage
    model.fit(X_train, y_train)

    print("✓ Training complete!")

    return model


def evaluate_model(model, X_train, X_test, y_train, y_test):
    """
    Test how well the model predicts profit percentages

    We'll use regression metrics:
    1. RMSE (Root Mean Squared Error) - average prediction error
    2. MAE (Mean Absolute Error) - average absolute difference
    3. R² Score - how much variance is explained (0-1, higher is better)
    """

    print("\n" + "=" * 60)
    print("MODEL EVALUATION")
    print("=" * 60)

    # Make predictions on training data
    print("\n📈 Testing on training data...")
    train_predictions = model.predict(X_train)
    train_rmse = np.sqrt(mean_squared_error(y_train, train_predictions))
    train_mae = mean_absolute_error(y_train, train_predictions)
    train_r2 = r2_score(y_train, train_predictions)

    print(f"   Training RMSE:  {train_rmse:.3f}%")
    print(f"   Training MAE:   {train_mae:.3f}%")
    print(f"   Training R²:    {train_r2:.3f}")

    # Make predictions on test data (the important one!)
    print("\n📈 Testing on test data (new, unseen data)...")
    test_predictions = model.predict(X_test)
    test_rmse = np.sqrt(mean_squared_error(y_test, test_predictions))
    test_mae = mean_absolute_error(y_test, test_predictions)
    test_r2 = r2_score(y_test, test_predictions)

    print(f"   Test RMSE:      {test_rmse:.3f}%")
    print(f"   Test MAE:       {test_mae:.3f}%")
    print(f"   Test R²:        {test_r2:.3f}")

    # Interpret the results
    print("\n💡 What does this mean?")
    print(f"   RMSE: On average, predictions are off by ±{test_rmse:.2f}%")
    print(f"   MAE:  Typical prediction error is {test_mae:.2f}%")

    if test_r2 > 0.8:
        print(f"   R²:   Excellent! Model explains {test_r2 * 100:.1f}% of variance")
    elif test_r2 > 0.6:
        print(f"   R²:   Good! Model explains {test_r2 * 100:.1f}% of variance")
    elif test_r2 > 0.4:
        print(f"   R²:   Moderate. Model explains {test_r2 * 100:.1f}% of variance")
    else:
        print(f"   R²:   Low. Model explains only {test_r2 * 100:.1f}% of variance")

    # Check for overfitting
    r2_gap = train_r2 - test_r2
    if r2_gap > 0.1:
        print(f"\n⚠️  Warning: Training R² is {r2_gap:.3f} higher than test R²")
        print("   This suggests overfitting (model memorized training data)")

    return test_predictions, test_rmse, test_mae, test_r2


def detailed_error_analysis(y_test, predictions):
    """
    Analyze where the model makes mistakes

    This helps us understand:
    - Does it overestimate or underestimate profits?
    - Are errors worse for high-profit or low-profit matches?
    """

    print("\n" + "=" * 60)
    print("DETAILED ERROR ANALYSIS")
    print("=" * 60)

    # Calculate errors
    errors = predictions - y_test
    abs_errors = np.abs(errors)

    # Overall error distribution
    print("\n📊 Error Distribution:")
    print(f"   Mean error (bias):           {errors.mean():.3f}%")
    print(f"   Median absolute error:       {np.median(abs_errors):.3f}%")

    if errors.mean() > 0.1:
        print("   ⚠️  Model tends to OVERESTIMATE profits")
    elif errors.mean() < -0.1:
        print("   ⚠️  Model tends to UNDERESTIMATE profits")
    else:
        print("   ✓ Model is well-calibrated (no systematic bias)")

    # Error by profit level
    print("\n📊 Errors by Actual Profit Level:")
    df_analysis = pd.DataFrame({
        'actual': y_test,
        'predicted': predictions,
        'error': abs_errors
    })

    # Group by profit buckets
    bins = [0, 1, 2, 3, 5, 10, 100]
    labels = ['0-1%', '1-2%', '2-3%', '3-5%', '5-10%', '10%+']
    df_analysis['profit_bucket'] = pd.cut(df_analysis['actual'], bins=bins, labels=labels)

    bucket_errors = df_analysis.groupby('profit_bucket')['error'].agg(['mean', 'median', 'count'])

    print("\n   Profit Range  |  Avg Error  |  Median Error  |  Count")
    print("   " + "-" * 60)
    for bucket in labels:
        if bucket in bucket_errors.index:
            row = bucket_errors.loc[bucket]
            print(f"   {bucket:12s}  |  {row['mean']:8.3f}%  |  {row['median']:11.3f}%  |  {int(row['count']):6,}")

    print("\n💡 Interpretation:")
    print("   Lower errors = model is more accurate for that profit range")
    print("   Focus on improving predictions for high-error buckets")

    # Show some example predictions
    print("\n📊 Sample Predictions (first 10 test examples):")
    print("   Actual  →  Predicted  (Error)")
    for i in range(min(10, len(y_test))):
        actual = y_test.iloc[i]
        pred = predictions[i]
        err = pred - actual
        print(f"   {actual:5.2f}% → {pred:6.2f}%  ({err:+6.2f}%)")


def show_feature_importance(model, feature_names):
    """
    Show which features the model finds most important

    This tells us: "What does the model look at when predicting profit?"
    """

    print("\n" + "=" * 60)
    print("FEATURE IMPORTANCE")
    print("=" * 60)

    print("\n🔍 Which features matter most for profit predictions?")

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
    print("   These patterns help predict how profitable a match will be")


def save_model(model, output_dir='../../models'):
    """
    Save the trained model so we can use it later without retraining
    """

    print("\n" + "=" * 60)
    print("SAVING MODEL")
    print("=" * 60)

    os.makedirs(output_dir, exist_ok=True)
    model_path = f'{output_dir}/model_profit_prediction.pkl'

    # Save using joblib (efficient for sklearn models)
    joblib.dump(model, model_path)

    print(f"\n✓ Model saved to: {model_path}")
    print(f"   You can load it later with: model = joblib.load('{model_path}')")

    return model_path


def main():
    """Main execution"""

    # Step 1: Load data
    X_train, X_test, y_train, y_test = load_prepared_data()

    # Step 2: Analyze profit distribution
    analyze_target_distribution(y_train, y_test)

    # Step 3: Train the model
    model = train_model(X_train, y_train)

    # Step 4: Evaluate performance
    predictions, test_rmse, test_mae, test_r2 = evaluate_model(
        model, X_train, X_test, y_train, y_test
    )

    # Step 5: Detailed error analysis
    detailed_error_analysis(y_test, predictions)

    # Step 6: Show feature importance
    show_feature_importance(model, X_train.columns)

    # Step 7: Save the model
    model_path = save_model(model)

    print("\n" + "=" * 60)
    print("STEP 4 COMPLETE!")
    print("=" * 60)
    print(f"\n✓ Model trained with:")
    print(f"   - RMSE: {test_rmse:.3f}%")
    print(f"   - MAE:  {test_mae:.3f}%")
    print(f"   - R²:   {test_r2:.3f}")
    print(f"✓ Model saved to: {model_path}")
    print("\nNext steps:")
    print("  1. Train Model 3: Home Betting Timing")
    print("  2. Train Model 4: Draw Betting Timing")
    print("  3. Train Model 5: Away Betting Timing")
    print("\n💡 You now have a profit predictor!")
    print("   You can use it to identify high-value arbitrage opportunities")


if __name__ == "__main__":
    main()