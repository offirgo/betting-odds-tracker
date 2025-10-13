#!/usr/bin/env python3
"""
Step 2b: Create Profit Threshold Target Files
Convert continuous profit percentages into binary classification targets
for different thresholds (3%, 5%, 7%)
"""

import pandas as pd
import os


def create_threshold_targets(data_dir='../../data/prepared'):
    """
    Read existing profit target files and create binary classification
    targets for multiple thresholds
    """

    print("=" * 60)
    print("Creating Profit Threshold Target Files")
    print("=" * 60)

    # Define thresholds
    thresholds = [3.0, 3.5, 4.0, 5.0, 7.0]

    # Load original profit targets
    print("\n📂 Loading original profit target files...")
    profit_train = pd.read_csv(f'{data_dir}/target_train_profit_percent.csv')
    profit_test = pd.read_csv(f'{data_dir}/target_test_profit_percent.csv')

    print(f"✓ Loaded training targets: {len(profit_train)} rows")
    print(f"✓ Loaded test targets: {len(profit_test)} rows")

    # Analyze original profit distribution
    print("\n" + "=" * 60)
    print("Original Profit Distribution")
    print("=" * 60)
    print(f"\nTraining set profit statistics:")
    print(f"  Mean:   {profit_train['target'].mean():.2f}%")
    print(f"  Median: {profit_train['target'].median():.2f}%")
    print(f"  Min:    {profit_train['target'].min():.2f}%")
    print(f"  Max:    {profit_train['target'].max():.2f}%")

    # Create binary targets for each threshold
    for threshold in thresholds:
        print("\n" + "=" * 60)
        print(f"Creating Binary Targets: Profit > {threshold}%")
        print("=" * 60)

        # Create binary targets
        train_binary = (profit_train['target'] > threshold).astype(int)
        test_binary = (profit_test['target'] > threshold).astype(int)

        # Analyze distribution
        train_positive = train_binary.sum()
        train_total = len(train_binary)
        train_pct = 100 * train_positive / train_total

        test_positive = test_binary.sum()
        test_total = len(test_binary)
        test_pct = 100 * test_positive / test_total

        print(f"\n📊 Training Set:")
        print(f"   Profit > {threshold}% (1): {train_positive:,} ({train_pct:.1f}%)")
        print(f"   Profit ≤ {threshold}% (0): {train_total - train_positive:,} ({100 - train_pct:.1f}%)")

        print(f"\n📊 Test Set:")
        print(f"   Profit > {threshold}% (1): {test_positive:,} ({test_pct:.1f}%)")
        print(f"   Profit ≤ {threshold}% (0): {test_total - test_positive:,} ({100 - test_pct:.1f}%)")

        # Check if data is balanced
        if train_pct > 70 or train_pct < 30:
            if train_pct > 70:
                print(f"\n⚠️  Data is imbalanced: {train_pct:.1f}% positive class")
                print("   Most matches exceed this threshold - consider higher threshold")
            else:
                print(f"\n⚠️  Data is imbalanced: {train_pct:.1f}% positive class")
                print("   Few matches exceed this threshold - might be hard to predict")
        else:
            print(f"\n✓ Data is reasonably balanced ({train_pct:.1f}% / {100 - train_pct:.1f}%)")

        # Save to CSV
        # Handle decimal thresholds in filename
        threshold_label = f"{threshold:.1f}".replace('.', '_')
        train_filename = f'{data_dir}/target_train_profit_gt{threshold_label}pct.csv'
        test_filename = f'{data_dir}/target_test_profit_gt{threshold_label}pct.csv'

        pd.DataFrame({'target': train_binary}).to_csv(train_filename, index=False)
        pd.DataFrame({'target': test_binary}).to_csv(test_filename, index=False)

        print(f"\n✓ Saved: {train_filename}")
        print(f"✓ Saved: {test_filename}")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print("\n✓ Created 10 new target files (5 thresholds × train/test)")
    print("\nFiles created:")
    for threshold in thresholds:
        threshold_label = f"{threshold:.1f}".replace('.', '_')
        print(f"  - target_train_profit_gt{threshold_label}pct.csv")
        print(f"  - target_test_profit_gt{threshold_label}pct.csv")

    print("\n" + "=" * 60)
    print("Threshold Selection Guide")
    print("=" * 60)
    print("\n3.0% threshold:")
    print("  • Use if: You want to catch most profitable opportunities")
    print("  • Trade-off: Less selective, includes moderate-profit matches")

    print("\n3.5% threshold:")
    print("  • Use if: Sweet spot between selectivity and coverage")
    print("  • Trade-off: Should have good class balance (~40/60)")

    print("\n4.0% threshold:")
    print("  • Use if: You want a balanced filter for above-average opportunities")
    print("  • Trade-off: Middle ground, might be getting selective")

    print("\n5.0% threshold:")
    print("  • Use if: You want to focus on high-value opportunities")
    print("  • Trade-off: More selective - catches only good opportunities")

    print("\n7.0% threshold:")
    print("  • Use if: You only want exceptional opportunities")
    print("  • Trade-off: Very selective, might miss some good opportunities")

    print("\n💡 Class Balance Guide:")
    print("   • 40-60%: IDEAL - model can learn both classes well")
    print("   • 30-70%: GOOD - still workable with class_weight='balanced'")
    print("   • 20-80%: CHALLENGING - model might be biased")
    print("   • 10-90%: DIFFICULT - hard to predict minority class")
    print("\n   Start with whichever threshold gives closest to 40-60% split!")


def main():
    """Main execution"""
    create_threshold_targets()

    print("\n" + "=" * 60)
    print("NEXT STEP")
    print("=" * 60)
    print("\n✓ Threshold target files created!")
    print("\nNow you can train classification models:")
    print("  python 04_train_high_profit_classifier.py --threshold 3.0")
    print("  python 04_train_high_profit_classifier.py --threshold 3.5")
    print("  python 04_train_high_profit_classifier.py --threshold 4.0")
    print("  python 04_train_high_profit_classifier.py --threshold 5.0")
    print("  python 04_train_high_profit_classifier.py --threshold 7.0")


if __name__ == "__main__":
    main()