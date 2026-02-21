#!/usr/bin/env python3
"""
Compare Different Arbitrage Betting Strategies

This script runs multiple simulations with different parameters to find
the optimal strategy.
"""

from run_arbitrage_simulation import ArbitrageSimulator
import pandas as pd


def run_strategy_comparison(db_path):
    """Run multiple simulations with different strategies."""

    simulator = ArbitrageSimulator(db_path)

    # Test different minimum profit thresholds
    print("\n" + "="*80)
    print("STRATEGY COMPARISON: Testing Different Profit Thresholds")
    print("="*80)

    strategies = []

    # Test profit thresholds from 0.5% to 3%
    for min_profit in [0.5, 1.0, 1.5, 2.0, 2.5, 3.0]:
        print(f"\nTesting {min_profit}% minimum profit threshold...")
        result = simulator.run_season_simulation(
            season='21/22',
            initial_bankroll=10000,
            bet_amount=100,
            min_profit_pct=min_profit,
            use_timing_models=False
        )

        strategies.append({
            'Strategy': f'Min {min_profit}% profit',
            'ROI': result['roi'],
            'Total Profit': result['total_profit'],
            'Opportunities': result['arbitrage_opportunities_found'],
            'Avg Profit %': result['average_profit_pct'],
            'Max Profit %': result['max_profit_achieved']
        })

    # Test with timing models enabled
    print(f"\nTesting with ML timing models enabled...")
    result_ml = simulator.run_season_simulation(
        season='21/22',
        initial_bankroll=10000,
        bet_amount=100,
        min_profit_pct=1.0,
        use_timing_models=True
    )

    strategies.append({
        'Strategy': 'With ML Timing (1%)',
        'ROI': result_ml['roi'],
        'Total Profit': result_ml['total_profit'],
        'Opportunities': result_ml['arbitrage_opportunities_found'],
        'Avg Profit %': result_ml['average_profit_pct'],
        'Max Profit %': result_ml['max_profit_achieved']
    })

    # Display comparison
    print("\n" + "="*80)
    print("STRATEGY COMPARISON RESULTS")
    print("="*80)
    df = pd.DataFrame(strategies)
    df = df.sort_values('Total Profit', ascending=False)

    print(df.to_string(index=False))
    print(f"\n{'='*80}")

    # Best strategy
    best = df.iloc[0]
    print(f"\n🏆 BEST STRATEGY: {best['Strategy']}")
    print(f"   ROI: {best['ROI']:.2f}%")
    print(f"   Total Profit: £{best['Total Profit']:.2f}")
    print(f"   Opportunities: {best['Opportunities']}")

    return df


def run_multi_season_analysis(db_path):
    """Analyze all seasons."""

    simulator = ArbitrageSimulator(db_path)

    print("\n" + "="*80)
    print("MULTI-SEASON ANALYSIS (1% minimum profit)")
    print("="*80)

    seasons = ['21/22', '22/23', '23/24', '24/25']
    season_results = []

    for season in seasons:
        print(f"\nRunning season {season}...")
        result = simulator.run_season_simulation(
            season=season,
            initial_bankroll=10000,
            bet_amount=100,
            min_profit_pct=1.0,
            use_timing_models=False
        )

        season_results.append({
            'Season': season,
            'ROI': result['roi'],
            'Total Profit': result['total_profit'],
            'Matches': result['total_matches_available'],
            'Opportunities': result['arbitrage_opportunities_found'],
            'Coverage %': (result['arbitrage_opportunities_found']/result['total_matches_available']*100),
            'Avg Profit %': result['average_profit_pct']
        })

    # Display results
    print("\n" + "="*80)
    print("MULTI-SEASON RESULTS")
    print("="*80)
    df = pd.DataFrame(season_results)
    print(df.to_string(index=False))

    # Summary
    print(f"\n{'='*80}")
    print("OVERALL SUMMARY (All 4 Seasons Combined):")
    print(f"  Total Profit: £{df['Total Profit'].sum():.2f}")
    print(f"  Average ROI: {df['ROI'].mean():.2f}%")
    print(f"  Total Opportunities: {df['Opportunities'].sum()}")
    print(f"  Average Coverage: {df['Coverage %'].mean():.1f}%")
    print(f"{'='*80}")

    return df


if __name__ == "__main__":
    db_path = "../../data/raw/epl_arbitrage.db"

    # Part 1: Compare different strategies for season 21/22
    strategy_df = run_strategy_comparison(db_path)

    # Part 2: Analyze all seasons
    season_df = run_multi_season_analysis(db_path)

    print("\n✅ Analysis complete!")
