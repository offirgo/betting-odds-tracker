#!/usr/bin/env python3
"""
Comprehensive Analysis Suite for Arbitrage Betting Strategies

This script performs extensive testing, validation, and optimization:
1. Multi-season analysis
2. Parameter sensitivity testing
3. Outcome-specific performance analysis
4. Timing effectiveness analysis
5. Strategy comparison
6. Statistical validation
7. Risk analysis
"""

import sqlite3
import pandas as pd
import numpy as np
import json
from smart_arbitrage_sim import SmartArbitrageSimulator
from run_arbitrage_simulation import ArbitrageSimulator
from datetime import datetime
try:
    from scipy import stats
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False
    print("Warning: scipy not available, statistical tests will be skipped")


class ComprehensiveAnalyzer:
    """Performs comprehensive analysis of arbitrage strategies."""

    def __init__(self, db_path):
        self.db_path = db_path
        self.smart_sim = SmartArbitrageSimulator(db_path)
        self.simple_sim = ArbitrageSimulator(db_path)

    def analyze_all_seasons(self):
        """Run smart strategy across all 4 seasons."""
        print("\n" + "="*80)
        print("MULTI-SEASON ANALYSIS - SMART STRATEGY")
        print("="*80 + "\n")

        seasons = ['21/22', '22/23', '23/24', '24/25']
        results = []

        for season in seasons:
            print(f"\nRunning season {season}...")
            result = self.smart_sim.run_simulation(
                season=season,
                initial_bankroll=10000,
                bet_amount=100
            )
            results.append(result)

        # Create summary DataFrame
        summary = pd.DataFrame([{
            'Season': r['season'],
            'ROI %': r['roi'],
            'Profit £': r['total_profit'],
            'Matches': r['total_matches'],
            'Arbs': r['arbitrages_completed'],
            'Coverage %': r['coverage'],
            'Avg Profit %': r['avg_profit_pct'],
            'Odds Improved': r['odds_improved_count'],
            'Odds Worsened': r['odds_worsened_count'],
            'Avg Odds Δ %': r['avg_odds_change']
        } for r in results])

        print("\n" + "="*80)
        print("MULTI-SEASON SUMMARY")
        print("="*80)
        print(summary.to_string(index=False))

        # Overall summary
        total_profit = summary['Profit £'].sum()
        avg_roi = summary['ROI %'].mean()
        total_arbs = summary['Arbs'].sum()
        total_matches = summary['Matches'].sum()

        print(f"\n{'='*80}")
        print("OVERALL (4 SEASONS COMBINED):")
        print(f"  Total Profit:        £{total_profit:>10,.2f}")
        print(f"  Average ROI:         {avg_roi:>10.2f}%")
        print(f"  Total Arbitrages:    {total_arbs:>10,}")
        print(f"  Overall Coverage:    {(total_arbs/total_matches*100):>10.1f}%")
        print(f"  Odds Improved:       {summary['Odds Improved'].sum():>10,} times")
        print(f"  Odds Worsened:       {summary['Odds Worsened'].sum():>10,} times")
        print(f"{'='*80}\n")

        return results, summary

    def test_parameter_variations(self, season='21/22'):
        """Test different parameter configurations."""
        print("\n" + "="*80)
        print("PARAMETER SENSITIVITY ANALYSIS")
        print("="*80 + "\n")

        # Test different bet amounts
        print("\n1. BET AMOUNT SENSITIVITY (Season 21/22)")
        print("-" * 80)

        bet_amounts = [50, 100, 150, 200, 250]
        bet_results = []

        for bet_amt in bet_amounts:
            print(f"Testing bet amount: £{bet_amt}")
            result = self.smart_sim.run_simulation(
                season=season,
                initial_bankroll=10000,
                bet_amount=bet_amt
            )
            bet_results.append({
                'Bet Amount': bet_amt,
                'ROI %': result['roi'],
                'Total Profit': result['total_profit'],
                'Arbs Completed': result['arbitrages_completed'],
                'Avg Profit/Bet': result['avg_profit']
            })

        bet_df = pd.DataFrame(bet_results)
        print("\n" + bet_df.to_string(index=False))

        # Test different bankroll sizes
        print("\n\n2. BANKROLL SIZE IMPACT (Season 21/22)")
        print("-" * 80)

        bankroll_configs = [
            (5000, 50),
            (10000, 100),
            (20000, 200),
            (50000, 500),
            (100000, 1000)
        ]
        bankroll_results = []

        for bankroll, bet_amt in bankroll_configs:
            print(f"Testing bankroll £{bankroll:,} with £{bet_amt} bets")
            result = self.smart_sim.run_simulation(
                season=season,
                initial_bankroll=bankroll,
                bet_amount=bet_amt
            )
            bankroll_results.append({
                'Bankroll': f'£{bankroll:,}',
                'Bet Size': f'£{bet_amt}',
                'ROI %': result['roi'],
                'Total Profit': result['total_profit'],
                'Arbs': result['arbitrages_completed']
            })

        bankroll_df = pd.DataFrame(bankroll_results)
        print("\n" + bankroll_df.to_string(index=False))

        return bet_df, bankroll_df

    def analyze_outcome_performance(self, season='21/22'):
        """Analyze which outcomes (home/draw/away) benefit most from waiting."""
        print("\n" + "="*80)
        print("OUTCOME-SPECIFIC PERFORMANCE ANALYSIS")
        print("="*80 + "\n")

        result = self.smart_sim.run_simulation(
            season=season,
            initial_bankroll=10000,
            bet_amount=100
        )

        bets = result['bets']

        # Group by which outcome was waited for (bet3)
        by_outcome = {
            'home': [b for b in bets if b['bet3_outcome'] == 'home'],
            'draw': [b for b in bets if b['bet3_outcome'] == 'draw'],
            'away': [b for b in bets if b['bet3_outcome'] == 'away']
        }

        outcome_stats = []
        for outcome, outcome_bets in by_outcome.items():
            if outcome_bets:
                odds_changes = [b['odds_change_pct'] for b in outcome_bets]
                profits = [b['profit_pct'] for b in outcome_bets]
                improved = len([b for b in outcome_bets if b['odds_change_pct'] > 0])
                worsened = len([b for b in outcome_bets if b['odds_change_pct'] < 0])

                outcome_stats.append({
                    'Outcome': outcome.capitalize(),
                    'Count': len(outcome_bets),
                    'Avg Odds Δ %': np.mean(odds_changes),
                    'Median Odds Δ %': np.median(odds_changes),
                    'Max Odds Δ %': np.max(odds_changes),
                    'Improved': improved,
                    'Worsened': worsened,
                    'Avg Profit %': np.mean(profits),
                    'Success Rate %': (improved / len(outcome_bets) * 100)
                })

        outcome_df = pd.DataFrame(outcome_stats)
        print(outcome_df.to_string(index=False))

        print(f"\n{'='*80}")
        print("KEY INSIGHTS:")
        best_outcome = outcome_df.loc[outcome_df['Avg Odds Δ %'].idxmax()]
        print(f"  Best outcome to wait for: {best_outcome['Outcome']}")
        print(f"  Average odds improvement: {best_outcome['Avg Odds Δ %']:.2f}%")
        print(f"  Success rate: {best_outcome['Success Rate %']:.1f}%")
        print(f"{'='*80}\n")

        return outcome_df, bets

    def analyze_timing_effectiveness(self, season='21/22'):
        """Analyze how timing affects results."""
        print("\n" + "="*80)
        print("TIMING EFFECTIVENESS ANALYSIS")
        print("="*80 + "\n")

        result = self.smart_sim.run_simulation(
            season=season,
            initial_bankroll=10000,
            bet_amount=100
        )

        bets = result['bets']

        # Calculate wait duration
        wait_durations = []
        for bet in bets:
            wait_days = bet['days_before_start'] - bet['days_before_complete']
            wait_durations.append({
                'wait_days': wait_days,
                'odds_change_pct': bet['odds_change_pct'],
                'profit_pct': bet['profit_pct'],
                'outcome': bet['bet3_outcome']
            })

        wait_df = pd.DataFrame(wait_durations)

        # Timing statistics
        print("WAIT DURATION STATISTICS:")
        print(f"  Average wait: {wait_df['wait_days'].mean():.2f} days")
        print(f"  Median wait: {wait_df['wait_days'].median():.2f} days")
        print(f"  Min wait: {wait_df['wait_days'].min():.2f} days")
        print(f"  Max wait: {wait_df['wait_days'].max():.2f} days")

        # Correlation analysis
        corr_wait_odds = wait_df['wait_days'].corr(wait_df['odds_change_pct'])
        corr_wait_profit = wait_df['wait_days'].corr(wait_df['profit_pct'])

        print(f"\nCORRELATION ANALYSIS:")
        print(f"  Wait time vs Odds improvement: {corr_wait_odds:.3f}")
        print(f"  Wait time vs Profit: {corr_wait_profit:.3f}")

        # Bucket analysis by wait time
        wait_df['wait_bucket'] = pd.cut(wait_df['wait_days'],
                                         bins=[0, 1, 3, 7, 14, 30],
                                         labels=['<1 day', '1-3 days', '3-7 days', '7-14 days', '>14 days'])

        bucket_stats = wait_df.groupby('wait_bucket').agg({
            'odds_change_pct': ['mean', 'count'],
            'profit_pct': 'mean'
        }).round(2)

        print(f"\nPERFORMANCE BY WAIT DURATION:")
        print(bucket_stats)

        print(f"\n{'='*80}\n")

        return wait_df

    def compare_strategies(self, season='21/22'):
        """Compare different betting strategies."""
        print("\n" + "="*80)
        print("STRATEGY COMPARISON")
        print("="*80 + "\n")

        strategies_results = []

        # 1. Simple strategy (immediate betting, 1% threshold)
        print("Running: Simple Strategy (1% threshold)...")
        simple_1pct = self.simple_sim.run_season_simulation(
            season=season,
            initial_bankroll=10000,
            bet_amount=100,
            min_profit_pct=1.0,
            use_timing_models=False
        )

        strategies_results.append({
            'Strategy': 'Simple (1%)',
            'Description': 'Bet immediately on any 1%+ arbitrage',
            'ROI %': simple_1pct['roi'],
            'Profit': simple_1pct['total_profit'],
            'Opportunities': simple_1pct['arbitrage_opportunities_found'],
            'Avg Profit %': simple_1pct['average_profit_pct']
        })

        # 2. Simple strategy (0.5% threshold)
        print("Running: Simple Strategy (0.5% threshold)...")
        simple_05pct = self.simple_sim.run_season_simulation(
            season=season,
            initial_bankroll=10000,
            bet_amount=100,
            min_profit_pct=0.5,
            use_timing_models=False
        )

        strategies_results.append({
            'Strategy': 'Simple (0.5%)',
            'Description': 'Bet immediately on any 0.5%+ arbitrage',
            'ROI %': simple_05pct['roi'],
            'Profit': simple_05pct['total_profit'],
            'Opportunities': simple_05pct['arbitrage_opportunities_found'],
            'Avg Profit %': simple_05pct['average_profit_pct']
        })

        # 3. Smart strategy
        print("Running: Smart Strategy...")
        smart = self.smart_sim.run_simulation(
            season=season,
            initial_bankroll=10000,
            bet_amount=100
        )

        strategies_results.append({
            'Strategy': 'Smart (ML)',
            'Description': 'Bet 2 highest, wait for ML signal on 3rd',
            'ROI %': smart['roi'],
            'Profit': smart['total_profit'],
            'Opportunities': smart['arbitrages_completed'],
            'Avg Profit %': smart['avg_profit_pct']
        })

        comparison_df = pd.DataFrame(strategies_results)
        print("\n" + "="*80)
        print("STRATEGY COMPARISON RESULTS")
        print("="*80)
        print(comparison_df[['Strategy', 'ROI %', 'Profit', 'Opportunities', 'Avg Profit %']].to_string(index=False))

        # Calculate improvements
        baseline_roi = simple_1pct['roi']
        baseline_profit = simple_1pct['total_profit']

        print(f"\n{'='*80}")
        print("SMART STRATEGY VS SIMPLE (1%) BASELINE:")
        roi_improvement = ((smart['roi'] - baseline_roi) / baseline_roi) * 100
        profit_improvement = ((smart['total_profit'] - baseline_profit) / baseline_profit) * 100

        print(f"  ROI Improvement: {roi_improvement:>10.1f}%")
        print(f"  Profit Improvement: {profit_improvement:>10.1f}%")
        print(f"  Additional Profit: £{smart['total_profit'] - baseline_profit:>10.2f}")
        print(f"{'='*80}\n")

        return comparison_df

    def statistical_validation(self, seasons=['21/22', '22/23', '23/24', '24/25']):
        """Perform statistical validation of the smart strategy."""
        print("\n" + "="*80)
        print("STATISTICAL VALIDATION")
        print("="*80 + "\n")

        smart_results = []
        simple_results = []

        for season in seasons:
            print(f"Analyzing season {season}...")

            # Smart strategy
            smart = self.smart_sim.run_simulation(
                season=season,
                initial_bankroll=10000,
                bet_amount=100
            )
            smart_results.append({
                'season': season,
                'roi': smart['roi'],
                'profit': smart['total_profit'],
                'avg_profit_pct': smart['avg_profit_pct'],
                'n_bets': smart['arbitrages_completed'],
                'individual_profits': [b['profit_pct'] for b in smart['bets']]
            })

            # Simple strategy
            simple = self.simple_sim.run_season_simulation(
                season=season,
                initial_bankroll=10000,
                bet_amount=100,
                min_profit_pct=1.0,
                use_timing_models=False
            )
            simple_results.append({
                'season': season,
                'roi': simple['roi'],
                'profit': simple['total_profit'],
                'avg_profit_pct': simple['average_profit_pct'],
                'n_bets': simple['arbitrage_opportunities_found']
            })

        # Statistical tests
        smart_rois = [r['roi'] for r in smart_results]
        simple_rois = [r['roi'] for r in simple_results]

        smart_profits = [r['profit'] for r in smart_results]
        simple_profits = [r['profit'] for r in simple_results]

        print("\nSTATISTICAL COMPARISON (Smart vs Simple):")
        print(f"  Smart ROI: {np.mean(smart_rois):.2f}% ± {np.std(smart_rois):.2f}%")
        print(f"  Simple ROI: {np.mean(simple_rois):.2f}% ± {np.std(simple_rois):.2f}%")

        # T-test (if scipy available)
        if HAS_SCIPY:
            t_stat, p_value = stats.ttest_rel(smart_rois, simple_rois)
            print(f"  T-statistic: {t_stat:.3f}")
            print(f"  P-value: {p_value:.4f}")

            if p_value < 0.05:
                print(f"  ✓ Statistically significant difference (p < 0.05)")
            else:
                print(f"  ✗ No statistically significant difference (p >= 0.05)")
        else:
            print(f"  (Statistical tests skipped - scipy not available)")

        # Consistency analysis
        smart_cv = np.std(smart_rois) / np.mean(smart_rois)  # Coefficient of variation
        simple_cv = np.std(simple_rois) / np.mean(simple_rois)

        print(f"\nCONSISTENCY ANALYSIS:")
        print(f"  Smart Strategy CV: {smart_cv:.3f} (lower is more consistent)")
        print(f"  Simple Strategy CV: {simple_cv:.3f}")

        # Win rate
        smart_wins = sum(1 for r in smart_results if r['roi'] > 0)
        simple_wins = sum(1 for r in simple_results if r['roi'] > 0)

        print(f"\nWIN RATE (Positive ROI seasons):")
        print(f"  Smart Strategy: {smart_wins}/{len(seasons)} ({smart_wins/len(seasons)*100:.0f}%)")
        print(f"  Simple Strategy: {simple_wins}/{len(seasons)} ({simple_wins/len(seasons)*100:.0f}%)")

        print(f"\n{'='*80}\n")

        return smart_results, simple_results

    def risk_analysis(self, season='21/22'):
        """Analyze risk characteristics of the strategy."""
        print("\n" + "="*80)
        print("RISK ANALYSIS")
        print("="*80 + "\n")

        result = self.smart_sim.run_simulation(
            season=season,
            initial_bankroll=10000,
            bet_amount=100
        )

        bets = result['bets']
        profits = [b['profit_pct'] for b in bets]

        print("PROFIT DISTRIBUTION:")
        print(f"  Mean: {np.mean(profits):.2f}%")
        print(f"  Median: {np.median(profits):.2f}%")
        print(f"  Std Dev: {np.std(profits):.2f}%")
        print(f"  Min: {np.min(profits):.2f}%")
        print(f"  Max: {np.max(profits):.2f}%")
        print(f"  25th percentile: {np.percentile(profits, 25):.2f}%")
        print(f"  75th percentile: {np.percentile(profits, 75):.2f}%")

        # Simulate bankroll progression
        bankroll_history = [10000]
        current_bankroll = 10000

        for bet in bets:
            current_bankroll -= bet['total_stake']
            current_bankroll += bet['total_stake'] * (1 + bet['profit_pct'] / 100)
            bankroll_history.append(current_bankroll)

        max_bankroll = max(bankroll_history)
        max_drawdown = max(max_bankroll - b for b in bankroll_history)
        max_drawdown_pct = (max_drawdown / max_bankroll) * 100

        print(f"\nBANKROLL ANALYSIS:")
        print(f"  Starting: £{bankroll_history[0]:,.2f}")
        print(f"  Peak: £{max_bankroll:,.2f}")
        print(f"  Final: £{bankroll_history[-1]:,.2f}")
        print(f"  Max Drawdown: £{max_drawdown:.2f} ({max_drawdown_pct:.2f}%)")

        # Sharpe-like ratio (assuming risk-free rate of 0 for simplicity)
        sharpe = np.mean(profits) / np.std(profits) if np.std(profits) > 0 else 0
        print(f"\nRISK-ADJUSTED METRICS:")
        print(f"  Sharpe Ratio (simplified): {sharpe:.3f}")

        print(f"\n{'='*80}\n")

        return {
            'profits': profits,
            'bankroll_history': bankroll_history,
            'max_drawdown': max_drawdown,
            'max_drawdown_pct': max_drawdown_pct,
            'sharpe': sharpe
        }


if __name__ == "__main__":
    db_path = "../../data/raw/epl_arbitrage.db"

    print("\n" + "="*80)
    print("COMPREHENSIVE ARBITRAGE STRATEGY ANALYSIS")
    print("="*80)

    analyzer = ComprehensiveAnalyzer(db_path)

    # 1. Multi-season analysis
    print("\n\n### PART 1: MULTI-SEASON ANALYSIS ###")
    season_results, season_summary = analyzer.analyze_all_seasons()

    # 2. Parameter testing
    print("\n\n### PART 2: PARAMETER SENSITIVITY ###")
    bet_df, bankroll_df = analyzer.test_parameter_variations()

    # 3. Outcome analysis
    print("\n\n### PART 3: OUTCOME-SPECIFIC ANALYSIS ###")
    outcome_df, bets_data = analyzer.analyze_outcome_performance()

    # 4. Timing analysis
    print("\n\n### PART 4: TIMING EFFECTIVENESS ###")
    timing_df = analyzer.analyze_timing_effectiveness()

    # 5. Strategy comparison
    print("\n\n### PART 5: STRATEGY COMPARISON ###")
    strategy_comparison = analyzer.compare_strategies()

    # 6. Statistical validation
    print("\n\n### PART 6: STATISTICAL VALIDATION ###")
    smart_stats, simple_stats = analyzer.statistical_validation()

    # 7. Risk analysis
    print("\n\n### PART 7: RISK ANALYSIS ###")
    risk_metrics = analyzer.risk_analysis()

    # Save all results
    print("\n\nSaving comprehensive analysis results...")
    results = {
        'timestamp': datetime.now().isoformat(),
        'season_summary': season_summary.to_dict('records'),
        'outcome_analysis': outcome_df.to_dict('records'),
        'strategy_comparison': strategy_comparison.to_dict('records'),
        'risk_metrics': {
            'max_drawdown': risk_metrics['max_drawdown'],
            'max_drawdown_pct': risk_metrics['max_drawdown_pct'],
            'sharpe': risk_metrics['sharpe']
        }
    }

    with open('../../data/comprehensive_analysis.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)

    print("\n" + "="*80)
    print("ANALYSIS COMPLETE!")
    print("Results saved to: ../../data/comprehensive_analysis.json")
    print("="*80 + "\n")
