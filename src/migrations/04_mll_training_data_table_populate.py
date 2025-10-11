#!/usr/bin/env python3
"""
Populate ml_features table from raw odds data
Transforms time-series odds data into ML-ready features with targets
"""

import sqlite3
import numpy as np
from datetime import datetime
from collections import defaultdict


def calculate_trend(values):
    """Calculate linear regression slope for trend analysis"""
    if len(values) < 2:
        return 0.0

    n = len(values)
    x = np.arange(n)
    y = np.array(values)

    # Linear regression: slope = (n*sum(xy) - sum(x)*sum(y)) / (n*sum(x²) - (sum(x))²)
    slope = (n * np.sum(x * y) - np.sum(x) * np.sum(y)) / (n * np.sum(x ** 2) - np.sum(x) ** 2)
    return float(slope)


def get_best_bookmaker_odds(cursor, snapshot_id):
    """Get the best odds from all bookmakers for a given snapshot"""
    cursor.execute('''
    SELECT 
        MAX(home_odds) as best_home,
        MAX(draw_odds) as best_draw,
        MAX(away_odds) as best_away
    FROM bookmaker_odds
    WHERE snapshot_id = ?
    ''', (snapshot_id,))

    result = cursor.fetchone()
    return result if result else (None, None, None)


def create_team_encoding(cursor):
    """Create numeric encoding for team names"""
    cursor.execute("SELECT DISTINCT home_team FROM matches UNION SELECT DISTINCT away_team FROM matches")
    teams = [row[0] for row in cursor.fetchall()]
    team_to_id = {team: idx for idx, team in enumerate(sorted(teams))}
    return team_to_id


def populate_ml_features(db_path='../../data/raw/epl_arbitrage.db'):
    """Main function to populate ml_features table"""

    print("=" * 60)
    print("Populating ML Features Table")
    print("=" * 60)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Clear existing data
        cursor.execute("DELETE FROM ml_features")
        print("Cleared existing ml_features data")

        # Create team encoding
        print("\nCreating team encodings...")
        team_to_id = create_team_encoding(cursor)
        print(f"Encoded {len(team_to_id)} unique teams")

        # Get all matches
        cursor.execute("SELECT match_id, home_team, away_team, season FROM matches ORDER BY commence_time")
        matches = cursor.fetchall()

        print(f"\nProcessing {len(matches)} matches...")

        processed_matches = 0
        total_features_created = 0

        for match_id, home_team, away_team, season in matches:
            # Get all snapshots for this match, ordered by time (earliest first)
            cursor.execute('''
            SELECT snapshot_id, snapshot_time, days_before_match, hours_before_match
            FROM odds_snapshots
            WHERE match_id = ?
            AND days_before_match > 0
            ORDER BY days_before_match DESC
            ''', (match_id,))

            snapshots = cursor.fetchall()

            if len(snapshots) < 2:
                # Need at least 2 snapshots (current + future)
                continue

            # Get best odds for each snapshot
            snapshot_odds = {}
            for snap_id, snap_time, days_before, hours_before in snapshots:
                best_home, best_draw, best_away = get_best_bookmaker_odds(cursor, snap_id)
                if best_home and best_draw and best_away:
                    snapshot_odds[snap_id] = {
                        'time': snap_time,
                        'days_before': days_before,
                        'hours_before': hours_before,
                        'home': best_home,
                        'draw': best_draw,
                        'away': best_away
                    }

            if len(snapshot_odds) < 2:
                continue

            # Process each snapshot (except the last one, which has no future)
            snapshot_list = list(snapshot_odds.items())

            for i in range(len(snapshot_list) - 1):
                current_snap_id, current_data = snapshot_list[i]

                # === CURRENT SNAPSHOT DATA ===
                current_home = current_data['home']
                current_draw = current_data['draw']
                current_away = current_data['away']
                current_combined = (1 / current_home) + (1 / current_draw) + (1 / current_away)

                # === HISTORICAL DATA (all snapshots before current) ===
                historical_home = []
                historical_draw = []
                historical_away = []

                for j in range(i):
                    hist_snap_id, hist_data = snapshot_list[j]
                    historical_home.append(hist_data['home'])
                    historical_draw.append(hist_data['draw'])
                    historical_away.append(hist_data['away'])

                # Calculate historical aggregates
                num_snapshots_seen = len(historical_home)

                if num_snapshots_seen > 0:
                    home_min = min(historical_home)
                    home_max = max(historical_home)
                    home_mean = np.mean(historical_home)
                    home_std = np.std(historical_home) if len(historical_home) > 1 else 0
                    home_trend = calculate_trend(historical_home)

                    draw_min = min(historical_draw)
                    draw_max = max(historical_draw)
                    draw_mean = np.mean(historical_draw)
                    draw_std = np.std(historical_draw) if len(historical_draw) > 1 else 0
                    draw_trend = calculate_trend(historical_draw)

                    away_min = min(historical_away)
                    away_max = max(historical_away)
                    away_mean = np.mean(historical_away)
                    away_std = np.std(historical_away) if len(historical_away) > 1 else 0
                    away_trend = calculate_trend(historical_away)

                    # Recent changes (last 3 snapshots if available)
                    lookback = min(3, num_snapshots_seen)
                    if lookback > 0:
                        home_change_recent = (current_home - historical_home[-lookback]) / historical_home[-lookback]
                        draw_change_recent = (current_draw - historical_draw[-lookback]) / historical_draw[-lookback]
                        away_change_recent = (current_away - historical_away[-lookback]) / historical_away[-lookback]
                    else:
                        home_change_recent = draw_change_recent = away_change_recent = 0
                else:
                    # First snapshot - no history
                    home_min = home_max = home_mean = current_home
                    home_std = home_trend = 0
                    draw_min = draw_max = draw_mean = current_draw
                    draw_std = draw_trend = 0
                    away_min = away_max = away_mean = current_away
                    away_std = away_trend = 0
                    home_change_recent = draw_change_recent = away_change_recent = 0

                # === FUTURE DATA (targets - all snapshots after current) ===
                future_home_odds = []
                future_draw_odds = []
                future_away_odds = []

                for j in range(i + 1, len(snapshot_list)):
                    future_snap_id, future_data = snapshot_list[j]
                    future_home_odds.append(future_data['home'])
                    future_draw_odds.append(future_data['draw'])
                    future_away_odds.append(future_data['away'])

                # Best future odds
                best_future_home = max(future_home_odds)
                best_future_draw = max(future_draw_odds)
                best_future_away = max(future_away_odds)

                # When do best odds appear?
                snapshots_until_best_home = future_home_odds.index(best_future_home) + 1
                snapshots_until_best_draw = future_draw_odds.index(best_future_draw) + 1
                snapshots_until_best_away = future_away_odds.index(best_future_away) + 1

                # Should we bet now?
                home_will_improve = best_future_home > current_home
                draw_will_improve = best_future_draw > current_draw
                away_will_improve = best_future_away > current_away

                # Conservative: only bet now if odds won't improve by more than 1%
                should_bet_home_now = not home_will_improve or (best_future_home / current_home < 1.01)
                should_bet_draw_now = not draw_will_improve or (best_future_draw / current_draw < 1.01)
                should_bet_away_now = not away_will_improve or (best_future_away / current_away < 1.01)

                # Overall arbitrage with best future odds
                future_combined = (1 / best_future_home) + (1 / best_future_draw) + (1 / best_future_away)
                will_have_future_arbitrage = future_combined < 1.0
                max_future_profit_percent = ((1 - future_combined) * 100) if will_have_future_arbitrage else 0
                snapshots_until_arbitrage = max(snapshots_until_best_home, snapshots_until_best_draw,
                                                snapshots_until_best_away)

                # Insert into ml_features
                cursor.execute('''
                INSERT INTO ml_features (
                    match_id, snapshot_time, season,
                    home_team, away_team, days_before_match, hours_before_match,
                    home_odds_current, draw_odds_current, away_odds_current, combined_inverse_current,
                    num_snapshots_seen,
                    home_odds_min_historical, home_odds_max_historical, home_odds_mean_historical,
                    home_odds_std_historical, home_odds_trend,
                    draw_odds_min_historical, draw_odds_max_historical, draw_odds_mean_historical,
                    draw_odds_std_historical, draw_odds_trend,
                    away_odds_min_historical, away_odds_max_historical, away_odds_mean_historical,
                    away_odds_std_historical, away_odds_trend,
                    home_odds_change_recent, draw_odds_change_recent, away_odds_change_recent,
                    home_team_id, away_team_id,
                    should_bet_home_now, home_odds_will_improve, best_future_home_odds, snapshots_until_best_home,
                    should_bet_draw_now, draw_odds_will_improve, best_future_draw_odds, snapshots_until_best_draw,
                    should_bet_away_now, away_odds_will_improve, best_future_away_odds, snapshots_until_best_away,
                    will_have_future_arbitrage, max_future_profit_percent, snapshots_until_arbitrage,
                    created_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                ''', (
                    match_id, current_data['time'], season,
                    home_team, away_team, current_data['days_before'], current_data['hours_before'],
                    current_home, current_draw, current_away, current_combined,
                    num_snapshots_seen,
                    home_min, home_max, home_mean, home_std, home_trend,
                    draw_min, draw_max, draw_mean, draw_std, draw_trend,
                    away_min, away_max, away_mean, away_std, away_trend,
                    home_change_recent, draw_change_recent, away_change_recent,
                    team_to_id.get(home_team, 0), team_to_id.get(away_team, 0),
                    should_bet_home_now, home_will_improve, best_future_home, snapshots_until_best_home,
                    should_bet_draw_now, draw_will_improve, best_future_draw, snapshots_until_best_draw,
                    should_bet_away_now, away_will_improve, best_future_away, snapshots_until_best_away,
                    will_have_future_arbitrage, max_future_profit_percent, snapshots_until_arbitrage,
                    datetime.now().isoformat()
                ))

                total_features_created += 1

            processed_matches += 1
            if processed_matches % 50 == 0:
                print(
                    f"Processed {processed_matches}/{len(matches)} matches, created {total_features_created} feature rows")
                conn.commit()  # Commit periodically

        conn.commit()

        # Final statistics
        cursor.execute("SELECT COUNT(*) FROM ml_features")
        total_rows = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT match_id) FROM ml_features")
        unique_matches = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM ml_features WHERE will_have_future_arbitrage = 1")
        arbitrage_opportunities = cursor.fetchone()[0]

        cursor.execute("SELECT AVG(max_future_profit_percent) FROM ml_features WHERE will_have_future_arbitrage = 1")
        avg_profit = cursor.fetchone()[0]

        print("\n" + "=" * 60)
        print("POPULATION COMPLETE!")
        print("=" * 60)
        print(f"Total feature rows created: {total_rows}")
        print(f"Unique matches: {unique_matches}")
        print(
            f"Rows with future arbitrage: {arbitrage_opportunities} ({100 * arbitrage_opportunities / total_rows:.1f}%)")
        print(f"Average profit when arbitrage exists: {avg_profit:.2f}%")
        print("=" * 60)

        return True

    except Exception as e:
        print(f"\n✗ Error during population: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
        return False

    finally:
        conn.close()


def main():
    """Run the population script"""
    populate_ml_features()


if __name__ == "__main__":
    main()