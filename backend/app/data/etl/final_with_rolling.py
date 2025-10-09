import pandas as pd

# === Step 1: Load tables ===
player_week = pd.read_csv("../play_by_play/aggregated_player_week_redzone.csv")
games_context = pd.read_csv("../play_by_play/matchup_tendencies_2021_2024_clean.csv")

# === Step 2: Clean column names ===
player_week.columns = player_week.columns.str.strip()
games_context.columns = games_context.columns.str.strip()

# === Step 3: Merge matchup/game context for each player's team only ===
context_cols = [
    'season','week','posteam','defteam',
    'total_plays_off','pass_plays_off','rush_plays_off','pass_pct_off','rush_pct_off',
    'red_zone_pass_pct_off','deep_pass_pct_off','avg_air_yards_off','avg_yac_off',
    'total_pass_plays','blitz_rate_def','pressure_rate_def','man_coverage_pct_def','zone_coverage_pct_def',
    'spread_line','total_line','over_odds','under_odds','home_moneyline','away_moneyline'
]

games_context = games_context[context_cols]

player_week = player_week.merge(
    games_context,
    on=['season','week','posteam'],
    how='left'
)

# Fill any missing values
player_week.fillna(0, inplace=True)

# === Step 4: Sort for rolling calculations ===
player_week = player_week.sort_values(['player_id','season','week'])

# Columns for rolling averages
rolling_cols = [
    'pass_touchdown','rush_touchdown','reception',
    'rushing_yards','receiving_yards','passing_yards','fantasy_points'
]

# === Step 5: Calculate rolling averages per player (previous 3 weeks) ===
for col in rolling_cols:
    player_week[f'{col}_rolling3'] = (
        player_week
        .groupby(['player_id','season'])[col]
        .rolling(window=3, min_periods=1)
        .mean()
        .reset_index(level=[0,1], drop=True)
    )

# === Step 6: Save final table ===
player_week.to_csv("../play_by_play/player_week_with_context_rolling.csv", index=False)
print("✅ Saved player-week table with context and rolling averages")
print(player_week.head())
