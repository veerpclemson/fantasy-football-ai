import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()
DB_URI = os.getenv("DATABASE_URL")
engine = create_engine(DB_URI)

roster_files = [
    "../2025/defense_tendencies_2025_2025.csv",
    "../2025/offense_tendencies_2025_2025.csv",
    "../2025/player_week_data.csv",
    "../2025/games_context_2025_2025.csv",
    "../2025/roster_2025_2025.csv"
]

dfs = []

for file in roster_files:
    print(f"reading file:{file}")
    df = pd.read_csv(file)
    dfs.append(df)

df_defense, df_offense, df_player_week, df_games, df_players = dfs

df_players = df_players[['first_name', 'last_name', 'gsis_id']].drop_duplicates().reset_index(drop=True)
df_players['name'] = df_players['first_name'] + " " + df_players['last_name']

df_players = df_players.rename(columns ={'gsis_id' : 'player_id'})
df_players = df_players.drop(columns = ['first_name', 'last_name'])

df_player_week = df_player_week.merge(
    df_offense,
    how='left',
    on=['season', 'week', 'posteam']
)

df_player_week = df_player_week.merge(
    df_defense,
    how='left',
    on=['season', 'week', 'defteam']
)


df_player_week = df_player_week.merge(
    df_games,
    how='left',
    on=['season', 'week', 'game_id']
)

df_player_week = df_player_week.sort_values(['season', 'week', 'player_id']).reset_index(drop=True)


df_player_week = df_player_week.rename(columns={
    'rush_plays_x': 'rush_plays',
    'rush_plays_y': 'rush_plays_off',
    'total_plays': 'total_plays_off',
    'pass_plays': 'pass_plays_off',
    'pass_pct': 'pass_pct_off',
    'rush_pct': 'rush_pct_off',
    'avg_yards_after_catch': 'avg_yac_off',
    'avg_air_yards': 'avg_air_yards_off',
    'total_pass_plays': 'total_pass_plays',
    'blitz_rate': 'blitz_rate_def',
    'pressure_rate': 'pressure_rate_def',
    'man_coverage_pct': 'man_coverage_pct_def',
    'zone_coverage_pct': 'zone_coverage_pct_def'
})

rolling_cols = [
    "passing_yards", "pass_attempt", "complete_pass", "rush_plays",
    "rushing_yards", "receiving_yards", "reception", "total_touches",
    "rush_inside_10", "rush_inside_20", "target_inside_10", "target_inside_20",
    "total_plays_off", "pass_plays_off", "rush_plays_off", "total_pass_plays",
    "avg_yac_off", "avg_air_yards_off", "fantasy_points",
    "pass_touchdown", "rush_touchdown", "receiving_touchdown",
    "blitz_rate_def", "pressure_rate_def", "man_coverage_pct_def", "zone_coverage_pct_def",
    "pass_pct_off", "rush_pct_off"
]

# Define rolling window size (example: 3 weeks)
window_size = 3

# Sort by player and week for proper rolling
df_player_week = df_player_week.sort_values(['player_id','season','week'])

# Compute rolling sums/means per player
for col in rolling_cols:
    # rolling mean
    df_player_week[f'{col}_rolling_{window_size}'] = (
        df_player_week.groupby('player_id')[col]
        .rolling(window=window_size, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )


df_player_week = df_player_week.merge(
    df_players,
    how='left',
    on=['player_id']
)
df_player_week = df_player_week[df_player_week['player_id'] != '0']
df_player_week.to_csv("../2025/2025_final_data.csv", index=False)


