import pandas as pd

# Load player-week table
player_week = pd.read_csv("../play_by_play/aggregated_player_week_redzone.csv")

# Load matchup / games context table
games_context = pd.read_csv("../play_by_play/matchup_tendencies_2021_2024_clean.csv")

# Select the features we want from games_context
context_cols = [
    'season','week','posteam','defteam',
    # Team offensive tendencies
    'total_plays_off','pass_plays_off','rush_plays_off','pass_pct_off','rush_pct_off',
    'red_zone_pass_pct_off','deep_pass_pct_off','avg_air_yards_off','avg_yac_off',
    # Opponent defensive context
    'total_pass_plays','blitz_rate_def','pressure_rate_def','man_coverage_pct_def','zone_coverage_pct_def'
]

games_context = games_context[context_cols]

# Merge by season, week, and posteam (player's team)
player_week = player_week.merge(
    games_context,
    on=['season','week','posteam'],
    how='left'
)

# Fill missing values if needed
player_week.fillna(0, inplace=True)

# Save the merged table
player_week.to_csv("../play_by_play/player_week_with_context.csv", index=False)
print("✅ Player-week table merged with team/opponent context")
print(player_week.head())
