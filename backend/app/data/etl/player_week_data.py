import pandas as pd

# === Step 1: Load play-by-play data ===
pbp = pd.read_csv("../play_by_play/pbp_2021_2024.csv")

# --- Derive passing_yards first ---
pbp['passing_yards'] = pbp['air_yards'].fillna(0) + pbp['yards_after_catch'].fillna(0)

# === Step 2: Create red zone flags ===
pbp['rush_inside_10'] = ((pbp['play_type'] == 'run') & (pbp['yardline_100'] <= 10)).astype(int)
pbp['rush_inside_20'] = ((pbp['play_type'] == 'run') & (pbp['yardline_100'] <= 20)).astype(int)
pbp['target_inside_10'] = ((pbp['play_type'] == 'pass') & (pbp['yardline_100'] <= 10)).astype(int)
pbp['target_inside_20'] = ((pbp['play_type'] == 'pass') & (pbp['yardline_100'] <= 20)).astype(int)

# === Step 3: Prepare DataFrames by role ===

# --- Passers ---
passer_df = pbp[['season','week','game_id','posteam','defteam','passer_player_id',
                 'pass_attempt','complete_pass','passing_yards','pass_touchdown',
                 'interception','fumble_lost','target_inside_10','target_inside_20']].copy()
passer_df = passer_df.rename(columns={'passer_player_id': 'player_id'})
passer_df.fillna(0, inplace=True)
passer_df['rush_plays'] = 0
passer_df['rushing_yards'] = 0
passer_df['rush_touchdown'] = 0
passer_df['reception'] = 0
passer_df['receiving_yards'] = 0
passer_df['rush_inside_10'] = 0
passer_df['rush_inside_20'] = 0

# --- Rushers ---
rusher_df = pbp[['season','week','game_id','posteam','defteam','rusher_player_id',
                 'rushing_yards','rush_touchdown','fumble_lost',
                 'rush_inside_10','rush_inside_20']].copy()
rusher_df = rusher_df.rename(columns={'rusher_player_id': 'player_id'})
rusher_df.fillna(0, inplace=True)
rusher_df['rush_plays'] = 1
rusher_df['pass_attempt'] = 0
rusher_df['complete_pass'] = 0
rusher_df['passing_yards'] = 0
rusher_df['pass_touchdown'] = 0
rusher_df['interception'] = 0
rusher_df['reception'] = 0
rusher_df['receiving_yards'] = 0
rusher_df['target_inside_10'] = 0
rusher_df['target_inside_20'] = 0

# --- Receivers ---
receiver_df = pbp[['season','week','game_id','posteam','defteam','receiver_player_id',
                   'reception','receiving_yards','fumble_lost',
                   'target_inside_10','target_inside_20']].copy()
receiver_df = receiver_df.rename(columns={'receiver_player_id': 'player_id'})
receiver_df.fillna(0, inplace=True)
receiver_df['pass_attempt'] = 0
receiver_df['complete_pass'] = 0
receiver_df['passing_yards'] = 0
receiver_df['pass_touchdown'] = 0
receiver_df['interception'] = 0
receiver_df['rush_plays'] = 0
receiver_df['rushing_yards'] = 0
receiver_df['rush_touchdown'] = 0
receiver_df['rush_inside_10'] = 0
receiver_df['rush_inside_20'] = 0

# === Step 4: Combine all players ===
all_players = pd.concat([passer_df, rusher_df, receiver_df], ignore_index=True)
all_players = all_players.dropna(subset=['player_id'])

# === Step 5: Aggregate per player-week ===
agg_cols = [
    'pass_attempt','complete_pass','passing_yards','pass_touchdown','interception',
    'rush_plays','rushing_yards','rush_touchdown',
    'reception','receiving_yards','fumble_lost',
    'rush_inside_10','rush_inside_20','target_inside_10','target_inside_20'
]

player_week = (
    all_players
    .groupby(['season','week','game_id','posteam','defteam','player_id'], as_index=False)[agg_cols]
    .sum()
)

# === Step 6: Total touches ===
player_week['total_touches'] = (
    player_week['pass_attempt'] + player_week['rush_plays'] + player_week['reception']
)

# === Step 7: Fantasy points (PPR) ===
player_week['fantasy_points'] = (
    0.04 * player_week['passing_yards'] +     
    4 * player_week['pass_touchdown'] -
    2 * player_week['interception'] +
    0.1 * player_week['rushing_yards'] +      
    6 * player_week['rush_touchdown'] +
    0.1 * player_week['receiving_yards'] +    
    1.0 * player_week['reception'] -
    2.0 * player_week['fumble_lost']
)

# === Step 8: Save ===
player_week.to_csv("../play_by_play/aggregated_player_week_redzone.csv", index=False)
print("✅ Saved player-week table with red zone metrics and defteam")
print(player_week.head(10))
