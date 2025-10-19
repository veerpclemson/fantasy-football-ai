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
    "../2025/pbp_2025_2025.csv",
    "../2025/games_context_2025_2025.csv",
    
]

dfs = []

for file in roster_files:
    print(f"reading file:{file}")
    df = pd.read_csv(file)
    dfs.append(df)

defense_df, offense_df, pbp, games_df = dfs
pbp['passing_yards'] = pbp['receiving_yards'].fillna(0)
pbp['receiving_touchdown'] = pbp['pass_touchdown'].fillna(0)
pbp['rush_inside_10'] = ((pbp['play_type'] == 'run') & (pbp['yardline_100'] <= 10)).astype(int)
pbp['rush_inside_20'] = ((pbp['play_type'] == 'run') & (pbp['yardline_100'] <= 20)).astype(int)
pbp['target_inside_10'] = ((pbp['play_type'] == 'pass') & (pbp['yardline_100'] <= 10)).astype(int)
pbp['target_inside_20'] = ((pbp['play_type'] == 'pass') & (pbp['yardline_100'] <= 20)).astype(int)

# === Step 2: Passers ===
passer_df = pbp[pbp['play_type'] == 'pass'][[
    'season','week','game_id','posteam','defteam','passer_player_id',
    'pass_attempt','complete_pass','passing_yards','pass_touchdown',
    'interception','fumble_lost','target_inside_10','target_inside_20'
]].copy()

passer_df = passer_df.rename(columns={'passer_player_id': 'player_id'})
passer_df.fillna(0, inplace=True)

# Add missing columns
passer_df['rush_plays'] = 0
passer_df['rushing_yards'] = 0
passer_df['rush_touchdown'] = 0
passer_df['reception'] = 0
passer_df['receiving_yards'] = 0
passer_df['receiving_touchdown'] = 0
passer_df['rush_inside_10'] = 0
passer_df['rush_inside_20'] = 0

# === Step 3: Rushers ===
rusher_df = pbp[pbp['play_type'] == 'run'][[
    'season','week','game_id','posteam','defteam','rusher_player_id',
    'rushing_yards','rush_touchdown','fumble_lost',
    'rush_inside_10','rush_inside_20'
]].copy()

rusher_df = rusher_df.rename(columns={'rusher_player_id': 'player_id'})
rusher_df.fillna(0, inplace=True)

# Add missing columns
rusher_df['rush_plays'] = 1
rusher_df['pass_attempt'] = 0
rusher_df['complete_pass'] = 0
rusher_df['passing_yards'] = 0
rusher_df['pass_touchdown'] = 0
rusher_df['interception'] = 0
rusher_df['reception'] = 0
rusher_df['receiving_yards'] = 0
rusher_df['receiving_touchdown'] = 0
rusher_df['target_inside_10'] = 0
rusher_df['target_inside_20'] = 0

# === Step 4: Receivers ===
receiver_df = pbp[pbp['play_type'] == 'pass'][[
    'season','week','game_id','posteam','defteam','receiver_player_id',
    'reception','receiving_yards','receiving_touchdown','fumble_lost',
    'target_inside_10','target_inside_20'
]].copy()

receiver_df = receiver_df.rename(columns={'receiver_player_id': 'player_id'})
receiver_df.fillna(0, inplace=True)

# Add missing columns
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

# === Step 5: Combine all players ===
all_players = pd.concat([passer_df, rusher_df, receiver_df], ignore_index=True)
all_players = all_players.dropna(subset=['player_id'])

# === Step 6: Aggregate per player-week ===
agg_cols = [
    'pass_attempt','complete_pass','passing_yards','pass_touchdown','interception',
    'rush_plays','rushing_yards','rush_touchdown',
    'reception','receiving_yards','receiving_touchdown','fumble_lost',
    'rush_inside_10','rush_inside_20','target_inside_10','target_inside_20'
]

player_week = (
    all_players
    .groupby(['season','week','game_id','posteam','defteam','player_id'], as_index=False)[agg_cols]
    .sum()
)

# === Step 7: Total touches ===
player_week['total_touches'] = (
    player_week['pass_attempt'] + player_week['rush_plays'] + player_week['reception']
)

# === Step 8: Fantasy points (PPR) ===
player_week['fantasy_points'] = (
    0.04 * player_week['passing_yards'] +     
    4 * player_week['pass_touchdown'] -
    2 * player_week['interception'] +
    0.1 * player_week['rushing_yards'] +      
    6 * player_week['rush_touchdown'] +
    0.1 * player_week['receiving_yards'] +    
    6 * player_week['receiving_touchdown'] +  # ✅ added
    1.0 * player_week['reception'] -
    2.0 * player_week['fumble_lost']
)

# === Step 9: Save ===
player_week.to_csv("../2025/player_week_data.csv", index=False)
print("✅ Saved player-week table with receiving touchdowns and accurate stats")

