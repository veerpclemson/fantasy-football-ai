import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
load_dotenv()
DB_URI = os.getenv("DATABASE_URL")
engine = create_engine(DB_URI)
# === Step 1: Load the final player-week table with rolling averages ===
player_week = pd.read_csv("../play_by_play/player_week_modeling_features.csv")

# === Step 2: Player efficiency ratios (avoid divide by zero) ===
#player_week['pass_td_per_attempt'] = player_week['pass_touchdown_rolling3'] / player_week['pass_attempt'].replace(0, 1)
#player_week['rush_td_per_carry'] = player_week['rush_touchdown_rolling3'] / player_week['rush_plays'].replace(0, 1)
#player_week['rec_yards_per_target'] = player_week['receiving_yards_rolling3'] / player_week['reception_rolling3'].replace(0, 1)
#player_week['pass_yards_per_attempt'] = player_week['passing_yards_rolling3'] / player_week['pass_attempt'].replace(0, 1)

# === Step 3: Target variable: next week's fantasy points ===
#player_week = player_week.sort_values(['player_id','season','week'])
#player_week['fantasy_points_next_week'] = player_week.groupby('player_id')['fantasy_points'].shift(-1)

# === Step 4: Save final table for modeling ===
#player_week.to_csv("../play_by_play/player_week_modeling_features.csv", index=False)
#print("✅ Player-week table with efficiency ratios and next-week target saved")
#print(player_week.head(10))
player_week.to_sql("final_dataset_all_stats", engine, if_exists="replace", index=False)
