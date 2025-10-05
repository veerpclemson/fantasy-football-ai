import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()
DB_URI = os.getenv("DATABASE_URL")
engine = create_engine(DB_URI)

pbp = pd.read_sql_table("pbp_full_context", engine)

pass_df = pbp[pbp["passer_player_id"].notna()].copy()
pass_df["player_id"] = pass_df["passer_player_id"]
pass_df["role"] = "passer"

rush_df = pbp[pbp["rusher_player_id"].notna()].copy()
rush_df["player_id"] = rush_df["rusher_player_id"]
rush_df["role"] = "rusher"

recv_df = pbp[pbp["receiver_player_id"].notna()].copy()
recv_df["player_id"] = recv_df["receiver_player_id"]
recv_df["role"] = "receiver"

# combine all into one dataframe
players_df = pd.concat([pass_df, rush_df, recv_df], ignore_index=True)

# aggregate player stats per week
player_weeks = players_df.groupby(
    ["season", "week", "posteam", "player_id", "role"], dropna=False
).agg({
    "fantasy_points": "sum",
    "pass_attempt": "sum",
    "complete_pass": "sum",
    "passing_yards": "sum",
    "pass_touchdown": "sum",
    "rushing_yards": "sum",
    "rush_touchdown": "sum",
    "receiving_yards": "sum",
    "reception": "sum",
    "interception": "sum",
    "fumble_lost": "sum",
    "blitz_rate": "mean",
    "pressure_rate": "mean",
}).reset_index()

# save result
player_weeks.to_sql("player_weeks", engine, if_exists="replace", index=False)

print("✅ player_weeks table created successfully!")

