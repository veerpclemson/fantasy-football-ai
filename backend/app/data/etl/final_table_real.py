import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# === Load environment and database ===
load_dotenv()
DB_URI = os.getenv("DATABASE_URL")
engine = create_engine(DB_URI)

# === Step 1: Load the player-week table ===
player_week = pd.read_csv("../play_by_play/final_table_modeling.csv")

# === Step 2: Load play-by-play table ===
pbp = pd.read_sql_table("pbp_full_context", engine)

# === Step 3: Fix passing yards for passers ===
passing_summary = (
    pbp.groupby(["game_id", "passer_player_id"])["passing_yards"]
    .sum()
    .reset_index()
    .rename(columns={"passer_player_id": "player_id", "passing_yards": "passing_yards_sum"})
)

player_week = player_week.merge(
    passing_summary,
    on=["game_id", "player_id"],
    how="left"
)

player_week["passing_yards_sum"] = player_week["passing_yards_sum"].fillna(0)
player_week["passing_yards"] = player_week["passing_yards_sum"]
player_week = player_week.drop(columns=["passing_yards_sum"])

# === Step 4: Calculate receiving TDs for receivers ===
receiving_summary = (
    pbp[pbp["pass_touchdown"] > 0]
    .groupby(["game_id", "receiver_player_id"])["pass_touchdown"]
    .sum()
    .reset_index()  # converts Series to DataFrame
    .rename(columns={"receiver_player_id": "player_id", "pass_touchdown": "receiving_touchdown"})
)



player_week = player_week.merge(
    receiving_summary,
    on=["game_id", "player_id"],
    how="left"
)

player_week["receiving_touchdown"] = player_week["receiving_touchdown"].fillna(0)

# === Step 5: Recalculate fantasy points for everyone ===
player_week["fantasy_points"] = (
    (player_week["passing_yards"] * 0.04) +
    (player_week["pass_touchdown"] * 4) -
    (player_week["interception"] * 2) +
    ((player_week["rushing_yards"] + player_week["receiving_yards"]) * 0.1) +
    ((player_week["rush_touchdown"] + player_week["receiving_touchdown"]) * 6) +
    (player_week["reception"] * 1) -
    (player_week["fumble_lost"] * 2)
)

# === Step 6: Recalculate rolling 3-game averages ===
player_week = player_week.sort_values(["player_id", "season", "week"])

rolling_cols = [
    "pass_touchdown", "rush_touchdown", "reception",
    "rushing_yards", "receiving_yards", "passing_yards", "fantasy_points"
]

for col in rolling_cols:
    player_week[f"{col}_rolling3"] = (
        player_week.groupby("player_id")[col]
        .rolling(3, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )

# === Step 7: Save updated table ===
player_week.to_csv("../play_by_play/player_week_fixed_fantasy.csv", index=False)
print("✅ Updated player-week table with correct passing/receiving stats and rolling averages!")
