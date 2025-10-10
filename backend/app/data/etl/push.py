import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
DB_URI = os.getenv("DATABASE_URL")
engine = create_engine(DB_URI)

# Load dataset
finals = pd.read_csv("../play_by_play/player_week_fixed_fantasy.csv")

# Sort by player and week to ensure correct rolling calculation
finals = finals.sort_values(by=["player_id", "season", "week"])

# Add 3-week rolling average of receiving touchdowns
finals["receiving_touchdown_rolling3"] = (
    finals.groupby("player_id")["receiving_touchdown"]
    .rolling(window=3, min_periods=1)
    .mean()
    .reset_index(level=0, drop=True)
)

# Save to database
finals.to_sql("final_modeling_data", engine, if_exists="replace", index=False)
