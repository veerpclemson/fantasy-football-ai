import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()
DB_URI = os.getenv("DATABASE_URL")
engine = create_engine(DB_URI)

roster_files = [
    "../play_by_play/games_context_2021_2024.csv"
]

dfs = []

for file in roster_files:
    print(f"Reading {file}...")
    df = pd.read_csv(file)

    # Select relevant columns
    df_small = df[[
        "game_id","season","week","gameday","weekday","gametime","home_team","away_team","home_score","away_score","home_rest","away_rest","spread_line","total_line","over_odds","under_odds","home_moneyline","away_moneyline","roof","surface","temp","wind","stadium_id","game_date" 
    ]].copy()

    dfs.append(df_small)

roster_all = pd.concat(dfs, ignore_index=True)


print("Saving game context to Postgres...")
roster_all.to_sql("game_context", engine, if_exists="replace", index=False)
print("Done!")