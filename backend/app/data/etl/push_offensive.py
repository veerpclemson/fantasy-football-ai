import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()
DB_URI = os.getenv("DATABASE_URL")
engine = create_engine(DB_URI)

roster_files = [
    "../play_by_play/offense_tendencies_2021_2024.csv"
]

dfs = []

for file in roster_files:
    print(f"Reading {file}...")
    df = pd.read_csv(file)

    # Select relevant columns
    df_small = df[[
        
    ]].copy()

    dfs.append(df_small)

roster_all = pd.concat(dfs, ignore_index=True)


print("Saving def tendency to Postgres...")
roster_all.to_sql("offensive_tendencies", engine, if_exists="replace", index=False)
print("Done!")