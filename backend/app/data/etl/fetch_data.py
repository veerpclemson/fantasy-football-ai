import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os


load_dotenv()
DB_URI = os.getenv("DATABASE_URL")
engine = create_engine(DB_URI)

csv_files = [
    "play_by_play/pbp_2021_2024.csv"
]

dfs = []

for file in csv_files:
    print(f"Reading {file}...")
    df = pd.read_csv(file)

    #get columns
    df_small = df[[
        "game_id", "season", "week", "posteam", "defteam", "play_type",
        "down", "ydstogo", "yardline_100", "passer_player_id",
        "rusher_player_id", "receiver_player_id", "air_yards", "yards_after_catch", "rushing_yards",
        "pass_touchdown", "rush_touchdown", "return_touchdown",
        "interception", "fumble_lost", "pass_attempt", "complete_pass"
    ]].copy()

    
    df_small["reception"] = df_small["complete_pass"].apply(lambda x: 1 if x == 1 else 0)
    df_small["receiving_yards"] = df_small["air_yards"] + df_small["yards_after_catch"]
    df_small["passing_yards"] = df_small.apply(
        lambda row: row["air_yards"] + row["yards_after_catch"] if row["complete_pass"] == 1 else 0,
        axis=1
    )

    # Fantasy points calculation
    def calc_fantasy_points(row):
        points = 0

        # QB passing points
        if row["passer_player_id"]:
            points += row["pass_touchdown"] * 4
            points += row["passing_yards"] / 25
            points -= row["interception"] * 2
            points -= row["fumble_lost"] * 2

        # Rushing points 
        if row["rusher_player_id"]:
            points += row["rush_touchdown"] * 6
            points += row["rushing_yards"] / 10
            points -= row["fumble_lost"] * 2

        # Receiving points 
        if row["receiver_player_id"]:
            points += row["receiving_yards"] / 10
            points += row["reception"]        # PPR
            points += row["return_touchdown"] * 6

        return points

    df_small["fantasy_points"] = df_small.apply(calc_fantasy_points, axis=1)
    dfs.append(df_small)


plays_all = pd.concat(dfs, ignore_index=True)

print("Saving to Postgres...")
plays_all.to_sql("plays", engine, if_exists="replace", index=False)
print("Done!")

roster_files = [
    "play_by_play/roster_2021_2024.csv"
]

dfs = []

for file in roster_files:
    print(f"Reading {file}...")
    df = pd.read_csv(file)

    # Select relevant columns
    df_small = df[[
        "gsis_id", "full_name", "team", "position", "height", "weight", "birth_date"
    ]].copy()

    dfs.append(df_small)

roster_all = pd.concat(dfs, ignore_index=True)


print("Saving roster to Postgres...")
roster_all.to_sql("players", engine, if_exists="replace", index=False)
print("Done!")
