import requests
import pandas as pd
import time
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os


load_dotenv()
DB_URI = os.getenv("DATABASE_URL")
engine = create_engine(DB_URI)


players_url = "https://api.sleeper.app/v1/players/nfl"
res = requests.get(players_url)
res.raise_for_status()
players_data = res.json()
players_df = pd.DataFrame.from_dict(players_data, orient='index').reset_index()
players_df.rename(columns={'index': 'master_player_id'}, inplace=True)
players_df = players_df[['master_player_id', 'full_name', 'team', 'position']]


def fetch_weekly_stats(year: int, week: int):
    url = f"https://api.sleeper.app/v1/stats/nfl/regular/{year}/{week}"
    res = requests.get(url)
    res.raise_for_status()
    data = res.json()

    df = pd.DataFrame(data).T 
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'sleeper_id'}, inplace=True)


    df = df.drop(columns=[col for col in ['player_id', 'master_player_id'] if col in df.columns])

    df['season'] = year
    df['week'] = week
    return df

all_weeks = []


for year in range(2021, 2025):
    for week in range(1, 19):
        try:
            print(f"Fetching {year} Week {week}")
            week_df = fetch_weekly_stats(year, week)

           
            week_df = week_df.merge(
                players_df,
                left_on='sleeper_id',
                right_on='master_player_id',
                how='left'
            )

            all_weeks.append(week_df)
            time.sleep(0.1)  # stay under 1000 calls/min
        except Exception as e:
            print(f"Failed {year} Week {week}: {e}")

# Combine all weeks
weekly_stats_df = pd.concat(all_weeks, ignore_index=True)
weekly_stats_df = weekly_stats_df.copy()


weekly_stats_df['rank'] = weekly_stats_df.groupby(
    ['team', 'position', 'season', 'week']
)['off_snp'].rank(method='first', ascending=False)


def assign_role(row):
    pos = row['position']
    rank = row['rank']
    if pd.isna(rank):
        return pos
    rank = int(rank)
    if pos in ['WR', 'RB', 'TE', 'QB']:
        return f"{pos}{rank}"
    return pos

weekly_stats_df['role'] = weekly_stats_df.apply(assign_role, axis=1)
weekly_stats_df['starter_flag'] = weekly_stats_df['rank'].apply(lambda x: 1 if pd.notna(x) and x <= 2 else 0)


depth_chart_df = weekly_stats_df[['sleeper_id', 'full_name', 'team', 'position',
                                  'season', 'week', 'off_snp', 'role', 'starter_flag']].copy()

depth_chart_df.rename(columns={'sleeper_id': 'player_id'}, inplace=True)


# push to neon
depth_chart_df.to_sql('depth_chart', engine, if_exists='replace', index=False)

print("Depth chart ETL complete!")
