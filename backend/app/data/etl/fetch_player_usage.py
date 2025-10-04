#import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()
DB_URI = os.getenv("DATABASE_URL")
engine = create_engine(DB_URI)
#store all datasets in heere per year
dfs = []

for i in range(2021, 2025):
    url = f"https://www.pro-football-reference.com/years/{i}/fantasy.htm"
    tables = pd.read_html(url, header=1)
    usage_df = tables[0]
    
    usage_df.columns = usage_df.columns.get_level_values(0)
    
    usage_df = usage_df[usage_df['Rk'] != 'Rk'].copy()  # Make an explicit copy
    usage_df.loc[:, "season"] = i

    
    usage_df = usage_df.reset_index(drop=True)
     

    dfs.append(usage_df)
#merge them all into one dataset
usage_all = pd.concat(dfs, ignore_index=True)

usage_all.to_sql("player_usage", engine, if_exists = "replace", index = False)
