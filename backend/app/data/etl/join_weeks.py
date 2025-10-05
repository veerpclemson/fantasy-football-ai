import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()
DB_URI = os.getenv("DATABASE_URL")
engine = create_engine(DB_URI)

pbp = pd.read_sql_table("plays", engine)
games = pd.read_sql_table("game_context", engine)
off_tend = pd.read_sql_table("offensive_tendencies", engine)
def_tend = pd.read_sql_table("defensive_tendencies", engine)



pbp_full = pbp.merge(games, on="game_id", how="left", suffixes=('', '_games'))
pbp_full = pbp_full.merge(
    def_tend,
    on=['season','week','defteam'],
    how='left',
    suffixes=('', '_def') 
)


pbp_full = pbp_full.merge(
    off_tend,
    on=['season','week','posteam'],
    how='left',
    suffixes=('', '_off')
)

pbp_full.to_sql("pbp_full_context", engine, if_exists="replace", index=False)

