import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()
DB_URI = os.getenv("DATABASE_URL")
engine = create_engine(DB_URI)

finals = pd.read_csv("../play_by_play/player_week_fixed_fantasy.csv")

finals.to_sql("final_modeling_data", engine, if_exists="replace", index=False)