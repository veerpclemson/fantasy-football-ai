import requests
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()
DB_URI = os.getenv("DATABASE_URL")
engine = create_engine(DB_URI)

