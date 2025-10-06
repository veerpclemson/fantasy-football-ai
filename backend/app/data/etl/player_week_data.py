import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()
DB_URI = os.getenv("DATABASE_URL")
engine = create_engine(DB_URI)

# Load play-by-play data
pbp = pd.read_sql_table("pbp_full_context", engine)

# --- Create role-specific subsets ---
pass_df = pbp[pbp["passer_player_id"].notna()].copy()
pass_df["player_id"] = pass_df["passer_player_id"]
pass_df["role"] = "passer"

rush_df = pbp[pbp["rusher_player_id"].notna()].copy()
rush_df["player_id"] = rush_df["rusher_player_id"]
rush_df["role"] = "rusher"

recv_df = pbp[pbp["receiver_player_id"].notna()].copy()
recv_df["player_id"] = recv_df["receiver_player_id"]
recv_df["role"] = "receiver"

# Combine all
players_df = pd.concat([pass_df, rush_df, recv_df], ignore_index=True)

# --- Aggregate per player per week ---
player_weeks = players_df.groupby(
    ["season", "week", "posteam", "player_id", "role"], dropna=False
).agg({
    "fantasy_points": "sum",
    "pass_attempt": "sum",
    "complete_pass": "sum",
    "passing_yards": "sum",
    "pass_touchdown": "sum",
    "interception": "sum",
    "rushing_yards": "sum",
    "rush_plays": "sum",
    "rush_touchdown": "sum",
    "receiving_yards": "sum",
    "reception": "sum",
    "fumble_lost": "sum",
    "blitz_rate": "mean",
    "pressure_rate": "mean",
    "man_coverage_pct": "mean",
    "zone_coverage_pct": "mean",
}).reset_index()

# --- Add opportunity counts ---
player_weeks["total_touches"] = (
    player_weeks["rush_plays"].fillna(0)
    + player_weeks["reception"].fillna(0)
    + player_weeks["pass_attempt"].fillna(0)
)

# --- Merge game context ---
# --- Merge game context ---
game_file = "../play_by_play/games_context_2021_2024.csv"
try:
    games_context = pd.read_csv(game_file)
    
    # Compute opponent, home_away, rest_days per posteam
    games_context['rest_days_home'] = games_context['home_rest']
    games_context['rest_days_away'] = games_context['away_rest']
    
    # Merge player_weeks with games_context on season/week
    player_weeks = player_weeks.merge(
        games_context,
        left_on=['season','week'],
        right_on=['season','week'],
        how='left'
    )
    
    # Now compute opponent, home_away, rest_days
    player_weeks['opponent'] = player_weeks.apply(
        lambda x: x['away_team'] if x['posteam'] == x['home_team'] else x['home_team'], axis=1
    )
    player_weeks['home_away'] = player_weeks.apply(
        lambda x: 'home' if x['posteam'] == x['home_team'] else 'away', axis=1
    )
    player_weeks['rest_days'] = player_weeks.apply(
        lambda x: x['home_rest'] if x['posteam'] == x['home_team'] else x['away_rest'], axis=1
    )
    
    # Keep only relevant columns from games_context
    keep_cols = ['home_away','opponent','spread_line','total_line','rest_days','roof','surface','temp','wind','stadium_id']
    player_weeks = player_weeks.drop(columns=['home_team','away_team','home_rest','away_rest','over_odds','under_odds','home_score','away_score','gametime','weekday','gameday','game_date'], errors='ignore')
    
except Exception as e:
    print("⚠️ games_context not found, skipping merge:", e)

# --- Merge opponent defensive tendencies ---
def_file = "../play_by_play/defense_tendencies_2021_2024.csv"
try:
    def_tend = pd.read_csv(def_file)
    player_weeks = player_weeks.merge(
        def_tend[['season','week','defteam','blitz_rate','pressure_rate','man_coverage_pct','zone_coverage_pct']],
        left_on=['season','week','opponent'],
        right_on=['season','week','defteam'],
        how='left'
    ).drop(columns=['defteam'])
except Exception as e:
    print("⚠️ defensive_tendencies not found, skipping merge:", e)


# --- Fill missing values ---
player_weeks = player_weeks.fillna(0)

# --- Create lagged rolling features (last 1–3 weeks per player) ---
lag_cols = ["fantasy_points", "total_touches", "passing_yards", "rushing_yards", "receiving_yards"]
player_weeks = player_weeks.sort_values(["player_id", "season", "week"])
for col in lag_cols:
    for lag in range(1, 4):  # 1-,2-,3-week lag
        player_weeks[f"{col}_lag{lag}"] = player_weeks.groupby("player_id")[col].shift(lag)
        player_weeks[f"{col}_lag{lag}"] = player_weeks[f"{col}_lag{lag}"].fillna(0)

# Save to Neon
player_weeks.to_sql("player_weeks", engine, if_exists="replace", index=False)

print("✅ player_weeks table created successfully with context and lagged features!")
