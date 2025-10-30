import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score

# -------------------------
# Load environment and connect
# -------------------------
load_dotenv()
DB_URI = os.getenv("DATABASE_URL")
engine = create_engine(DB_URI)

# -------------------------
# Load historical and 2025 data separately
# -------------------------
df = pd.read_sql_table("final_modeling_data", engine)
df2025 = pd.read_sql_table("2025_updated", engine)  # weeks 1-7 only

# Keep only rows with valid passing yards
df = df[df["receiving_yards"].notna()]
df2025 = df2025[df2025["receiving_yards"].notna()]

df.columns = [str(c) for c in df.columns]
df2025.columns = [str(c) for c in df2025.columns]

# Save names for 2025 players
names_map = df2025[["player_id", "name"]].drop_duplicates().set_index("player_id")["name"]

# -------------------------
# Combine for rolling calculations
# -------------------------
combined = pd.concat([df, df2025], ignore_index=True).sort_values(["player_id", "season", "week"])

# -------------------------
# Rolling features
# -------------------------
rolling_features = [
    "blitz_rate_def", "pressure_rate_def", "man_coverage_pct_def",
    "zone_coverage_pct_def", "avg_yac_off", "total_touches",
    "pass_pct_off", "rush_pct_off", "pass_attempt", "complete_pass", "avg_air_yards_off"
]
window = 3

rolling_cols = []

for col in rolling_features:
    if col in ["blitz_rate_def", "pressure_rate_def", "man_coverage_pct_def"]:
        rolled = combined.groupby("defteam_x")[col].shift(1).rolling(window, min_periods=1).mean()
    else:
        rolled = combined.groupby("player_id")[col].shift(1).rolling(window, min_periods=1).mean()
    rolled.name = col + f"_rolling{window}"
    rolling_cols.append(rolled)

combined = pd.concat([combined] + rolling_cols, axis=1)

# -------------------------
# Prepare training and test sets
# -------------------------
target = "receiving_yards"

# Leak columns to drop
leak_cols = [
    "passing_yards", "pass_attempt", "complete_pass", "rush_plays",
    "receiving_yards", "reception", "total_touches", "rush_inside_10", "rush_inside_20", "target_inside_10", "target_inside_20",
    "total_plays_off", "pass_plays_off", "rush_plays_off", "total_pass_plays", "avg_yac_off", "avg_air_yards_off",
    "fantasy_points", "pass_touchdown", "rush_touchdown", "receiving_touchdown","blitz_rate_def",
    "pressure_rate_def","man_coverage_pct_def","zone_coverage_pct_def","pass_pct_off",
    "rush_pct_off"
]

# Train on all historical + first 7 weeks of 2025
train = combined[(combined["season"] < 2025) | ((combined["season"] == 2025) & (combined["week"] <= 8))]
X_train = train.drop(columns=[target, "player_id", "game_id", "season", "week"] + leak_cols)
y_train = train[target]

total_receiving_yards = combined[(combined["season"] == 2025) & (combined["week"] <= 8)] \
    .groupby("player_id")["receiving_yards"].sum()

# Keep only players with >0 passing yards
active_players = total_receiving_yards[total_receiving_yards > 0].index

# Prepare next week (week 8) test set
last_week = combined[(combined["season"] == 2025) & (combined["week"] <= 8) & (combined["player_id"].isin(active_players))] \
    .groupby("player_id").last().reset_index()

# Build X_test for prediction
X_test = last_week.drop(columns=[target, "player_id", "game_id", "season", "week"] + leak_cols)
X_test = pd.get_dummies(X_test, drop_first=True)
X_train, X_test = X_train.align(X_test, join="left", axis=1, fill_value=0)




# Encode categorical variables
X_train = pd.get_dummies(X_train, drop_first=True)
X_test = pd.get_dummies(X_test, drop_first=True)
X_train, X_test = X_train.align(X_test, join="left", axis=1, fill_value=0)

# -------------------------
# Train model
# -------------------------
model = RandomForestRegressor(
    n_estimators=300,
    max_depth=12,
    min_samples_split=4,
    min_samples_leaf=2,
    random_state=42,
    n_jobs=-1
)
model.fit(X_train, y_train)

# -------------------------
# Predict week 8
# -------------------------
week7_test = combined[(combined["season"] == 2025) & (combined["week"] == 8)]

X_week7 = week7_test.drop(columns=[target, "player_id", "game_id", "season", "week"] + leak_cols)
X_week7 = pd.get_dummies(X_week7, drop_first=True)
X_train, X_week7 = X_train.align(X_week7, join="left", axis=1, fill_value=0)

y_week7 = week7_test[target]
y_week7_pred = model.predict(X_week7)

mae_week7 = mean_absolute_error(y_week7, y_week7_pred)
r2_week7 = r2_score(y_week7, y_week7_pred)
y_pred = model.predict(X_test)

last_week["predicted_receiving_yards"] = y_pred
last_week["name"] = last_week["player_id"].map(names_map)
print(f"Week 7 Mean Absolute Error: {mae_week7:.2f}")
print(f"Week 7 R² Score: {r2_week7:.3f}")
print(last_week[["player_id", "name", "predicted_receiving_yards"]].head(50))
last_week[["player_id", "name", "predicted_receiving_yards"]].to_csv("predictions/week8_receiving_predictions.csv", index=False)
