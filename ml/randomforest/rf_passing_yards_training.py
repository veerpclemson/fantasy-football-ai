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
df2025 = pd.read_sql_table("2025_week_6", engine)  # weeks 1-6 only

# Keep only rows with valid passing yards
df = df[df["passing_yards"].notna()]
df2025 = df2025[df2025["passing_yards"].notna()]

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
target = "passing_yards"

# Leak columns to drop
leak_cols = [
    "passing_yards", "pass_attempt", "complete_pass", "rush_plays",
    "receiving_yards", "reception", "total_touches", "rush_inside_10", "rush_inside_20", "target_inside_10", "target_inside_20",
    "total_plays_off", "pass_plays_off", "rush_plays_off", "total_pass_plays", "avg_yac_off", "avg_air_yards_off",
    "fantasy_points", "pass_touchdown", "rush_touchdown", "receiving_touchdown","blitz_rate_def",
    "pressure_rate_def","man_coverage_pct_def","zone_coverage_pct_def","pass_pct_off",
    "rush_pct_off"
]

# Train on all historical + first 6 weeks of 2025
train = combined[(combined["season"] < 2025) | ((combined["season"] == 2025) & (combined["week"] <= 6))]
X_train = train.drop(columns=[target, "player_id", "game_id", "season", "week"] + leak_cols)
y_train = train[target]

total_passing_yards = combined[(combined["season"] == 2025) & (combined["week"] <= 6)] \
    .groupby("player_id")["passing_yards"].sum()

# Keep only players with >0 passing yards
active_players = total_passing_yards[total_passing_yards > 0].index

# Prepare next week (week 7) test set
last_week = combined[(combined["season"] == 2025) & (combined["week"] <= 6) & (combined["player_id"].isin(active_players))] \
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
# Predict week 7
# -------------------------
# Use week 6 as a pseudo-test set

week6_test = combined[(combined["season"] == 2025) & (combined["week"] == 6)]

X_week6 = week6_test.drop(columns=[target, "player_id", "game_id", "season", "week"] + leak_cols)
X_week6 = pd.get_dummies(X_week6, drop_first=True)
X_train, X_week6 = X_train.align(X_week6, join="left", axis=1, fill_value=0)

y_week6 = week6_test[target]
y_week6_pred = model.predict(X_week6)

mae_week6 = mean_absolute_error(y_week6, y_week6_pred)
r2_week6 = r2_score(y_week6, y_week6_pred)



y_pred = model.predict(X_test)




last_week["predicted_passing_yards"] = y_pred
last_week["name"] = last_week["player_id"].map(names_map)
print(f"Week 6 Mean Absolute Error: {mae_week6:.2f}")
print(f"Week 6 R² Score: {r2_week6:.3f}")
print(last_week[["player_id", "name", "predicted_passing_yards"]].head(50))
# Save predictions to CSV
last_week[["player_id", "name", "predicted_passing_yards"]].to_csv("predictions/week7_passing_predictions.csv", index=False)
