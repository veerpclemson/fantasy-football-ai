import pandas as pd
import lightgbm as lgb
from sklearn.metrics import root_mean_squared_error # type: ignore
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from typing import List, Callable, Any

# -------------------------------
# Load environment and connect
# -------------------------------
load_dotenv()
DB_URI = os.getenv("DATABASE_URL")
engine = create_engine(DB_URI)

# -------------------------------
# Read and preprocess data
# -------------------------------
df = pd.read_sql_table("final_modeling_data", engine)

# Filter valid rows
df = df[(df["player_id"].astype(str) != "0") & (df["receiving_yards"] > 0)]

# Sort to preserve time order
df = df.sort_values(by=["player_id", "season", "week"])

# Extract home team from game_id
df["home_team"] = df["game_id"].apply(lambda x: x.split("_")[-1])

# Determine if player’s team is home or away, and assign moneyline
df["player_moneyline"] = df.apply(
    lambda row: row["home_moneyline"] if row["posteam"] == row["home_team"] else row["away_moneyline"],
    axis=1
)

# -------------------------------
# Define features and target
# -------------------------------
features = [
    "week", "total_touches", "rushing_yards", "rush_touchdown", "pass_touchdown",
    "total_pass_plays", "pass_plays_off", "pass_pct_off", "reception", "receiving_touchdown",
    "red_zone_pass_pct_off", "deep_pass_pct_off", "avg_air_yards_off", "avg_yac_off",
    "man_coverage_pct_def", "zone_coverage_pct_def", "blitz_rate_def", "pressure_rate_def", 
    "spread_line", "total_line", "over_odds", "under_odds", "fumble_lost", "rush_inside_10", "rush_inside_20",
    "player_moneyline", "fantasy_points_rolling3", "target_inside_10", "target_inside_20",
    "receiving_yards_rolling3", "reception_rolling3", "receiving_touchdown_rolling3", "rush_touchdown_rolling3"
]
target = "fantasy_points"

# Drop NaNs
df_model = df[features + [target, "player_id", "season"]].dropna()

# -------------------------------
# Time-based split
# -------------------------------
train_df = df_model[df_model["season"].isin([2021, 2022, 2023])]
test_df = df_model[df_model["season"] == 2024]

X_train, y_train = train_df[features], train_df[target]
X_test, y_test = test_df[features], test_df[target]
pid_test = test_df["player_id"]

# -------------------------------
# LightGBM training setup
# -------------------------------
train_data = lgb.Dataset(X_train, label=y_train)
valid_data = lgb.Dataset(X_test, label=y_test, reference=train_data)

params = {
    "objective": "regression",
    "metric": "rmse",
    "boosting_type": "gbdt",
    "learning_rate": 0.01,
    "num_leaves": 31,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 5,
    "verbose": -1
}

# -------------------------------
# Train with new callback style
# -------------------------------
callbacks: List[Callable[..., Any]] = [
    lgb.early_stopping(stopping_rounds=50),
    lgb.log_evaluation(period=50)
]

model = lgb.train(
    params=params,
    train_set=train_data,
    num_boost_round=1500,
    valid_sets=[train_data, valid_data],
    valid_names=["train", "valid"],
    callbacks=callbacks
)

# -------------------------------
# Predict and evaluate
# -------------------------------
y_pred = model.predict(X_test, num_iteration=model.best_iteration)
rmse = root_mean_squared_error(y_test, y_pred)
print(f"Test RMSE: {rmse:.2f}")

# -------------------------------
# Save model and predictions
# -------------------------------
model.save_model("../model_files/lgb_wr_fantay_points.txt")

pd.DataFrame({
    "player_id": pid_test,
    "actual_receiving_yards": y_test,
    "predicted_receiving_yards": y_pred
}).to_csv("../prediction_files/predicted_wr_fantay_points_2024.csv", index=False)

print("✅ Model and predictions saved successfully.")
