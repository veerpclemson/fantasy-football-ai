import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score


load_dotenv()
DB_URI = os.getenv("DATABASE_URL")
engine = create_engine(DB_URI)


df = pd.read_sql_table("final_modeling_data", engine)
df = df.sort_values(["player_id", "season", "week"])
df = df[df["passing_yards"].notna() & (df["passing_yards"] != 0)]

# Define features to roll
rolling_features = [
    "blitz_rate_def",
    "pressure_rate_def",
    "man_coverage_pct_def",
    "zone_coverage_pct_def",
    "avg_yac_off",
    "total_touches",
    "pass_pct_off",
    "rush_pct_off",
    "pass_attempt",
    "complete_pass",
    "avg_air_yards_off"
]

# Rolling window size
window = 3

# Apply rolling mean grouped by player (or team for defensive stats)
for col in rolling_features:
    if col in ["blitz_rate_def", "pressure_rate_def", "man_coverage_pct_def"]:
        # For defensive stats, group by team on defense
        df[col + f"_rolling{window}"] = df.groupby("defteam_x")[col].shift(1).rolling(window).mean()
    else:
        # For offensive stats, group by player
        df[col + f"_rolling{window}"] = df.groupby("player_id")[col].shift(1).rolling(window).mean()
target = "passing_yards"

leak_cols = [
    "rushing_yards", "pass_attempt", "complete_pass", "rush_plays",
    "receiving_yards", "reception", "total_touches", "rush_inside_10", "rush_inside_20", "target_inside_10", "target_inside_20",
    "total_plays_off", "pass_plays_off", "rush_plays_off", "total_pass_plays", "avg_yac_off", "avg_air_yards_off",
    "fantasy_points", "pass_touchdown", "rush_touchdown", "receiving_touchdown","blitz_rate_def",
    "pressure_rate_def","man_coverage_pct_def","zone_coverage_pct_def","pass_pct_off",
    "rush_pct_off"
]

X = df.drop(columns=[target, "player_id", "game_id", "season", "week"] + leak_cols)
y = df[target]


train = df[df["season"] <= 2023]
test = df[df["season"] == 2024]

X_train = train.drop(columns=[target, "player_id", "game_id", "season", "week"] + leak_cols)
y_train = train[target]
X_test = test.drop(columns=[target, "player_id", "game_id", "season", "week"] + leak_cols)
y_test = test[target]
X_train = pd.get_dummies(X_train, drop_first=True)
X_test = pd.get_dummies(X_test, drop_first=True)

X_train, X_test = X_train.align(X_test, join='left', axis=1, fill_value=0)

X_train.columns = [str(c) for c in X_train.columns] # type:ignore
X_test.columns = [str(c) for c in X_test.columns] # type:ignore


model = RandomForestRegressor(
    n_estimators=300,
    max_depth=12,
    min_samples_split=4,
    min_samples_leaf=2,
    random_state=42,
    n_jobs=-1
)
model.fit(X_train, y_train)


y_pred = model.predict(X_test)
mae = mean_absolute_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print(f"Mean Absolute Error: {mae:.2f}")
print(f"R² Score: {r2:.3f}")


importances = pd.Series(model.feature_importances_, index=X_train.columns).sort_values(ascending=False)
print("\nTop 10 Important Features:")
print(importances.head(10))