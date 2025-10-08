import pandas as pd

# --- Load data ---
games = pd.read_csv("../play_by_play/games_context_2021_2024.csv")
off = pd.read_csv("../play_by_play/offense_tendencies_2021_2024.csv")
defn = pd.read_csv("../play_by_play/defense_tendencies_2021_2024.csv")

# --- Standardize column names for merging ---
off = off.rename(columns={"posteam": "team"})
defn = defn.rename(columns={"defteam": "team"})

# --- Home Offense vs Away Defense ---
home_vs_away = games.merge(
    off, left_on=["season", "week", "home_team"], right_on=["season", "week", "team"], how="left"
).merge(
    defn, left_on=["season", "week", "away_team"], right_on=["season", "week", "team"], how="left", suffixes=("_off", "_def")
)

home_vs_away["posteam"] = home_vs_away["home_team"]
home_vs_away["defteam"] = home_vs_away["away_team"]

# --- Away Offense vs Home Defense ---
away_vs_home = games.merge(
    off, left_on=["season", "week", "away_team"], right_on=["season", "week", "team"], how="left"
).merge(
    defn, left_on=["season", "week", "home_team"], right_on=["season", "week", "team"], how="left", suffixes=("_off", "_def")
)

away_vs_home["posteam"] = away_vs_home["away_team"]
away_vs_home["defteam"] = away_vs_home["home_team"]

# --- Combine both perspectives ---
matchup_tendencies = pd.concat([home_vs_away, away_vs_home], ignore_index=True)

# --- Drop redundant team columns ---
matchup_tendencies = matchup_tendencies.drop(columns=["team_off", "team_def"], errors="ignore")

# --- Fill missing values ---
matchup_tendencies = matchup_tendencies.fillna(0)

print("✅ Matchup tendencies table created:", matchup_tendencies.shape)
#matchup_tendencies.to_csv("../play_by_play/matchup_tendencies_2021_2024.csv", index=False)


# Load the previously created matchup table
#matchup_tendencies = pd.read_csv("../play_by_play/matchup_tendencies_2021_2024.csv")

# --- Rename columns for clarity ---
matchup_tendencies = matchup_tendencies.rename(columns={
    # Offensive columns
    "total_plays": "total_plays_off",
    "pass_plays": "pass_plays_off",
    "rush_plays": "rush_plays_off",
    "pass_pct": "pass_pct_off",
    "rush_pct": "rush_pct_off",
    "red_zone_pass_pct": "red_zone_pass_pct_off",
    "deep_pass_pct": "deep_pass_pct_off",
    "avg_air_yards": "avg_air_yards_off",
    "avg_yards_after_catch": "avg_yac_off",
    
    # Defensive columns
    "total_pass_plays_def": "total_pass_plays_def",
    "blitz_rate": "blitz_rate_def",
    "pressure_rate": "pressure_rate_def",
    "man_coverage_pct": "man_coverage_pct_def",
    "zone_coverage_pct": "zone_coverage_pct_def"
})

# --- Strip whitespace from string columns ---
matchup_tendencies["surface"] = matchup_tendencies["surface"].str.strip()
matchup_tendencies["roof"] = matchup_tendencies["roof"].str.strip()
matchup_tendencies["posteam"] = matchup_tendencies["posteam"].str.strip()
matchup_tendencies["defteam"] = matchup_tendencies["defteam"].str.strip()

# --- Fill any remaining missing values ---
matchup_tendencies = matchup_tendencies.fillna(0)

# --- Save cleaned table ---
matchup_tendencies.to_csv("../play_by_play/matchup_tendencies_2021_2024_clean.csv", index=False)

print("✅ Cleaned matchup tendencies table saved:", matchup_tendencies.shape)

print(matchup_tendencies.head())
