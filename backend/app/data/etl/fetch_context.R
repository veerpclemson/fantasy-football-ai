library(nflfastR)
library(tidyverse)
library(lubridate)

SEASONS <- 2025
OUTPUT_DIR <- "../2025/"

if(!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

cat("Fetching game metadata\n")
games <- fast_scraper_schedules(SEASONS)

# select relevant columns for model context
games_context <- games %>%
  select(
    game_id, season, week, gameday, weekday, gametime,
    home_team, away_team,
    home_score, away_score,
    home_rest, away_rest,
    spread_line, total_line, over_odds, under_odds,
    home_moneyline, away_moneyline,
    roof, surface, temp, wind,
    stadium_id
  ) %>%
  mutate(game_date = as.Date(gameday)) # optional: ensure date format

# save CSV
games_file <- paste0(OUTPUT_DIR, "games_context_", min(SEASONS), "_", max(SEASONS), ".csv")
write_csv(games_context, games_file)
cat("Saved game context to", games_file, "\n")