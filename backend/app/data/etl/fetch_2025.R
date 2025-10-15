library(nflfastR)
library(tidyverse)
library(lubridate) # for date calculations

SEASONS <- 2025
OUTPUT_DIR <- "../2025/"

# ensure output directory exists
if(!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

# ------------------------
# Fetch play-by-play data
# ------------------------
cat("Fetching play-by-play data for seasons:", SEASONS, "\n")
pbp <- load_pbp(SEASONS)

pbp_small <- pbp %>%
  select(
    game_id, season, week, posteam, defteam, play_type, down, ydstogo, yardline_100,
    passer_player_id, rusher_player_id, receiver_player_id, air_yards, yards_after_catch,
    rushing_yards,            
    pass_touchdown, rush_touchdown, return_touchdown,
    interception, fumble_lost, pass_attempt, complete_pass
  ) %>%
  mutate(
    reception = ifelse(complete_pass == 1, 1, 0),
    receiving_yards = air_yards + yards_after_catch
  )

pbp_file <- paste0(OUTPUT_DIR, "pbp_", min(SEASONS), "_", max(SEASONS), ".csv")
write_csv(pbp_small, pbp_file)
cat("Saved play-by-play to", pbp_file, "\n")

# ------------------------
# Fetch roster data
# ------------------------
cat("Fetching roster data\n")
roster <- fast_scraper_roster(SEASONS)

roster_file <- paste0(OUTPUT_DIR, "roster_", min(SEASONS), "_", max(SEASONS), ".csv")
write_csv(roster, roster_file)
cat("Saved roster to", roster_file, "\n")

cat("Done!\n")
