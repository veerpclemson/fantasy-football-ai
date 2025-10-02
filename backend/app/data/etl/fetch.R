

library(nflfastR)
library(tidyverse)


SEASONS <- 2021:2024 #get data 2021-2024
OUTPUT_DIR <- "backend/app/data/play_by_play/"

#fetch
cat("Fetching play-by-play data for seasons:", SEASONS, "\n")
pbp <- load_pbp(SEASONS)

# get relevant columns
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


cat("Fetching roster data\n")
roster <- fast_scraper_roster(SEASONS)


roster_file <- paste0(OUTPUT_DIR, "roster_", min(SEASONS), "_", max(SEASONS), ".csv")
write_csv(roster, roster_file)
cat("Saved roster to", roster_file, "\n")

cat("Done!\n")
