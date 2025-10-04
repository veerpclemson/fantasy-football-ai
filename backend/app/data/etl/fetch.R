

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


cat("Fetching game metadata\n")
games <- fast_scraper_schedules(SEASONS)

# select useful columns
games_small <- games %>%
  select(game_id, season, week, home_team, away_team, game_date, stadium, roof, surface) %>%
  mutate(game_date = as.Date(game_date))

# ------------------------
# Calculate rest days
# ------------------------
games_small <- games_small %>%
  arrange(home_team, game_date) %>%
  group_by(home_team) %>%
  mutate(home_days_rest = as.numeric(difftime(game_date, lag(game_date), units = "days"))) %>%
  ungroup() %>%
  arrange(away_team, game_date) %>%
  group_by(away_team) %>%
  mutate(away_days_rest = as.numeric(difftime(game_date, lag(game_date), units = "days"))) %>%
  ungroup()

games_file <- paste0(OUTPUT_DIR, "games_", min(SEASONS), "_", max(SEASONS), ".csv")
write_csv(games_small, games_file)
cat("Saved game metadata to", games_file, "\n")


cat("Done!\n")
