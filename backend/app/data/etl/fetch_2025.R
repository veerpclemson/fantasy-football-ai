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

cat("Fetching play-by-play data for seasons:", SEASONS, "\n")
pbp <- load_pbp(SEASONS)

# Filter for passing plays only
pbp_pass <- pbp %>% 
  filter(pass_attempt == 1)

# Defensive heuristics
pbp_def <- pbp_pass %>%
  mutate(
    # Blitz heuristic: TFL or sack
    blitz = ifelse(!is.na(tackle_for_loss_1_player_id) | 
                   !is.na(tackle_for_loss_2_player_id) |
                   !is.na(sack_player_id), 1, 0),
    
    # Pressure heuristic: QB hit or sack
    pressure = ifelse(!is.na(qb_hit_1_player_id) |
                      !is.na(qb_hit_2_player_id) |
                      !is.na(sack_player_id), 1, 0),
    
    # Coverage heuristic: approximate man coverage if any defender listed, zone otherwise
    man_coverage = ifelse(!is.na(pass_defense_1_player_id) | 
                          !is.na(pass_defense_2_player_id), 1, 0),
    
    zone_coverage = 1 - man_coverage
  ) %>%
  select(season, week, defteam, blitz, pressure, man_coverage, zone_coverage)

# Aggregate per defense/week/season
def_tendencies <- pbp_def %>%
  group_by(season, week, defteam) %>%
  summarise(
    total_pass_plays = n(),
    blitz_rate = sum(blitz, na.rm = TRUE)/n(),
    pressure_rate = sum(pressure, na.rm = TRUE)/n(),
    man_coverage_pct = sum(man_coverage, na.rm = TRUE)/n(),
    zone_coverage_pct = sum(zone_coverage, na.rm = TRUE)/n(),
    .groups = "drop"
  )

# Save CSV
def_file <- paste0(OUTPUT_DIR, "defense_tendencies_", min(SEASONS), "_", max(SEASONS), ".csv")
write_csv(def_tendencies, def_file)
cat("Saved improved defensive tendencies to", def_file, "\n")

cat("Fetching play-by-play data for seasons:", SEASONS, "\n")
pbp <- load_pbp(SEASONS)

# Only offensive plays
pbp_off <- pbp %>% 
  filter(pass_attempt == 1 | rush_attempt == 1) %>%
  mutate(
    play_type_simple = case_when(
      pass_attempt == 1 ~ "pass",
      rush_attempt == 1 ~ "rush",
      TRUE ~ "other"
    ),
    red_zone = ifelse(yardline_100 <= 20, 1, 0), # inside opponent 20-yard line
    deep_pass = ifelse(pass_attempt == 1 & air_yards >= 20, 1, 0)
  )

# Aggregate per team/week/season
off_tendencies <- pbp_off %>%
  group_by(season, week, posteam) %>%
  summarise(
    total_plays = n(),
    pass_plays = sum(play_type_simple == "pass"),
    rush_plays = sum(play_type_simple == "rush"),
    pass_pct = pass_plays / total_plays,
    rush_pct = rush_plays / total_plays,
    red_zone_pass_pct = sum(pass_attempt & red_zone) / sum(red_zone),
    deep_pass_pct = sum(deep_pass) / pass_plays,
    avg_air_yards = mean(air_yards[pass_attempt == 1], na.rm = TRUE),
    avg_yards_after_catch = mean(yards_after_catch[pass_attempt == 1], na.rm = TRUE),
    .groups = "drop"
  )

# Save CSV
off_file <- paste0(OUTPUT_DIR, "offense_tendencies_", min(SEASONS), "_", max(SEASONS), ".csv")
write_csv(off_tendencies, off_file)
cat("Saved offensive tendencies to", off_file, "\n")

cat("Done!\n")
