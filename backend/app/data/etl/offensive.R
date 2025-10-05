library(nflfastR)
library(tidyverse)

SEASONS <- 2021:2024
OUTPUT_DIR <- "backend/app/data/play_by_play/"

if(!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

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
