library(nflfastR)
library(tidyverse)

SEASONS <- 2
OUTPUT_DIR <- "backend/app/data/play_by_play/"

if(!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

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
