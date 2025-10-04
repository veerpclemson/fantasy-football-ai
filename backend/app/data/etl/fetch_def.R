library(nflfastR)
library(tidyverse)

SEASONS <- 2021:2024
OUTPUT_DIR <- "backend/app/data/play_by_play/"

if(!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

cat("Fetching play-by-play data for seasons:", SEASONS, "\n")
pbp <- load_pbp(SEASONS)

# Filter for passing plays only
pbp_pass <- pbp %>%
  filter(pass_attempt == 1)

# Heuristics:
# Blitz: defenders_in_box > 6
# Pressure: qb_hit == 1 OR pressure == 1
# Coverage: man_coverage / zone_coverage if available, else approximate

pbp_def <- pbp_pass %>%
  mutate(
    blitz = ifelse(!is.na(defenders_in_box) & defenders_in_box > 6, 1, 0),
    pressure = ifelse(!is.na(qb_hit) & qb_hit == 1, 1, 0),
    man_coverage = ifelse(!is.na(coverage) & coverage == "Man", 1, 0),
    zone_coverage = ifelse(!is.na(coverage) & coverage == "Zone", 1, 0)
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


def_file <- paste0(OUTPUT_DIR, "defense_tendencies_", min(SEASONS), "_", max(SEASONS), ".csv")
write_csv(def_tendencies, def_file)
cat("Saved defensive tendencies to", def_file, "\n")
