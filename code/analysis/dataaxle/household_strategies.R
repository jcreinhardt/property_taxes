# Household counts 2006-2013: ACS 1-yr households vs Data Axle under two de-duplication strategies.
#   S1:  every distinct building x unit combination (unit NULL = its own combination) is one household
#   S2a: as S1, but drop the unit-NULL combination when the building also has >= 1 real unit (same year)
#   S2b: as S1, but drop the unit-NULL combination only when the building has >= 2 distinct real units
# Building = street address (address_type 'S', house number + street name filled): zip + house number +
#   pre-direction + street name + suffix + post-direction. Unit = upper(trim(unit_num)), blank = NULL.
# Records without a usable street address (PO box, rural route, missing house number) count as one household each.
# Inputs: Data Axle counts from Bouchet batch job (hh_strat_full.sql, full data); ACS B11001_001E from api.census.gov.
# Run from the repo root.

# Clean up workspace
gc()
rm(list = ls())

# Load libraries
library(tidyverse)

# Per-script output folder
source("code/R/paths.R")
OUTPUT_DIR <- file.path(ROOT, "output/analysis/dataaxle/household_strategies")
if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

acs <- read_csv(file.path(OUTPUT_DIR, "acs_households_2006_2013.csv"), show_col_types = FALSE)
da  <- read_csv(file.path(OUTPUT_DIR, "dataaxle_hh_strategies_2006_2013.csv"), show_col_types = FALSE)

series <- da |>
    left_join(acs, by = "year") |>
    transmute(year,
              `ACS households (1-yr)`             = acs_households,
              `Data Axle: all family records`     = fam_records,
              `Data Axle: primary families`       = primary_fams,
              `S1: building x unit`               = s1_bldg_unit_combos + nobldg_records,
              `S2a: drop no-unit if ≥1 unit`      = s2a_combos + nobldg_records,
              `S2b: drop no-unit if ≥2 units`     = s2b_combos + nobldg_records) |>
    pivot_longer(-year, names_to = "series", values_to = "n") |>
    mutate(series = fct_inorder(series), n_m = n / 1e6)
write_csv(series |> select(year, series, n) |> pivot_wider(names_from = series, values_from = n),
          file.path(OUTPUT_DIR, "household_strategies_2006_2013.csv"))

p <- ggplot(series, aes(year, n_m, colour = series, linetype = series)) +
    geom_line(linewidth = 0.9) + geom_point(size = 1.6) +
    scale_colour_manual(values = c("black", "grey55", "#DD8452", "#4C72B0", "#55A868", "#C44E52")) +
    scale_linetype_manual(values = c("solid", "dotted", "dashed", "solid", "solid", "dashed")) +
    scale_x_continuous(breaks = 2006:2013) +
    labs(x = NULL, y = "Millions", colour = NULL, linetype = NULL,
         title = "Households: ACS vs Data Axle under building x unit de-duplication, 2006–2013",
         caption = paste0("S1-S2b count records without a usable street address (PO box, rural route; ~13–14m) as one household each.\n",
                          "Data Axle: full annual files on Bouchet. ACS: 2006–2013 1-yr B11001_001E (US).")) +
    theme_minimal(base_size = 11) +
    theme(legend.position = "right", plot.caption = element_text(hjust = 0), panel.grid.minor = element_blank())
ggsave(file.path(OUTPUT_DIR, "household_strategies_2006_2013.png"), p, width = 10, height = 5, dpi = 150, bg = "white")
