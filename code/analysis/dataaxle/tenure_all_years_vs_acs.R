# Tenure (time at current address), all years: Data Axle length_of_residence (primary families, full data) vs ACS
# 1-year PUMS "when moved into this house" (MV), occupied units weighted by WGTP. Extends tenure_2006_vs_acs.R (method A).
# ACS tallies: pums_mv_by_year.csv, built by pums_mv.sh from www2.census.gov/programs-surveys/acs/data/pums/YYYY/1-Year/
# csv_hus.zip (2007-2023; no standard 2020 1-yr file); 2006 from data/ACS_microdata/pums2006. Data Axle 2022 dropped (truncated).
# Run from code/analysis/dataaxle/.

# Clean up workspace
gc()
rm(list = ls())

# Load libraries
library(tidyverse)
library(duckdb)

# Per-script output folder
OUTPUT_DIR <- "../../../output/analysis/dataaxle/tenure_all_years_vs_acs"
if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

BINS <- c("≤1 yr", "1–2 yrs", "2–4 yrs", "5–9 yrs", "10–19 yrs", "20–29 yrs", "30+ yrs")
CUTS <- c(0, 12, 24, 60, 120, 240, 360, Inf)  # month boundaries of the MV bins
GROUPS <- c(all = "All households", own = "Owners", rent = "Renters")

# ACS ---------------------------------------------------------------------------------------
con <- dbConnect(duckdb())
acs06 <- dbGetQuery(con, "
    SELECT 2006 AS y, MV::INT AS bin, CASE WHEN TEN IN ('1', '2') THEN 'own' ELSE 'rent' END AS ten, sum(WGTP::INT) AS n
    FROM read_csv(['../../../data/ACS_microdata/pums2006/ss06husa.csv', '../../../data/ACS_microdata/pums2006/ss06husb.csv'],
                  all_varchar = true)
    WHERE TYPE = '1' AND MV IS NOT NULL GROUP BY ALL")
dbDisconnect(con, shutdown = TRUE)
acs <- read_csv(file.path(OUTPUT_DIR, "pums_mv_by_year.csv"), show_col_types = FALSE) |>
    rename(bin = mv) |> bind_rows(acs06) |> mutate(source = "ACS")

# Data Axle ---------------------------------------------------------------------------------
# LOR = k covers months [12k - 6, 12k + 6) for k >= 2 and [0, 18) for k = 1; spread uniformly and split across MV bins.
da <- read_csv("../../../output/dataaxle_exploration/full5/lor_counts.csv", show_col_types = FALSE) |>
    filter(pf, record_year != 2022) |>
    transmute(y = record_year, lor, ten = ors, n) |>
    mutate(lo = if_else(lor == 1, 0, 12 * lor - 6), hi = 12 * lor + 6) |>
    cross_join(tibble(bin = 1:7, blo = head(CUTS, -1), bhi = CUTS[-1])) |>
    mutate(n = n * pmax(0, pmin(hi, bhi) - pmax(lo, blo)) / (hi - lo)) |>
    filter(n > 0) |>
    summarise(n = sum(n), .by = c(y, bin, ten)) |>
    mutate(source = "Data Axle")

# Shares within All / Owners / Renters (Data Axle 'unk' owner/renter flag only enters "All")
tab <- bind_rows(acs, da) |>
    filter(y %in% intersect(acs$y, da$y)) |>
    (\(d) bind_rows(d |> mutate(group = "all"), d |> filter(ten %in% c("own", "rent")) |> mutate(group = ten)))() |>
    summarise(n = sum(n), .by = c(source, y, group, bin)) |>
    mutate(pct = 100 * n / sum(n), .by = c(source, y, group)) |>
    mutate(group = factor(GROUPS[group], levels = GROUPS), bin = factor(BINS[bin], levels = BINS))
write_csv(tab, file.path(OUTPUT_DIR, "tenure_bins_by_year.csv"))

wide <- tab |> select(-n) |> pivot_wider(names_from = source, values_from = pct) |>
    mutate(diff = `Data Axle` - ACS)
write_csv(wide, file.path(OUTPUT_DIR, "tenure_bins_by_year_wide.csv"))

# Unknown owner/renter share in Data Axle, by year
unk <- read_csv("../../../output/dataaxle_exploration/full5/lor_counts.csv", show_col_types = FALSE) |>
    filter(pf) |> summarise(unk = sum(n[ors == "unk"]) / sum(n), .by = record_year)
write_csv(unk, file.path(OUTPUT_DIR, "da_unknown_tenure_share.csv"))

# Figure 1: share in each bin over time, Data Axle vs ACS
p1 <- ggplot(tab, aes(y, pct, colour = source)) +
    geom_line() + geom_point(size = 0.8) +
    facet_grid(group ~ bin) +
    scale_colour_manual(values = c("ACS" = "#4C72B0", "Data Axle" = "#DD8452")) +
    scale_x_continuous(breaks = c(2008, 2014, 2020)) +
    labs(x = NULL, y = "% of households", colour = NULL,
         title = "Time at current address, 2006–2023: Data Axle primary families vs ACS PUMS (when moved into this unit)",
         caption = paste("ACS: 1-yr PUMS, occupied housing units, WGTP-weighted (no 2020 file). Data Axle: full annual files, primary families (2022 dropped, truncated).",
                         "Data Axle LOR is rounded years (1 = 0–17 months, 2 = 18–29, ...); each value is spread uniformly over its months and split across ACS bins.",
                         "Owner/renter panels for Data Axle exclude families with an unknown (modelled) owner/renter flag.", sep = "\n")) +
    theme_minimal(base_size = 10) +
    theme(legend.position = "top", plot.caption = element_text(hjust = 0), panel.grid.minor = element_blank())
ggsave(file.path(OUTPUT_DIR, "tenure_bins_by_year.png"), p1, width = 15, height = 7.5, dpi = 150, bg = "white",
       device = ragg::agg_png)

# Figure 2: difference (Data Axle - ACS, pp), bin x year
p2 <- ggplot(wide, aes(y, bin, fill = diff)) +
    geom_tile(colour = "white") +
    geom_text(aes(label = sprintf("%+.0f", diff)), size = 2.6) +
    facet_wrap(~group, nrow = 1) +
    scale_fill_gradient2(low = "#4C72B0", mid = "white", high = "#C44E52", midpoint = 0, limits = c(-25, 25), oob = scales::squish) +
    scale_x_continuous(breaks = seq(2006, 2023, 2)) +
    labs(x = NULL, y = NULL, fill = "Data Axle − ACS (pp)",
         title = "Tenure distribution gap, Data Axle − ACS, percentage points") +
    theme_minimal(base_size = 10) + theme(panel.grid = element_blank(), axis.text.x = element_text(angle = 45, hjust = 1))
ggsave(file.path(OUTPUT_DIR, "tenure_gap_heatmap.png"), p2, width = 15, height = 4.5, dpi = 150, bg = "white",
       device = ragg::agg_png)

print(wide |> filter(group == "All households") |> select(y, bin, diff) |> mutate(diff = round(diff, 1)) |>
      pivot_wider(names_from = bin, values_from = diff), n = 30)
for (g in GROUPS) {
    cat("\n", g, "\n")
    print(wide |> filter(group == g, bin %in% c("≤1 yr", "1–2 yrs", "30+ yrs")) |>
          transmute(y, bin, ACS = round(ACS, 1), DA = round(`Data Axle`, 1)) |>
          pivot_wider(names_from = bin, values_from = c(ACS, DA)), n = 30)
}
