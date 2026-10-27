# Tenure histogram, 2006: Data Axle length_of_residence (primary families, full data) vs ACS 2006 1-year.
#   (A) ACS PUMS "when moved into this house" (MV, duration bins), occupied units weighted by WGTP
#   (B) published table B25038 (year householder moved into unit, calendar-year bins)
# Run from the repo root.

# Clean up workspace
gc()
rm(list = ls())

# Load libraries
library(tidyverse)
library(duckdb)
library(jsonlite)

# Per-script output folder
source("code/R/paths.R")
OUTPUT_DIR <- file.path(ROOT, "output/analysis/dataaxle/tenure_2006_vs_acs")
if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

SOURCES <- c("ACS 2006 1-yr", "Data Axle 2006")
GROUPS  <- c(all = "All households", own = "Owners", rent = "Renters")

# Data Axle ----------------------------------------------------------------------------------
# Codebook: months since arrival converted to years, 0-17 months = 1, 18-29 = 2, 30-41 = 3, ...
# so LOR = k covers months [12k - 6, 12k + 6) for k >= 2 and [0, 18) for k = 1.
# lor_counts.csv = full-data counts by record_year x primary family x owner/renter x LOR (Bouchet batch job)
da <- read_csv(file.path(ROOT, "output/dataaxle_exploration/full5/lor_counts.csv"), show_col_types = FALSE) |>
    filter(record_year == 2006, pf) |>
    select(lor, ten = ors, n)
unk_share <- sum(da$n[da$ten == "unk"]) / sum(da$n)

# Shares within All / Owners / Renters (Data Axle 'unk' owner/renter flag only enters "All")
shares <- function(df, source) {
    bind_rows(df |> mutate(group = "all"), df |> filter(ten %in% c("own", "rent")) |> mutate(group = ten)) |>
        group_by(group, bin) |> summarise(n = sum(n), .groups = "drop_last") |>
        mutate(pct = 100 * n / sum(n), source = source) |> ungroup() |>
        mutate(group = factor(GROUPS[group], levels = GROUPS), source = factor(source, levels = SOURCES))
}

plot_bins <- function(tab, xlab, title, caption) {
    ggplot(tab, aes(bin, pct, fill = source)) +
        geom_col(position = position_dodge(width = 0.8), width = 0.75) +
        facet_wrap(~group) +
        scale_fill_manual(values = c("#4C72B0", "#DD8452")) +
        labs(x = xlab, y = "% of households", fill = NULL, title = title, caption = caption) +
        theme_minimal(base_size = 11) +
        theme(legend.position = "top", axis.text.x = element_text(angle = 35, hjust = 1),
              panel.grid.major.x = element_blank(), plot.caption = element_text(hjust = 0))
}

# (A) PUMS duration bins -------------------------------------------------------------------
# ACS 2006 PUMS (www2.census.gov/programs-surveys/acs/data/pums/2006/csv_hus.zip)
# MV: 1 = <=12 months, 2 = 13-23 months, 3 = 2-4 yrs, 4 = 5-9, 5 = 10-19, 6 = 20-29, 7 = 30+
# TEN: 1, 2 = owner; 3, 4 = renter / occupied without rent
BINS_A <- c("≤1 yr", "1–2 yrs", "2–4 yrs", "5–9 yrs", "10–19 yrs", "20–29 yrs", "30+ yrs")
CUTS_A <- c(0, 12, 24, 60, 120, 240, 360, Inf)  # month boundaries of the MV bins

con <- dbConnect(duckdb::duckdb())
acs_a <- dbGetQuery(con, "
    SELECT MV::INT AS bin,
           CASE WHEN TEN IN ('1', '2') THEN 'own' ELSE 'rent' END AS ten,
           sum(WGTP::INT) AS n
    FROM read_csv(['../../../data/ACS_microdata/pums2006/ss06husa.csv',
                   '../../../data/ACS_microdata/pums2006/ss06husb.csv'], all_varchar = true)
    WHERE TYPE = '1' AND MV IS NOT NULL
    GROUP BY ALL")
dbDisconnect(con, shutdown = TRUE)

# Spread each LOR value uniformly over its month interval and split it across the MV bins it straddles
da_a <- da |>
    mutate(lo = if_else(lor == 1, 0, 12 * lor - 6), hi = 12 * lor + 6) |>
    cross_join(tibble(bin = 1:7, blo = head(CUTS_A, -1), bhi = CUTS_A[-1])) |>
    mutate(n = n * pmax(0, pmin(hi, bhi) - pmax(lo, blo)) / (hi - lo)) |>
    filter(n > 0) |>
    select(bin, ten, n)

tab_a <- bind_rows(shares(acs_a, "ACS 2006 1-yr"), shares(da_a, "Data Axle 2006")) |>
    mutate(bin = factor(BINS_A[bin], levels = BINS_A))
write_csv(tab_a, file.path(OUTPUT_DIR, "tenure_2006_pums_bins.csv"))

p_a <- plot_bins(tab_a, "Time at current address",
    "Time at current address, 2006: Data Axle primary families vs ACS PUMS (when moved into this unit)",
    sprintf(paste0("ACS: 2006 1-yr PUMS, occupied housing units, WGTP-weighted. Data Axle: full 2006 file, primary families.\n",
                   "Data Axle LOR is rounded years (1 = 0–17 months, 2 = 18–29, ...); each value is spread uniformly over its months and split across ACS bins.\n",
                   "Owner/renter panels for Data Axle exclude the %.0f%% with an unknown (modelled) owner/renter flag."), 100 * unk_share))
ggsave(file.path(OUTPUT_DIR, "tenure_2006_vs_acs_pums.png"), p_a, width = 12, height = 5, dpi = 150, bg = "white")

# (B) Published B25038, calendar-year bins ------------------------------------------------
# api.census.gov/data/2006/acs/acs1, B25038_001E-015E, for=us:1 (saved as b25038_2006_us.json)
BINS_B <- c("2005 or later", "2000–2004", "1990–1999", "1980–1989", "1970–1979", "1969 or earlier")
b <- fromJSON(file.path(OUTPUT_DIR, "b25038_2006_us.json"))
b <- setNames(as.numeric(b[2, 1:15]), b[1, 1:15])
acs_b <- tibble(ten = rep(c("own", "rent"), each = 6), bin = rep(1:6, 2),
                n = b[sprintf("B25038_%03dE", c(3:8, 10:15))])
stopifnot(sum(acs_b$n) == b["B25038_001E"])

# Data Axle: arrival year = 2006 - LOR (exact if the 2006 file is a mid-2006 snapshot)
da_b <- da |>
    mutate(yr = 2006 - lor,
           bin = case_when(yr >= 2005 ~ 1, yr >= 2000 ~ 2, yr >= 1990 ~ 3, yr >= 1980 ~ 4, yr >= 1970 ~ 5, TRUE ~ 6)) |>
    select(bin, ten, n)

tab_b <- bind_rows(shares(acs_b, "ACS 2006 1-yr"), shares(da_b, "Data Axle 2006")) |>
    mutate(bin = factor(BINS_B[bin], levels = BINS_B))
write_csv(tab_b, file.path(OUTPUT_DIR, "tenure_2006_b25038_bins.csv"))

p_b <- plot_bins(tab_b, "Year moved into current address",
    "Year moved in, 2006: Data Axle primary families vs ACS table B25038",
    sprintf(paste0("ACS: 2006 1-yr table B25038 (tenure by year householder moved into unit), US. Data Axle: full 2006 file, primary families.\n",
                   "Data Axle arrival year = 2006 − LOR (LOR is rounded years since arrival; exact for a mid-2006 snapshot, ±1 year otherwise).\n",
                   "Owner/renter panels for Data Axle exclude the %.0f%% with an unknown (modelled) owner/renter flag."), 100 * unk_share))
ggsave(file.path(OUTPUT_DIR, "tenure_2006_vs_acs_b25038.png"), p_b, width = 12, height = 5, dpi = 150, bg = "white")

# Print both tables
for (t in list(tab_a, tab_b)) {
    print(t |> select(group, bin, source, pct) |> mutate(pct = round(pct, 1)) |>
          pivot_wider(names_from = source, values_from = pct), n = 30)
}
