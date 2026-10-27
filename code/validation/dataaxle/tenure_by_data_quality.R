# Tenure gap vs ACS by family-level data quality. Data quality of a primary family = years it is observed in Data Axle
# relative to the span between its first and last year (dq_tenure.sql, full data, 2022 excluded). Two splits:
#   (1) share of span observed: complete, 80-99%, 50-79%, < 50% (single-year families separate)
#   (2) number of missing years inside the span: 0, 1, ..., 6+
# Each group's LOR distribution is compared with the same ACS PUMS MV distribution (tenure_all_years_vs_acs.R).
# Run from the repo root.

# Clean up workspace
gc()
rm(list = ls())

# Load libraries
library(tidyverse)

# Per-script output folder
source("code/R/paths.R")
OUTPUT_DIR <- file.path(ROOT, "output/validation/dataaxle/tenure_all_years_vs_acs")
if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

BINS <- c("≤1 yr", "1–2 yrs", "2–4 yrs", "5–9 yrs", "10–19 yrs", "20–29 yrs", "30+ yrs")
CUTS <- c(0, 12, 24, 60, 120, 240, 360, Inf)
FRAC_LAB <- c("0_single_year" = "Seen one year only", "1_complete" = "All years of span", "2_80_99" = "80–99% of span",
              "3_50_79" = "50–79% of span", "4_below_50" = "< 50% of span")

# ACS (all occupied units), from tenure_all_years_vs_acs.R's tallies
acs <- read_csv(file.path(OUTPUT_DIR, "tenure_bins_by_year.csv"), show_col_types = FALSE) |>
    filter(source == "ACS", group == "All households") |> transmute(y, bin = factor(bin, levels = BINS), acs = pct)

# Data Axle, spread LOR over months and split across MV bins (as in tenure_2006_vs_acs.R)
dq <- read_csv(file.path(OUTPUT_DIR, "dq_tenure_counts.csv"), show_col_types = FALSE) |>
    rename(y = record_year) |> filter(!is.na(lor), lor >= 1)
to_bins <- function(d, split) {
    d |> rename(q = all_of(split)) |>
        summarise(n = sum(n), .by = c(y, q, lor)) |>
        mutate(lo = if_else(lor == 1, 0, 12 * lor - 6), hi = 12 * lor + 6) |>
        cross_join(tibble(b = 1:7, blo = head(CUTS, -1), bhi = CUTS[-1])) |>
        mutate(n = n * pmax(0, pmin(hi, bhi) - pmax(lo, blo)) / (hi - lo)) |>
        summarise(n = sum(n), .by = c(y, q, b)) |>
        mutate(pct = 100 * n / sum(n), fam = sum(n), .by = c(y, q)) |>
        mutate(bin = factor(BINS[b], levels = BINS)) |>
        inner_join(acs, by = c("y", "bin")) |> mutate(diff = pct - acs, split = split)
}
res <- bind_rows(to_bins(dq, "frac_bin"), to_bins(dq, "miss_bin"))
write_csv(res, file.path(OUTPUT_DIR, "tenure_by_data_quality.csv"))

# Group sizes (share of primary families per year)
sizes <- bind_rows(dq |> rename(q = frac_bin) |> summarise(n = sum(n), .by = c(y, q)) |> mutate(split = "frac_bin"),
                   dq |> rename(q = miss_bin) |> summarise(n = sum(n), .by = c(y, q)) |> mutate(split = "miss_bin")) |>
    mutate(share = n / sum(n), .by = c(y, split)) |> arrange(split, y, q)
write_csv(sizes, file.path(OUTPUT_DIR, "tenure_by_data_quality_group_sizes.csv"))

heat <- function(split_name, labs_map, title) {
    d <- res |> filter(split == split_name) |>
        mutate(q = if (is.null(labs_map)) if_else(q == "0_single_year", "Seen one year only",
                                                    if_else(q == "6", "6+ missing years", paste(q, "missing years"))) else labs_map[q])
    if (is.null(labs_map)) d <- d |> mutate(q = factor(q, levels = c("Seen one year only", paste(0:5, "missing years"), "6+ missing years")))
    else d <- d |> mutate(q = factor(q, levels = labs_map))
    ggplot(d, aes(y, bin, fill = diff)) +
        geom_tile(colour = "white") +
        geom_text(aes(label = sprintf("%+.0f", diff)), size = 2.3) +
        facet_wrap(~q, nrow = 2) +
        scale_fill_gradient2(low = "#4C72B0", mid = "white", high = "#C44E52", midpoint = 0, limits = c(-20, 20), oob = scales::squish) +
        scale_x_continuous(breaks = seq(2006, 2023, 3)) +
        labs(x = NULL, y = NULL, fill = "Data Axle − ACS (pp)", title = title,
             caption = paste("Primary families, all households. Span = first to last year the family_id appears in Data Axle (2006–2023, 2022 excluded).",
                             "Each group is compared with the same ACS PUMS distribution (occupied units, when moved into this unit).", sep = "\n")) +
        theme_minimal(base_size = 10) + theme(panel.grid = element_blank(), plot.caption = element_text(hjust = 0))
}
ggsave(file.path(OUTPUT_DIR, "tenure_gap_by_share_observed.png"),
       heat("frac_bin", FRAC_LAB, "Tenure gap vs ACS by share of the family's span observed in Data Axle"),
       width = 15, height = 7, dpi = 150, bg = "white", device = ragg::agg_png)
ggsave(file.path(OUTPUT_DIR, "tenure_gap_by_missing_years.png"),
       heat("miss_bin", NULL, "Tenure gap vs ACS by number of missing years inside the family's span"),
       width = 15, height = 7, dpi = 150, bg = "white", device = ragg::agg_png)

print(sizes |> filter(y %in% c(2012, 2019)) |> mutate(share = round(share, 3)), n = 40)
print(res |> filter(y == 2017) |> select(split, q, bin, diff) |> mutate(diff = round(diff, 1)) |>
      pivot_wider(names_from = bin, values_from = diff), n = 20)
