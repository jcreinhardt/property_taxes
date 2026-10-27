# Adult children and older generations: inside the Data Axle primary family, in secondary families, or missing?
# Data Axle (hh_generations.sql): households = building x unit with exactly one primary family; reference = primary person 1;
# child generation = adult >= 18 years younger than the reference, older generation = >= 18 years older. Only households
# where every listed person has an age. ACS (IPUMS 2011-2023, households, adults 18+): the same age-gap definitions relative
# to the householder, plus relationship-based versions. Also: adults per primary family vs adults per ACS household.
# Run from the repo root.

# Clean up workspace
gc()
rm(list = ls())

# Load libraries
library(tidyverse)
library(duckdb)

# Per-script output folder
source("code/R/paths.R")
OUTPUT_DIR <- file.path(ROOT, "output/validation/dataaxle/hh_surnames")
if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

BANDS <- c("18-34", "35-49", "50-64", "65+")

# ACS benchmark (cached) ------------------------------------------------------------------------
acs_file <- file.path(OUTPUT_DIR, "acs_hh_generations.csv")
if (!file.exists(acs_file)) {
    con <- dbConnect(duckdb())
    acs <- dbGetQuery(con, "
        WITH p AS (SELECT YEAR, SERIAL, HHWT, UNITSSTR, RELATE, AGE,
                          max(CASE WHEN RELATE = 1 THEN AGE END) OVER (PARTITION BY YEAR, SERIAL) rage
                   FROM read_csv('../../../data/ACS_microdata/usa_00004.csv.gz')
                   WHERE GQ IN (1, 2) AND YEAR IN (2012, 2017, 2019, 2023)),
        h AS (SELECT YEAR, SERIAL, any_value(HHWT) hhwt, any_value(UNITSSTR) unitsstr, any_value(rage) rage,
                     count(*) FILTER (WHERE AGE >= 18) adults,
                     bool_or(AGE >= 18 AND RELATE <> 1 AND AGE <= rage - 18) child_gen,
                     bool_or(AGE >= 18 AND RELATE <> 1 AND AGE >= rage + 18) old_gen,
                     bool_or(AGE >= 18 AND RELATE = 3) adult_child,
                     bool_or(AGE >= 18 AND RELATE IN (4, 9)) adult_child_in_law_or_grandchild,
                     bool_or(RELATE IN (5, 6)) parent_or_in_law
              FROM p GROUP BY YEAR, SERIAL)
        SELECT YEAR y, CASE WHEN unitsstr IN (3, 4) THEN 'single-family' WHEN unitsstr >= 5 THEN 'multi-unit' ELSE 'other' END bldg,
               CASE WHEN rage < 35 THEN '18-34' WHEN rage < 50 THEN '35-49' WHEN rage < 65 THEN '50-64' ELSE '65+' END ref_age_band,
               least(adults, 6) adults, sum(hhwt) hh,
               sum(hhwt * child_gen::INT) child_gen, sum(hhwt * old_gen::INT) old_gen, sum(hhwt * adult_child::INT) adult_child,
               sum(hhwt * adult_child_in_law_or_grandchild::INT) child_in_law_or_grandchild, sum(hhwt * parent_or_in_law::INT) parent_or_in_law
        FROM h WHERE rage >= 18 GROUP BY ALL ORDER BY ALL")
    dbDisconnect(con, shutdown = TRUE)
    write_csv(acs, acs_file)
}
acs <- read_csv(acs_file, show_col_types = FALSE) |> filter(bldg != "other")

# Data Axle --------------------------------------------------------------------------------------
da <- read_csv(file.path(OUTPUT_DIR, "hh_generations.csv"), show_col_types = FALSE) |>
    filter(grp %in% c("sf_nounit", "unit")) |>
    mutate(bldg = if_else(grp == "sf_nounit", "single-family", "multi-unit"))

# Age coverage: share of households usable
cov <- da |> summarise(all_ages = sum(n * all_ages) / sum(n), hh = sum(n), .by = c(y, bldg))
write_csv(cov, file.path(OUTPUT_DIR, "generations_age_coverage.csv"))

dk <- da |> filter(all_ages, ref_age_band %in% BANDS)
gen_da <- dk |>
    summarise(hh = sum(n),
              child_gen_prim = sum(n * prim_child_gen) / hh, child_gen_sec_only = sum(n * (sec_child_gen & !prim_child_gen)) / hh,
              child_gen_any = sum(n * (prim_child_gen | sec_child_gen)) / hh,
              old_gen_prim = sum(n * prim_old_gen) / hh, old_gen_sec_only = sum(n * (sec_old_gen & !prim_old_gen)) / hh,
              old_gen_any = sum(n * (prim_old_gen | sec_old_gen)) / hh,
              same_gen_sec = sum(n * sec_same_gen) / hh,
              .by = c(y, bldg, ref_age_band))
gen_da_all <- dk |>
    summarise(hh = sum(n),
              child_gen_prim = sum(n * prim_child_gen) / hh, child_gen_sec_only = sum(n * (sec_child_gen & !prim_child_gen)) / hh,
              child_gen_any = sum(n * (prim_child_gen | sec_child_gen)) / hh,
              old_gen_prim = sum(n * prim_old_gen) / hh, old_gen_sec_only = sum(n * (sec_old_gen & !prim_old_gen)) / hh,
              old_gen_any = sum(n * (prim_old_gen | sec_old_gen)) / hh,
              same_gen_sec = sum(n * sec_same_gen) / hh,
              .by = c(y, bldg)) |> mutate(ref_age_band = "all")
gen_acs <- bind_rows(acs, acs |> mutate(ref_age_band = "all")) |>
    summarise(acs_hh = sum(hh), acs_child_gen = sum(child_gen) / acs_hh, acs_old_gen = sum(old_gen) / acs_hh,
              acs_adult_child = sum(adult_child) / acs_hh, acs_child_in_law_or_grandchild = sum(child_in_law_or_grandchild) / acs_hh,
              acs_parent_or_in_law = sum(parent_or_in_law) / acs_hh, .by = c(y, bldg, ref_age_band))
gen <- inner_join(bind_rows(gen_da, gen_da_all), gen_acs, by = c("y", "bldg", "ref_age_band")) |>
    arrange(bldg, y, factor(ref_age_band, levels = c("all", BANDS)))
write_csv(gen, file.path(OUTPUT_DIR, "generations_da_vs_acs.csv"))

# Adults per primary family vs adults per ACS household (all years with both), all households and age-complete ones
da_size <- read_csv(file.path(OUTPUT_DIR, "hh_surnames.csv"), show_col_types = FALSE) |>
    filter(grp %in% c("sf_nounit", "unit")) |>
    mutate(bldg = if_else(grp == "sf_nounit", "single-family", "multi-unit"), adults = pmin(adults_prim, 5)) |>
    filter(adults >= 1) |>
    summarise(hh = sum(n), .by = c(y, bldg, adults)) |> mutate(share = hh / sum(hh), .by = c(y, bldg)) |> mutate(source = "DA primary family")
acs_size <- read_csv(file.path(OUTPUT_DIR, "acs_hh_adults_relationships.csv"), show_col_types = FALSE) |>
    filter(bldg != "other", adults >= 1) |> mutate(adults = pmin(adults, 5)) |>
    summarise(hh = sum(hh), .by = c(y, bldg, adults)) |> mutate(share = hh / sum(hh), .by = c(y, bldg)) |> mutate(source = "ACS adults")
size <- bind_rows(da_size, acs_size) |> filter(y %in% intersect(da_size$y, acs_size$y))
write_csv(size, file.path(OUTPUT_DIR, "primary_family_adults_vs_acs.csv"))
size_wide <- size |> select(y, bldg, adults, source, share) |> pivot_wider(names_from = source, values_from = share) |>
    mutate(diff = `DA primary family` - `ACS adults`)
mean_tab <- size |> summarise(mean = sum(adults * hh) / sum(hh), .by = c(y, bldg, source)) |>
    pivot_wider(names_from = source, values_from = mean)
write_csv(mean_tab, file.path(OUTPUT_DIR, "primary_family_adults_mean_vs_acs.csv"))

p <- ggplot(size_wide, aes(y, diff * 100, colour = factor(adults, labels = c("1", "2", "3", "4", "5+")))) +
    geom_hline(yintercept = 0, colour = "grey60") + geom_line() + geom_point(size = 1) +
    facet_wrap(~bldg) +
    labs(x = NULL, y = "DA primary family − ACS household, pp", colour = "Adults",
         title = "Adults per household: Data Axle primary family minus ACS (adults 18+), by year",
         caption = "Single-family = no unit number, location type S / 1-family detached or attached; multi-unit = unit number / 2+ units.") +
    theme_minimal(base_size = 11) + theme(legend.position = "top", plot.caption = element_text(hjust = 0))
ggsave(file.path(OUTPUT_DIR, "primary_family_adults_vs_acs.png"), p, width = 11, height = 5, dpi = 150, bg = "white", device = ragg::agg_png)

pg <- gen |> filter(ref_age_band != "all") |>
    select(y, bldg, ref_age_band, child_gen_prim, child_gen_sec_only, acs_child_gen, old_gen_prim, old_gen_sec_only, acs_old_gen) |>
    pivot_longer(-c(y, bldg, ref_age_band)) |>
    mutate(gen = if_else(str_detect(name, "child"), "Adult ≥ 18 years younger (child generation)", "Adult ≥ 18 years older (parent generation)"),
           src = case_when(str_detect(name, "^acs") ~ "ACS", str_detect(name, "prim") ~ "DA: in primary family",
                           TRUE ~ "DA: only in a secondary family"),
           src = factor(src, levels = c("DA: only in a secondary family", "DA: in primary family", "ACS")),
           x = if_else(src == "ACS", "ACS", "Data Axle"))
p2 <- ggplot(pg |> filter(y == 2019), aes(x, value, fill = src)) +
    geom_col(width = 0.7) +
    facet_grid(gen ~ bldg + ref_age_band, labeller = labeller(gen = label_wrap_gen(22))) +
    scale_y_continuous(labels = scales::label_percent()) +
    scale_fill_manual(values = c("DA: only in a secondary family" = "#DD8452", "DA: in primary family" = "#4C72B0", "ACS" = "grey40")) +
    labs(x = NULL, y = "Share of households", fill = NULL,
         title = "Other generations in the household, 2019: Data Axle (primary vs secondary families) vs ACS, by age of reference person",
         caption = paste("Reference = Data Axle primary person 1 / ACS householder. Data Axle households with all ages known only.",
                         "ACS: any adult other than the householder with the age gap, any relationship.", sep = "\n")) +
    theme_minimal(base_size = 10) + theme(legend.position = "top", plot.caption = element_text(hjust = 0), axis.text.x = element_text(angle = 30, hjust = 1))
ggsave(file.path(OUTPUT_DIR, "generations_da_vs_acs_2019.png"), p2, width = 15, height = 7, dpi = 150, bg = "white", device = ragg::agg_png)

print(cov)
print(gen |> filter(ref_age_band == "all") |> mutate(across(where(is.double) & !c(hh, acs_hh), ~ round(., 3))), width = Inf)
print(gen |> filter(y == 2019, bldg == "single-family") |> mutate(across(where(is.double) & !c(hh, acs_hh), ~ round(., 3))), width = Inf)
print(mean_tab, n = 40)
print(size_wide |> filter(y %in% c(2012, 2019)) |> mutate(across(where(is.double) & !y, ~ round(., 3))), n = 40)
