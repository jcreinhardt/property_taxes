# Are Data Axle secondary families different-surname members of the primary family's household?
# Pool all family records (primary + secondary) at one building x unit and year into a "household" and count
# distinct surnames among its listed adults. Compare, by number of adults, with what ACS household composition implies.
# Data Axle: hh_surnames.sql (Bouchet, full data, 2006-2023 without 2022), output hh_surnames.csv.
# ACS: IPUMS extract data/ACS_microdata/usa_00004.csv.gz (2011-2023), persons 18+ in households (GQ 1, 2), HHWT.
# Surnames are not in the ACS, so the benchmark is built from relationships to the householder:
#   lower  = household has an adult nonrelative (partner, roommate, boarder, other nonrelative)
#   central = 1 - prod(1 - p_rel) over adults, with p_rel = assumed chance that an adult with relationship rel has a
#             surname different from the householder's (P_DIFF below)
#   upper  = household has any adult other than householder, spouse and own/adopted child; else 25% if a spouse is present
# Run from code/analysis/dataaxle/.

# Clean up workspace
gc()
rm(list = ls())

# Load libraries
library(tidyverse)
library(duckdb)

# Per-script output folder
OUTPUT_DIR <- "../../../output/analysis/dataaxle/hh_surnames"
if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

# Assumed probability that an adult's surname differs from the householder's, by IPUMS RELATED code
P_DIFF <- c(`201` = 0.20,                            # spouse: wife kept her name / husband did not take hers
            `301` = 0.03, `302` = 0.10, `303` = 0.60,  # own, adopted, stepchild
            `401` = 0.50,                            # child-in-law (daughter's husband differs, son's wife usually not)
            `501` = 0.40, `601` = 0.70,              # parent (mother of married female householder), parent-in-law
            `701` = 0.30, `801` = 0.70,              # sibling (married sister), sibling-in-law
            `901` = 0.40, `1001` = 0.60,             # grandchild, other relative
            `1114` = 0.95, `1115` = 0.95, `1241` = 0.95, `1242` = 0.95, `1260` = 0.95)  # nonrelatives

# ACS benchmark (cached; the extract is 2.5 GB) ---------------------------------------------
acs_file <- file.path(OUTPUT_DIR, "acs_hh_adults_relationships.csv")
if (!file.exists(acs_file)) {
    con <- dbConnect(duckdb())
    dbExecute(con, sprintf("CREATE TABLE p AS SELECT * FROM (VALUES %s) t(rel_code, p)",
                           paste(sprintf("(%s, %s)", names(P_DIFF), P_DIFF), collapse = ", ")))
    acs <- dbGetQuery(con, "
        WITH a AS (
            SELECT YEAR, SERIAL, any_value(HHWT) hhwt, any_value(UNITSSTR) unitsstr,
                   count(*) FILTER (WHERE AGE >= 18) adults,
                   bool_or(AGE >= 18 AND x.RELATE IN (11, 12)) nonrel,
                   bool_or(AGE >= 18 AND x.RELATED NOT IN (101, 201, 301, 302)) other_rel,
                   bool_or(AGE >= 18 AND x.RELATED = 201) spouse,
                   exp(sum(ln(1 - coalesce(p.p, 0))) FILTER (WHERE AGE >= 18)) p_same
            FROM read_csv('../../../data/ACS_microdata/usa_00004.csv.gz') x LEFT JOIN p ON p.rel_code = x.RELATED
            WHERE GQ IN (1, 2)
            GROUP BY YEAR, SERIAL)
        SELECT YEAR AS y,
               CASE WHEN unitsstr IN (3, 4) THEN 'single-family' WHEN unitsstr >= 5 THEN 'multi-unit' ELSE 'other' END bldg,
               least(adults, 10) adults, sum(hhwt) hh,
               sum(hhwt * nonrel::INT) / sum(hhwt) lower, sum(hhwt * (1 - p_same)) / sum(hhwt) central,
               sum(hhwt * (1 - (1 - 0.25 * spouse::INT) * (1 - other_rel::INT))) / sum(hhwt) upper
        FROM a GROUP BY ALL ORDER BY ALL")
    dbDisconnect(con, shutdown = TRUE)
    write_csv(acs, acs_file)
}
acs <- read_csv(acs_file, show_col_types = FALSE)

# Data Axle ---------------------------------------------------------------------------------
# grp: sf_nounit = no unit number, all families location_type S, exactly one primary family; unit = unit number,
# one primary; multi_primary / no_primary / other_nounit are not households in this sense.
da <- read_csv(file.path(OUTPUT_DIR, "hh_surnames.csv"), show_col_types = FALSE)

# Who is in which group, by year
groups <- da |> summarise(n = sum(n), .by = c(y, grp)) |> mutate(share = n / sum(n), .by = y) |> arrange(y, grp)
write_csv(groups, file.path(OUTPUT_DIR, "da_groups_by_year.csv"))

hh <- da |> filter(grp %in% c("sf_nounit", "unit")) |>
    mutate(bldg = if_else(grp == "sf_nounit", "single-family", "multi-unit"))

# Distribution of adults per household: primary family alone vs pooled vs ACS
dist <- bind_rows(
    hh |> mutate(adults = pmin(adults_prim, 5)) |> summarise(hh = sum(n), .by = c(y, bldg, adults)) |> mutate(source = "DA primary family"),
    hh |> mutate(adults = pmin(adults, 5)) |> summarise(hh = sum(n), .by = c(y, bldg, adults)) |> mutate(source = "DA pooled household"),
    acs |> filter(bldg != "other") |> mutate(adults = pmin(adults, 5)) |> summarise(hh = sum(hh), .by = c(y, bldg, adults)) |> mutate(source = "ACS")) |>
    filter(adults >= 1) |>
    mutate(share = hh / sum(hh), .by = c(source, y, bldg))
write_csv(dist, file.path(OUTPUT_DIR, "adults_distribution.csv"))

# Share with more than one surname, by number of adults
da_sn <- hh |> filter(adults >= 1) |> mutate(adults = pmin(adults, 6)) |>
    summarise(da_pooled = sum(n * (nsn > 1)) / sum(n), da_two_plus_families = sum(n * (nfam > 1)) / sum(n),
              da_primary_only = sum(n * (nsn_prim > 1)) / sum(n), da_hh = sum(n),
              .by = c(y, bldg, adults))
acs_sn <- acs |> filter(bldg != "other", adults >= 1) |> mutate(adults = pmin(adults, 6)) |>
    summarise(across(c(lower, central, upper), ~ sum(. * hh) / sum(hh)), acs_hh = sum(hh), .by = c(y, bldg, adults))
cmp <- full_join(da_sn, acs_sn, by = c("y", "bldg", "adults")) |> arrange(bldg, y, adults)
write_csv(cmp, file.path(OUTPUT_DIR, "surnames_by_adults_da_vs_acs.csv"))

# Figures: 2019 and pooled over years with both sources
yrs <- intersect(unique(da_sn$y), unique(acs_sn$y))
fig <- cmp |> filter(y %in% c(2012, 2017, 2019, 2023), adults >= 2) |>
    mutate(adults = if_else(adults == 6, "6+", as.character(adults)))
p <- ggplot(fig, aes(x = adults, group = 1)) +
    geom_ribbon(aes(ymin = lower, ymax = upper, fill = "ACS benchmark (lower–upper)"), alpha = 0.35) +
    geom_line(aes(y = central, colour = "ACS benchmark (central)"), linetype = "dashed") +
    geom_line(aes(y = da_pooled, colour = "Data Axle pooled household")) +
    geom_point(aes(y = da_pooled, colour = "Data Axle pooled household")) +
    geom_line(aes(y = da_primary_only, colour = "Data Axle primary family only")) +
    facet_grid(bldg ~ y) +
    scale_y_continuous(labels = scales::label_percent(), limits = c(0, 1)) +
    scale_colour_manual(values = c("ACS benchmark (central)" = "grey20", "Data Axle pooled household" = "#C44E52",
                                   "Data Axle primary family only" = "#4C72B0")) +
    scale_fill_manual(values = c("ACS benchmark (lower–upper)" = "grey60")) +
    labs(x = "Adults in household", y = "Households with more than one surname", colour = NULL, fill = NULL,
         title = "Do Data Axle secondary families complete the primary household? Surname mix by household size",
         caption = paste("Data Axle: all family records at a building × unit with exactly one primary family; adults = persons 1–5 of each family.",
                         "Single-family = no unit number, location type S; multi-unit = has a unit number. ACS (IPUMS): households, adults 18+;",
                         "single-family = 1-family detached/attached, multi-unit = 2+ units. Lower = any adult nonrelative; upper = any adult other",
                         "than householder, spouse, own/adopted child, or a spouse with 25%; central = assumed per-relationship probabilities.", sep = "\n")) +
    theme_minimal(base_size = 11) + theme(legend.position = "top", plot.caption = element_text(hjust = 0, size = 8))
ggsave(file.path(OUTPUT_DIR, "surnames_by_adults_da_vs_acs.png"), p, width = 13, height = 7.5, dpi = 150, bg = "white",
       device = ragg::agg_png)

p2 <- ggplot(dist |> filter(y %in% c(2012, 2017, 2019, 2023)),
             aes(x = factor(adults, labels = c("1", "2", "3", "4", "5+")), y = share, fill = source)) +
    geom_col(position = position_dodge(width = 0.8), width = 0.75) +
    facet_grid(bldg ~ y) +
    scale_y_continuous(labels = scales::label_percent()) +
    scale_fill_manual(values = c("ACS" = "grey40", "DA primary family" = "#4C72B0", "DA pooled household" = "#C44E52")) +
    labs(x = "Adults in household", y = "Share of households", fill = NULL,
         title = "Adults per household: Data Axle primary family vs pooled household vs ACS") +
    theme_minimal(base_size = 11) + theme(legend.position = "top")
ggsave(file.path(OUTPUT_DIR, "adults_distribution.png"), p2, width = 13, height = 7, dpi = 150, bg = "white",
       device = ragg::agg_png)

print(groups |> filter(y %in% c(2006, 2012, 2017, 2019, 2023)), n = 50)
print(dist |> filter(y == 2019) |> select(-hh) |> pivot_wider(names_from = source, values_from = share), n = 30)
print(cmp |> filter(y == 2019) |> mutate(across(where(is.double) & !c(da_hh, acs_hh), ~ round(., 3))), n = 30)
