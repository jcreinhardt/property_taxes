# Unit-number histogram, 2019: Data Axle family records vs CoreLogic tax parcel records.
# Top 10 unit values per source, all other non-null values pooled, NULL/blank as its own category.
# Unit = upper(trim(unit_num)) (Data Axle, record_year 2019) / upper(trim(situs_unit_number)) (tax, tax_year 2019).
# Input: full-data counts from Bouchet batch job (units_2019_full.sql). Run from the repo root.

# Clean up workspace
gc()
rm(list = ls())

# Load libraries
library(tidyverse)

# Per-script output folder
source("code/R/paths.R")
OUTPUT_DIR <- file.path(ROOT, "output/analysis/dataaxle/units_2019_dataaxle_vs_tax")
if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

SRC <- c(dataaxle_family_records = "Data Axle 2019 (family records)", tax_parcel_records = "CoreLogic tax 2019 (parcel records)")

h <- read_csv(file.path(OUTPUT_DIR, "units_2019_full.csv"), show_col_types = FALSE, col_types = "ccdi")
n_distinct_units <- h |> filter(rk == 13) |> mutate(src = SRC[src])
h <- h |>
    filter(rk <= 12) |>
    group_by(src) |>
    mutate(pct = 100 * n / sum(n), total = sum(n)) |>
    ungroup() |>
    mutate(src_lab = sprintf("%s\nN = %.1fm, %s distinct non-null units", SRC[src], total / 1e6,
                             format(n_distinct_units$n[match(SRC[src], n_distinct_units$src)], big.mark = ",")),
           cat = case_when(rk == 12 ~ "NULL / blank", rk == 11 ~ "Other non-null", TRUE ~ "Top 10 unit value"),
           unit = paste0(src, "__", unit))
write_csv(h |> select(src, unit_key = unit, n, pct), file.path(OUTPUT_DIR, "units_2019_shares.csv"))

p <- ggplot(h, aes(pct, fct_reorder(unit, -rk), fill = cat)) +
    geom_col(width = 0.75) +
    geom_text(aes(label = ifelse(pct >= 0.1, sprintf("%.1f%%", pct), sprintf("%.2f%%", pct))), hjust = -0.1, size = 3) +
    facet_wrap(~src_lab, scales = "free_y") +
    scale_y_discrete(labels = function(x) sub(".*__", "", x)) +
    scale_x_continuous(expand = expansion(mult = c(0, 0.15))) +
    scale_fill_manual(values = c("NULL / blank" = "grey60", "Other non-null" = "#8172B3", "Top 10 unit value" = "#4C72B0")) +
    labs(x = "% of records", y = "Unit number", fill = NULL,
         title = "Unit numbers in 2019: Data Axle vs CoreLogic tax records",
         caption = "Full data. Unit = upper(trim(unit field)); blank = NULL. Data Axle: one record per family; tax: one record per parcel row with tax_year 2019.") +
    theme_minimal(base_size = 11) +
    theme(legend.position = "top", panel.grid.major.y = element_blank(), plot.caption = element_text(hjust = 0))
ggsave(file.path(OUTPUT_DIR, "units_2019_dataaxle_vs_tax.png"), p, width = 11, height = 5, dpi = 150, bg = "white")

# Union of both top-10 lists (1-6, A-D, 101), same categories for both sources (units_2019_union.sql)
LEVELS <- c("1", "2", "3", "4", "5", "6", "A", "B", "C", "D", "101", "(other non-null)", "(NULL/blank)")
u <- read_csv(file.path(OUTPUT_DIR, "units_2019_union.csv"), show_col_types = FALSE, col_types = "ccd") |>
    group_by(src) |>
    mutate(pct = 100 * n / sum(n), pct_nonnull = if_else(unit == "(NULL/blank)", NA_real_, 100 * n / sum(n[unit != "(NULL/blank)"]))) |>
    ungroup() |>
    mutate(src = factor(SRC[src], levels = SRC), unit = factor(unit, levels = LEVELS))
write_csv(u, file.path(OUTPUT_DIR, "units_2019_union_shares.csv"))

p_u <- ggplot(u, aes(unit, pct, fill = src)) +
    geom_col(position = position_dodge(width = 0.8), width = 0.75) +
    geom_text(aes(label = sprintf("%.2f", pct)), position = position_dodge(width = 0.8), vjust = -0.3, size = 2.6) +
    scale_y_sqrt(breaks = c(0.1, 0.5, 1, 5, 10, 25, 50, 75, 100)) +
    scale_fill_manual(values = c("#DD8452", "#4C72B0")) +
    labs(x = "Unit number", y = "% of records (square-root scale)", fill = NULL,
         title = "Unit numbers in 2019: union of each source's top-10 values",
         caption = "Full data. Unit = upper(trim(unit field)); blank = NULL. Data Axle: family records; tax: parcel rows with tax_year 2019.") +
    theme_minimal(base_size = 11) +
    theme(legend.position = "top", panel.grid.major.x = element_blank(), plot.caption = element_text(hjust = 0))
ggsave(file.path(OUTPUT_DIR, "units_2019_union.png"), p_u, width = 11, height = 5, dpi = 150, bg = "white")
