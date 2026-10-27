# Street-level match shares, 2019: Data Axle vs CoreLogic tax.
# Street = ZIP5 + street name; address = street + house number (loose key, see streets_2019_full.sql).
# Per street: DA missing in CL = DA-only / DA addresses; CL missing in DA = tax-only / tax addresses.
# If non-matches are mostly the same properties recorded under different numbers, a street with DA-only
# addresses should also have tax-only addresses, in similar numbers.
# Input: streets_2019.parquet (one row per street) from the cluster. Run from the repo root.

# Clean up workspace
gc()
rm(list = ls())

# Load libraries
library(tidyverse)
library(duckdb)
library(patchwork)

# Per-script output folder
source("code/R/paths.R")
OUTPUT_DIR <- file.path(ROOT, "output/analysis/dataaxle/streets_2019_missing_shares")
if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

con <- dbConnect(duckdb())
st <- dbGetQuery(con, sprintf("SELECT * FROM '%s'", file.path(OUTPUT_DIR, "streets_2019.parquet"))) |> as_tibble()
dbDisconnect(con, shutdown = TRUE)

st <- st |>
    mutate(da_only = n_da - n_both, tax_only = n_tax - n_both,
           tax_only_nonres = tax_only - n_tax_only_res,
           in_both = n_da > 0 & n_tax > 0) |>
    group_by(z) |> mutate(zip_in_tax = sum(n_tax) > 0) |> ungroup()

# Sample loss from keeping streets with > 10 addresses (union of both sources)
loss <- st |>
    mutate(kept = n_union > 10) |>
    summarise(streets = n(), addresses = sum(n_union), da_addresses = sum(n_da), tax_addresses = sum(n_tax),
              da_only = sum(da_only), tax_only = sum(tax_only), .by = kept) |>
    mutate(across(-kept, ~ . / sum(.), .names = "{.col}_share"))
write_csv(loss, file.path(OUTPUT_DIR, "streets_2019_loss.csv"))
print(loss)

big <- st |> filter(n_union > 10) |>
    mutate(da_miss  = if_else(n_da > 0, da_only / n_da, NA_real_),
           cl_miss  = if_else(n_tax > 0, tax_only / n_tax, NA_real_),
           diff     = da_miss - cl_miss,
           balance  = (da_only - tax_only) / n_union,
           group    = case_when(!zip_in_tax ~ "ZIP not in tax",
                                n_tax == 0 ~ "Street only in Data Axle",
                                n_da == 0 ~ "Street only in tax",
                                TRUE ~ "Street in both"))

# How much of the non-matching could pair up within the street? min(DA-only, tax-only) per street
pairs <- big |>
    summarise(streets = n(),
              pairable = sum(pmin(da_only, tax_only)), pairable_res = sum(pmin(da_only, n_tax_only_res)),
              da_only = sum(da_only), tax_only = sum(tax_only), tax_only_res = sum(n_tax_only_res),
              .by = group) |>
    mutate(pairable_share_of_da_only = pairable / da_only, pairable_share_of_tax_only = pairable / tax_only,
           pairable_res_share_of_da_only = pairable_res / da_only, pairable_res_share_of_tax_only_res = pairable_res / tax_only_res)
write_csv(pairs, file.path(OUTPUT_DIR, "streets_2019_pairable.csv"))
print(pairs)

both <- big |> filter(group == "Street in both")
hist_tab <- both |>
    transmute(da_miss = round(da_miss, 2), cl_miss = round(cl_miss, 2), diff = round(diff, 2)) |>
    pivot_longer(everything(), names_to = "measure", values_to = "value") |>
    count(measure, value)
write_csv(hist_tab, file.path(OUTPUT_DIR, "streets_2019_hist.csv"))

summ <- both |> summarise(streets = n(),
                          da_miss_zero = mean(da_miss == 0), cl_miss_zero = mean(cl_miss == 0), both_zero = mean(da_miss == 0 & cl_miss == 0),
                          da_miss_pos_cl_pos = mean(da_miss > 0 & cl_miss > 0), da_miss_pos_cl_zero = mean(da_miss > 0 & cl_miss == 0),
                          da_miss_zero_cl_pos = mean(da_miss == 0 & cl_miss > 0),
                          abs_diff_le_10pp = mean(abs(diff) <= 0.1), cor = cor(da_miss, cl_miss))
write_csv(summ, file.path(OUTPUT_DIR, "streets_2019_summary.csv"))
print(summ)

h <- function(var, lab) {
    ggplot(both, aes(x = {{ var }})) +
        geom_histogram(binwidth = 0.02, boundary = 0, fill = "grey30") +
        scale_y_continuous(labels = scales::label_comma()) +
        labs(x = lab, y = "Streets") + theme_minimal(base_size = 11)
}
p1 <- h(da_miss, "Share of Data Axle addresses missing in CoreLogic")
p2 <- h(cl_miss, "Share of CoreLogic addresses missing in Data Axle")
p3 <- ggplot(both, aes(x = diff)) +
    geom_histogram(binwidth = 0.02, center = 0, fill = "grey30") +
    scale_y_continuous(labels = scales::label_comma()) +
    labs(x = "DA missing share − CL missing share", y = "Streets") + theme_minimal(base_size = 11)
p4 <- ggplot(both, aes(x = da_miss, y = cl_miss)) +
    geom_bin2d(binwidth = c(0.05, 0.05)) +
    scale_fill_viridis_c(trans = "log10", labels = scales::label_comma()) +
    coord_equal() +
    labs(x = "DA missing in CL", y = "CL missing in DA", fill = "Streets") + theme_minimal(base_size = 11)
p <- (p1 + p2) / (p3 + p4) +
    plot_annotation(title = "Streets (ZIP + street name) with > 10 addresses, in both sources, 2019",
                    subtitle = sprintf("%s streets", scales::comma(nrow(both))))
ggsave(file.path(OUTPUT_DIR, "streets_2019_missing_shares.png"), p, width = 11, height = 9, dpi = 150, bg = "white")
