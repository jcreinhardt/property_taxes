# NYC ZIP map, 2019: street-address overlap Data Axle vs CoreLogic tax, and near-miss ("typo") shares.
# Address = ZIP5 + street name + house number. Near miss for an unmatched address: the other source has an unmatched
# address with (i) same street + number in another ZIP with the same ZIP3, or (ii) same ZIP + number and a street name
# within 2 edits / Jaro-Winkler >= 0.92, or (iii) same ZIP + street and a house number one character off (typo_2019_full.sql).
# Geography: 2010 Census ZCTAs (tigris) for the 320 NYC ZIPs on the NYS DOH ZIP-county list.
# Run from the repo root.

# Clean up workspace
gc()
rm(list = ls())

# Load libraries
library(tidyverse)
library(sf)
library(tigris)
options(tigris_use_cache = TRUE)

# Per-script output folder
source("code/R/paths.R")
OUTPUT_DIR <- file.path(ROOT, "output/analysis/dataaxle/nyc_overlap_2019")
if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

z <- read_csv(file.path(OUTPUT_DIR, "typo_2019_nyc_zip.csv"), show_col_types = FALSE, col_types = "cdddddd") |>
    mutate(overlap      = if_else(da_addr + tax_addr > 0, both_addr / (da_addr + tax_addr - both_addr), NA_real_),
           da_missing   = if_else(da_addr > 0, 1 - both_addr / da_addr, NA_real_),
           tax_missing  = if_else(tax_addr > 0, 1 - both_addr / tax_addr, NA_real_),
           nearmiss     = if_else(da_addr + tax_addr - 2 * both_addr > 0,
                                  (da_unm_nearmiss + tax_unm_nearmiss) / (da_addr + tax_addr - 2 * both_addr), NA_real_))
write_csv(z, file.path(OUTPUT_DIR, "nyc_zip_overlap_2019.csv"))

shp <- zctas(year = 2010, state = "36", progress_bar = FALSE) |>
    rename(zip = ZCTA5CE10) |>
    filter(zip %in% z$zip)
boro <- counties(state = "36", cb = TRUE, year = 2019, progress_bar = FALSE) |>
    filter(COUNTYFP %in% c("005", "047", "061", "081", "085"))

m <- shp |>
    left_join(z, by = "zip") |>
    filter(da_addr + tax_addr >= 50) |>
    select(zip, overlap, da_missing, tax_missing, nearmiss) |>
    pivot_longer(-c(zip, geometry), names_to = "measure", values_to = "share") |>
    mutate(measure = factor(measure, levels = c("overlap", "da_missing", "tax_missing", "nearmiss"),
        labels = c("(a) Overlap: in both / in either",
                   "(b) Data Axle addresses missing in CoreLogic",
                   "(c) CoreLogic addresses missing in Data Axle",
                   "(d) Unmatched addresses with a near-miss partner")))

p <- ggplot(m) +
    geom_sf(data = boro, fill = "grey90", colour = NA) +
    geom_sf(aes(fill = share), colour = "white", linewidth = 0.1) +
    geom_sf(data = boro, fill = NA, colour = "grey30", linewidth = 0.3) +
    facet_wrap(~measure, ncol = 2) +
    scale_fill_viridis_c(labels = scales::percent, limits = c(0, 1), na.value = "grey85", option = "magma", direction = -1) +
    labs(fill = NULL, title = "New York City by ZIP, 2019: street addresses in Data Axle vs CoreLogic tax",
         caption = paste0("Address = ZIP5 + street name + house number. ZIPs with fewer than 50 addresses in both sources omitted (grey).\n",
                          "Near miss: other source has an unmatched address with a ZIP, street-name or house-number one small edit away.")) +
    theme_void(base_size = 11) +
    theme(legend.key.height = unit(1.2, "cm"), plot.caption = element_text(hjust = 0))
ggsave(file.path(OUTPUT_DIR, "nyc_overlap_2019.png"), p, width = 11, height = 11, dpi = 150, bg = "white")
