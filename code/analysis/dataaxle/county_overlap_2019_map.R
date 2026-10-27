# County maps, 2019: overlap of street-address universes, Data Axle vs CoreLogic tax.
# Address = ZIP5 + street name + house number (hashed; see county_2019_full.sql).
# Data Axle addresses are assigned to counties by ge_census_state_2010 * 1000 + ge_als_county_code_2010,
# tax addresses by fips_code. Matched addresses: counted in Data Axle's county for (a) and (b), tax county for (c).
#   (a) overlap        = matched / (DA + CL - matched)
#   (b) DA missing in CL = 1 - DA matched / DA
#   (c) CL missing in DA = 1 - CL matched / CL   (all tax addresses, and residential parcels only: codes 10, 11, 21, 22)
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
OUTPUT_DIR <- file.path(ROOT, "output/analysis/dataaxle/county_overlap_2019")
if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

da <- read_csv(file.path(OUTPUT_DIR, "county_2019_da.csv"), show_col_types = FALSE)
cl <- read_csv(file.path(OUTPUT_DIR, "county_2019_cl.csv"), show_col_types = FALSE)

cty <- full_join(da, cl, by = "fips") |>
    filter(!is.na(fips)) |>
    mutate(across(c(da_addr, da_in_cl, cl_addr, cl_in_da, cl_res_addr, cl_res_in_da), ~ replace_na(., 0)),
           overlap         = if_else(da_addr + cl_addr > 0, da_in_cl / (da_addr + cl_addr - da_in_cl), NA_real_),
           da_missing_cl   = if_else(da_addr > 0, 1 - da_in_cl / da_addr, NA_real_),
           cl_missing_da   = if_else(cl_addr > 0, 1 - cl_in_da / cl_addr, NA_real_),
           clres_missing_da = if_else(cl_res_addr > 0, 1 - cl_res_in_da / cl_res_addr, NA_real_),
           GEOID = sprintf("%05d", fips))
write_csv(cty, file.path(OUTPUT_DIR, "county_overlap_2019.csv"))

shp <- counties(cb = TRUE, resolution = "20m", year = 2019) |>
    filter(!STATEFP %in% c("60", "66", "69", "72", "78")) |>
    shift_geometry()
st <- states(cb = TRUE, resolution = "20m", year = 2019) |>
    filter(!STATEFP %in% c("60", "66", "69", "72", "78")) |>
    shift_geometry()

m <- shp |>
    left_join(cty, by = "GEOID") |>
    select(GEOID, overlap, da_missing_cl, cl_missing_da, clres_missing_da) |>
    pivot_longer(-c(GEOID, geometry), names_to = "measure", values_to = "share") |>
    mutate(measure = factor(measure,
        levels = c("overlap", "da_missing_cl", "cl_missing_da", "clres_missing_da"),
        labels = c("(a) Overlap: in both / in either",
                   "(b) Share of Data Axle addresses missing in CoreLogic",
                   "(c) Share of CoreLogic addresses missing in Data Axle",
                   "(c') Same, CoreLogic residential parcels only")))

p <- ggplot(m) +
    geom_sf(aes(fill = share), colour = NA) +
    geom_sf(data = st, fill = NA, colour = "white", linewidth = 0.2) +
    facet_wrap(~measure, ncol = 2) +
    scale_fill_viridis_c(labels = scales::percent, limits = c(0, 1), na.value = "grey85", option = "magma", direction = -1) +
    labs(fill = NULL, title = "Street-address universes by county, 2019: Data Axle vs CoreLogic tax",
         caption = paste0("Address = ZIP5 + street name + house number. Grey = county absent from the source(s) used in the panel.\n",
                          "Data Axle county: ge_als_county_code_2010; tax county: fips_code. Residential = property indicator 10, 11, 21, 22.")) +
    theme_void(base_size = 11) +
    theme(legend.position = "right", legend.key.height = unit(1.2, "cm"), plot.caption = element_text(hjust = 0),
          strip.text = element_text(size = 10, margin = margin(b = 4)))
ggsave(file.path(OUTPUT_DIR, "county_overlap_2019.png"), p, width = 13, height = 8.5, dpi = 150, bg = "white")
