# Clean and load
gc()
rm(list = ls())
getwd()

# Load libraries
library(duckdb)
library(tidycensus)
library(data.table); setDTthreads(10)
library(dplyr)
library(tidyr)
library(tidyverse)
library(purrr)
library(ggplot2)
library(readxl)

# Import HUD zipcode to county boundaries crosswalk
crosswalk <- read_excel("../../data/hud_zip_to_county_crosswalk.xlsx") |>
    transmute(
        zip = str_pad(ZIP, 5, pad = "0"),
        county_fips = str_pad(COUNTY, 5, pad = "0"),
        res_ratio = RES_RATIO
    )

# Import zipcode-level data on itemization
zipcode_data <- read.csv("../../data/zipcode_SOI/transactions_w_itemizations.csv") |>
    mutate(zip = str_pad(as.character(zipcode), 5, pad = "0")) |>
    filter(!is.na(tax_year) & !is.na(baseline)) #Using some tax years bc of incomplete corelogic data, SOI disclosures limit itemization information.

setdiff(crosswalk$zip, zipcode_data$zip) # These are expected
setdiff(zipcode_data$zip, crosswalk$zip)

# Merge to county level
county_data <- crosswalk |>
    expand_grid(tax_year = 2012:2022) |>
    left_join(zipcode_data, by = c("zip", "tax_year")) |>
    reframe(
        share_transacted = sum(share_transacted * res_ratio, na.rm = TRUE),
        baseline = sum(baseline * res_ratio, na.rm = TRUE),
        change = sum(change * res_ratio, na.rm = TRUE),
        .by = c(county_fips, tax_year)
    )

# Import migration data
migration_data <- read.csv("../../data/migration_SOI/net_migration.csv") |> 
    mutate(
        county_fips = str_pad(fips, 5, pad = "0"),
        tax_year = year + 2000
    ) |>
    select(-fips, -year)

# Merge to county level
county_data <- county_data |>
    left_join(migration_data, by = c("county_fips", "tax_year"))

# Add state FIPS
county_data <- county_data |>
    mutate(state = as.numeric(substr(county_fips, 1, 2)))

# Trends in net_migration by exposure
county_data |>
    filter(tax_year %in% c(2012:2022)) |>
    mutate(group = ntile(baseline, 5)) |>
    reframe(avg = mean(net_migration_filers, na.rm = TRUE), .by = c(group, tax_year)) |>
    filter(!is.na(group) & tax_year < 2021) |>
    ggplot(aes(x = tax_year, y = avg, color = as.factor(group), group = as.factor(group))) +
    geom_line() +
    geom_point() +
    geom_vline(xintercept = 2017, linetype = "dashed", color = "red") +
    labs(title = "Trends in Net Migration by Baseline Exposure", x = "Tax Year", y = "Net Migration", color = "Baseline Exposure") +
    theme_classic()

county_data |>
    filter(tax_year %in% c(2012:2022)) |>
    mutate(group = ntile(change, 5)) |>
    reframe(avg = mean(net_migration_filers, na.rm = TRUE), .by = c(group, tax_year)) |>
    filter(!is.na(group) & tax_year < 2021) |>
    ggplot(aes(x = tax_year, y = avg, color = as.factor(group), group = as.factor(group))) +
    geom_line() +
    geom_point() +
    geom_vline(xintercept = 2017, linetype = "dashed", color = "red") +
    labs(title = "Trends in Net Migration by Realized Itemization Change", x = "Tax Year", y = "Net Migration", color = "Realized Itemization Change") +
    theme_classic()

# Trends net of state-year fixed effects
county_data |>
    filter(tax_year %in% c(2012:2022)) |>
    mutate(group = ntile(change, 5)) |>
    mutate(resid = net_migration_filers - mean(net_migration_filers, na.rm = TRUE), .by = c(state, tax_year)) |>
    reframe(avg = mean(resid, na.rm = TRUE), .by = c(group, tax_year)) |>
    filter(!is.na(group) & tax_year < 2021) |>
    ggplot(aes(x = tax_year, y = avg, color = as.factor(group), group = as.factor(group))) +
    geom_line() +
    geom_point() +
    geom_vline(xintercept = 2017, linetype = "dashed", color = "red") +
    labs(title = "Trends in Net Migration by Realized Itemization Change", x = "Tax Year", y = "Net Migration", color = "Realized Itemization Change") +
    theme_classic()

county_data |>
    filter(tax_year %in% c(2012:2022)) |>
    mutate(group = ntile(baseline, 5)) |>
    mutate(resid = net_migration_filers - mean(net_migration_filers, na.rm = TRUE), .by = c(state, tax_year)) |>
    reframe(avg = mean(resid, na.rm = TRUE), .by = c(group, tax_year)) |>
    filter(!is.na(group) & tax_year < 2021) |>
    ggplot(aes(x = tax_year, y = avg, color = as.factor(group), group = as.factor(group))) +
    geom_line() +
    geom_point() +
    geom_vline(xintercept = 2017, linetype = "dashed", color = "red") +
    labs(title = "Trends in Net Migration by Baseline Exposure", x = "Tax Year", y = "Net Migration", color = "Baseline Exposure") +
    theme_classic()

# Regression based versions
county_data |>
    filter(tax_year %in% c(2012:2022)) |>
    feols(
        net_migration_filers ~ i(tax_year, baseline, ref = 2017) | state^tax_year + county_fips, 
        cluster = ~ county_fips) |>
    iplot()

county_data |>
    filter(tax_year %in% c(2012:2022)) |>
    feols(net_migration_filers ~ i(tax_year, change, ref = 2017) | state^tax_year + county_fips, 
    cluster = ~ county_fips) |>
    iplot()

# Repeat, but look at inmigration and outmigration separately
county_data |>
    filter(tax_year %in% c(2012:2022)) |>
    mutate(percent = inmigration_filers / inmigration_pop) |> 
    feols(percent ~ i(tax_year, baseline, ref = 2017) | state^tax_year + county_fips, 
    cluster = ~ county_fips) |>
    iplot()

county_data |>
    filter(tax_year %in% c(2012:2022)) |>
    mutate(percent = inmigration_filers / inmigration_pop) |> 
    feols(percent ~ i(tax_year, change, ref = 2017) | state^tax_year + county_fips, 
    cluster = ~ county_fips) |>
    iplot()

county_data |>
    filter(tax_year %in% c(2012:2022)) |>
    mutate(percent = outmigration_filers / outmigration_pop) |> 
    feols(percent ~ i(tax_year, baseline, ref = 2017) | state^tax_year + county_fips, 
    cluster = ~ county_fips) |>
    iplot()

county_data |>
    filter(tax_year %in% c(2012:2022)) |>
    mutate(percent = outmigration_filers / outmigration_pop) |> 
    feols(percent ~ i(tax_year, change, ref = 2017) | state^tax_year + county_fips, 
    cluster = ~ county_fips) |>
    iplot()
