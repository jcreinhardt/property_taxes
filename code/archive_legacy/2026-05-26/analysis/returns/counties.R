gc()
rm(list = ls())
getwd()

# Load libraries
library(dplyr)
library(stringr)
library(ggplot2)
library(tigris)
library(duckdb)
library(fixest)

# Load from database
con <- dbConnect(duckdb::duckdb(), dbdir = "../../../data/database.db")
county_returns <- dbGetQuery(con, "select * from returns_county_agi")

# Compute exposure measures
exposure_all <- county_returns |>
    reframe(
        avg_salt_per_filer = sum(state_and_local_income_taxes_amount, na.rm = TRUE) / sum(returns, na.rm = TRUE),
        .by = c(state, year)
    ) |>
    mutate(pre_period = if_else(year < 2018, 1, 0)) |>
    reframe(
        exposure_all = mean(avg_salt_per_filer[pre_period == 1]) -
            mean(avg_salt_per_filer[pre_period == 0]),
        .by = state
    )

exposure_top <- county_returns |>
    filter(agi_stub == max(agi_stub)) |>
    reframe(
        avg_salt_per_filer = sum(state_and_local_income_taxes_amount, na.rm = TRUE) / sum(returns, na.rm = TRUE),
        .by = c(state, year)
    ) |>
    mutate(pre_period = if_else(year < 2018, 1, 0)) |>
    reframe(
        exposure_top = mean(avg_salt_per_filer[pre_period == 1]) -
            mean(avg_salt_per_filer[pre_period == 0]),
        .by = state
    )

# Merge exposures
county_returns <- county_returns |>
    left_join(exposure_all, by = "state") |>
    left_join(exposure_top, by = "state")

# Add weights
county_returns <- county_returns |>
    mutate(
        w1 = sum(returns),
        w2 = sum(returns[agi_stub == max(agi_stub)]),
        .by = c(state, year)
    )

# TWFE of exposure on number of returns in different AGI classes

## AGI> $200,000
county_returns |>
    filter(agi_stub == 6) |>
    feols(
        returns ~ i(year, exposure_all, ref = 2017) - 1 | state,
        weights = ~ w1,
        cluster = ~ state
    ) |>
    iplot()
county_returns |>
    filter(agi_stub == 6) |>
    feols(
        returns ~ i(year, exposure_all, ref = 2017) - 1 | state,
        weights = ~ w1,
        cluster = ~ state
    ) |>
    iplot()
county_returns |>
    filter(agi_stub == 6) |>
    feols(
        returns ~ i(year, exposure_top, ref = 2017) - 1 | state,
        weights = ~ w1,
        cluster = ~ state
    ) |>
    iplot()