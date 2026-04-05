# Clean up workspace
gc()
rm(list = ls())
getwd()

# Load libraries
library(tidyverse)
library(tidycensus)
library(sf)
library(duckdb)

# Get state boundaries with geometry (one row per state)
states_sf <- get_acs(
    geography = "state",
    variables = "B01003_001",  # total population
    year = 2020,
    geometry = TRUE,
    show_call = FALSE
)

# Keep one geometry per state and standardize IDs
states_sf <- states_sf |>
    select(GEOID, NAME, geometry) |>
    distinct(GEOID, .keep_all = TRUE) |>
    mutate(state_fips = as.integer(GEOID))

# State centroids for distance calculation
states_sf <- states_sf |>
    st_transform(5070)  # Albers Equal Area for US (meters)
centroids <- st_centroid(st_geometry(states_sf))

# Pairwise distances (meters) between all states
dist_mat <- st_distance(centroids)
dimnames(dist_mat) <- list(states_sf$state_fips, states_sf$state_fips)

# Long format for gravity regressions: (origin_state, destination_state, distance_km)
state_ids <- states_sf$state_fips
dist_long <- expand_grid(origin_state = state_ids, destination_state = state_ids) |>
    filter(origin_state != destination_state) |>
    mutate(
        distance_km = as.numeric(dist_mat[cbind(
            match(origin_state, state_ids),
            match(destination_state, state_ids)
        )]) / 1000
    )

# -----------------------------------------------------------------------------
# Load state migration and state returns; build flow–distance–population table
# -----------------------------------------------------------------------------

con <- dbConnect(duckdb::duckdb(), dbdir = "../../../data/database.db")
migration_state <- dbGetQuery(con, "select * from migration_state")
returns_state <- dbGetQuery(con, "select * from returns_state")
dbDisconnect(con, shutdown = TRUE)

# State returns by state and year (sum across AGI stubs if present)
state_returns_by_year <- returns_state |>
    reframe(returns = sum(returns, na.rm = TRUE), .by = c(state, year)) |>
    rename(state_fips = state)

# Migration flow from origin to destination = returns_inflow (returns into destination from origin)
gravity_df <- migration_state |>
    select(origin_state, destination_state, year, returns_inflow) |>
    rename(flow = returns_inflow) |>
    left_join(dist_long, by = c("origin_state", "destination_state")) |>
    left_join(
        state_returns_by_year |> rename(returns_origin = returns),
        by = c("origin_state" = "state_fips", "year")
    ) |>
    left_join(
        state_returns_by_year |> rename(returns_destination = returns),
        by = c("destination_state" = "state_fips", "year")
    )

# One row per (origin_state, destination_state, year): flow, distance_km, returns_origin, returns_destination
glimpse(gravity_df)
gravity_df |> slice_sample(n = 5)
