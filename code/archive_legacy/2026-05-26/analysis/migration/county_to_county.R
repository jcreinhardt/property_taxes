# Clean and load
gc()
rm(list = ls())
getwd()

# Load libraries
library(tidyverse)
library(stringr)
library(ggplot2)
library(fixest)
library(duckdb)
library(binsreg)

# Load from database
con <- dbConnect(duckdb::duckdb(), dbdir = "../../../data/database.db")
migration <- dbGetQuery(con, "select * from migration_county_balanced")
migration_state <- dbGetQuery(con, "select * from migration_state")
county_returns <- dbGetQuery(con, "select * from returns_county")
county_returns_agi <- dbGetQuery(con, "select * from returns_county_agi")

# Compare county-to-county coverage to state totals
state_v_county <- migration |> 
    mutate(
        origin_state = origin_fips %/% 1000, 
        destination_state = destination_fips %/% 1000
    ) |>
    reframe(total = sum(returns, na.rm = TRUE), .by = c(origin_state, destination_state, year)) |> 
    right_join(migration_state, by = c("origin_state", "destination_state", "year")) 

state_v_county |> 
    reframe(share = sum(total, na.rm = TRUE) / sum(returns, na.rm = TRUE), .by = c(year)) 

migration |>
    mutate(
        origin_state = origin_fips %/% 1000,
        destination_state = destination_fips %/% 1000
    ) |>
    reframe(total = sum(returns, na.rm = TRUE), .by = c(origin_state, destination_state, year)) |>
    inner_join(migration_state, by = c("origin_state", "destination_state", "year")) |>
    reframe(share = sum(total) / sum(returns), .by = c(origin_state)) |>
    arrange(share)


########################################################
# Construct exposure measure
########################################################

# Step 1: Impute marginal tax rates for county AGI stubs
# Stubs present: 1, 3, 4, 5, 6, 8
# (stub 2 merged into stub 3; stub 7 merged into stub 8)
# - 1: $0
# - 3: $1-$50K  (merged $1-25K and $25-50K)
# - 4: $50-$75K
# - 5: $75-$100K
# - 6: $100-$200K
# - 8: $200K+   (merged $200K-$1M and $1M+)
county_returns_agi <- county_returns_agi |>
    mutate(
        w_mfj    = joint_returns / returns,
        w_single = single_returns / returns,
        w_hoh    = head_of_household_returns / returns,
        mtr_single = case_when(
            agi_stub == 1 ~ 0.00,    # $0
            agi_stub == 3 ~ 0.05,    # $1-50K: blends 0% ($1-25K) and 10%/12% ($25-50K)
            agi_stub == 4 ~ 0.22,    # $50-75K
            agi_stub == 5 ~ 0.225,   # $75-100K: clips 24%
            agi_stub == 6 ~ 0.25,    # $100-200K: blends 24%/32%
            agi_stub == 8 ~ 0.365,   # $200K+: blends 35%/37%
            .default = NA_real_
        ),
        mtr_mfj = case_when(
            agi_stub == 1 ~ 0.00,    # $0
            agi_stub == 3 ~ 0.05,    # $1-50K: mostly below $25.9K std ded then 10%/12%
            agi_stub == 4 ~ 0.12,    # $50-75K: mostly 12%
            agi_stub == 5 ~ 0.22,    # $75-100K: solidly 22%
            agi_stub == 6 ~ 0.24,    # $100-200K
            agi_stub == 8 ~ 0.36,    # $200K+: blends 32%/35%/37%
            .default = NA_real_
        ),
        mtr_hoh = case_when(
            agi_stub == 1 ~ 0.00,    # $0
            agi_stub == 3 ~ 0.06,    # $1-50K: small taxable slice above $18.8K std ded, then 12%
            agi_stub == 4 ~ 0.14,    # $50-75K
            agi_stub == 5 ~ 0.22,    # $75-100K
            agi_stub == 6 ~ 0.25,    # $100-200K
            agi_stub == 8 ~ 0.365,   # $200K+: blends 35%/37%
            .default = NA_real_
        ),
        mtr_weighted = w_mfj * mtr_mfj + w_single * mtr_single + w_hoh * mtr_hoh,
        salt_savings = taxes_paid_amount * mtr_weighted
    )

# Step 2: Compute gross SALT and federal refund per filer by county-year
# salt_pp: gross SALT paid per filer
# salt_refund_pp: federal tax offset from SALT deduction (MTR-weighted)
# effective_salt_pp: net out-of-pocket SALT cost = salt_pp - salt_refund_pp
salt_per_county_year <- county_returns_agi |>
    reframe(
        salt_pp           = sum(taxes_paid_amount, na.rm = TRUE) / sum(returns, na.rm = TRUE),
        salt_refund_pp    = sum(salt_savings,       na.rm = TRUE) / sum(returns, na.rm = TRUE),
        effective_salt_pp = salt_pp - salt_refund_pp,
        .by = c(county, year)
    )

# Step 3: Validate total salt refund size
county_returns_agi |> 
    reframe(total_salt_savings = sum(salt_savings, na.rm = TRUE), .by = year)

# Step 4: Compute SALT differential for each observed pair-year
salt_differentials <- expand_grid(
    origin_county = unique(county_returns_agi$county),
    destination_county = unique(county_returns_agi$county),
    year = 2017:2018
) |> 
    left_join(salt_per_county_year |> rename_with(~ paste0(.x, "_origin"), -c(county, year)),
              by = c("origin_county" = "county", "year")) |>
    left_join(salt_per_county_year |> rename_with(~ paste0(.x, "_dest"), -c(county, year)),
              by = c("destination_county" = "county", "year")) |>
    mutate(salt_differential = effective_salt_pp_origin - effective_salt_pp_dest)   

# Step 5: Compute exposure
salt_exposure <- salt_differentials |>
    reframe(
        exposure = salt_differential[year == 2018] - salt_differential[year == 2017],
        salt_origin = salt_pp_origin[year == 2017],
        salt_destination = salt_pp_dest[year == 2017],
        .by = c(origin_county, destination_county)
    )

# Merge to migration data
migration <- migration |>
    inner_join(
        salt_exposure, 
        by = c("origin_fips" = "origin_county", "destination_fips" = "destination_county")
    )

# Merge destination filer totals (for weights and share denominators)
filers <- county_returns |>
    reframe(total_filers = sum(returns), .by = c(county, year))
migration <- migration |>
    inner_join(filers |> rename(total_filers_dest = total_filers),
              by = c("destination_fips" = "county", "year")) |> 
    inner_join(filers |> rename(total_filers_orig = total_filers),
              by = c("origin_fips" = "county", "year"))


########################################################
# Compute net migration
########################################################

inflows <- migration |> rename(inflow = returns)
outflows <- migration |>
    rename(
        origin_fips = destination_fips, 
        destination_fips = origin_fips,
        outflow = returns
    ) |> 
    select(origin_fips, destination_fips, year, outflow)

net_migration <- inflows |>
    inner_join(outflows, by = c("origin_fips", "destination_fips", "year")) |>
    mutate(returns = inflow - outflow) |> 
    filter(origin_fips > destination_fips)




########################################################
# Who migrates where?
########################################################

## Levels 
net_migration |> 
    arrange(year, salt_origin) |> 
    mutate(cdf_salt_orig = cumsum(total_filers_orig) / sum(total_filers_orig), .by = year) |>
    arrange(year, salt_destination) |> 
    mutate(cdf_salt_dest = cumsum(total_filers_dest) / sum(total_filers_dest), .by = year) |>
    mutate(group = case_when(
        cdf_salt_dest[year == 2017] > .66 & cdf_salt_orig[year == 2017] > .66 ~ "High-High",
        cdf_salt_dest[year == 2017] > .66 & cdf_salt_orig[year == 2017] < .33 ~ "Low-High",
        cdf_salt_dest[year == 2017] < .33 & cdf_salt_orig[year == 2017] > .66 ~ "High-Low",
        cdf_salt_dest[year == 2017] < .33 & cdf_salt_orig[year == 2017] < .33 ~ "Low-Low",
        .default = "Other"
    ), .by = c(origin_fips, destination_fips)) |>   
    reframe(total_migration = sum(returns, na.rm = TRUE), .by = c(group, year)) |>
    ggplot(aes(x = year, y = total_migration, color = group, group = group)) +
    geom_line() +
    geom_point() +
    scale_color_viridis_d(name = "Migration group") +
    theme_classic()

## Shares
net_migration |> 
    arrange(year, salt_origin) |> 
    mutate(cdf_salt_orig = cumsum(total_filers_orig) / sum(total_filers_orig), .by = year) |>
    arrange(year, salt_destination) |> 
    mutate(cdf_salt_dest = cumsum(total_filers_dest) / sum(total_filers_dest), .by = year) |>
    mutate(group = case_when(
        cdf_salt_dest[year == 2017] > .5 & cdf_salt_orig[year == 2017] > .5 ~ "High-High",
        cdf_salt_dest[year == 2017] > .5 & cdf_salt_orig[year == 2017] < .5 ~ "Low-High",
        cdf_salt_dest[year == 2017] < .5 & cdf_salt_orig[year == 2017] > .5 ~ "High-Low",
        cdf_salt_dest[year == 2017] < .5 & cdf_salt_orig[year == 2017] < .5 ~ "Low-Low",
        .default = "Other"
    ), .by = c(origin_fips, destination_fips)) |>   
    reframe(mig_share = sum(returns) / sum(total_filers_orig), .by = c(group, year)) |>
    ggplot(aes(x = year, y = mig_share, color = group, group = group)) +
    geom_line() +
    geom_point() +
    scale_color_viridis_d() +
    theme_classic()



########################################################
# Trends in in-migration for differently exposed counties
########################################################

## Levels
net_migration |>
    arrange(year, exposure) |>
    mutate(
        cdf = cumsum(total_filers_dest) / sum(total_filers_dest),
        .by = year
    ) |>
    mutate(group = case_when(
        cdf[year == 2017] < .5 ~ "Bottom 50%", TRUE ~ "Top 50%"),
           .by = c(origin_fips, destination_fips)) |>
    reframe(total_migration = sum(returns, na.rm = TRUE), .by = c(group, year)) |>
    ggplot(aes(x = year, y = total_migration, color = group, group = group)) +
    geom_line() +
    geom_point() +
    scale_color_viridis_d() +
    theme_classic()

## Shares
net_migration |>
    arrange(year, exposure) |>
    mutate(
        cdf = cumsum(total_filers_dest) / sum(total_filers_dest),
        .by = year
    ) |>
    mutate(group = case_when(cdf[year == 2017] < .5 ~ "Bottom 50%", TRUE ~ "Top 50%"),
           .by = c(origin_fips, destination_fips)) |>
    reframe(total_migration_share = sum(returns, na.rm = TRUE) / sum(total_filers_dest),
            .by = c(group, year)) |>
    ggplot(aes(x = year, y = total_migration_share, color = group, group = group)) +
    geom_line() +
    geom_point() +
    scale_color_viridis_d() +
    theme_classic()

## AGI conditional on moving
net_migration |>
    arrange(year, exposure) |>
    mutate(
        cdf = cumsum(total_filers_dest) / sum(total_filers_dest),
        .by = year
    ) |>
    mutate(group = case_when(cdf[year == 2017] < .5 ~ "Bottom 50%", TRUE ~ "Top 50%"),
           .by = c(origin_fips, destination_fips)) |>
    reframe(avg_agi = sum(agi, na.rm = TRUE) / sum(returns, na.rm = TRUE), .by = c(group, year)) |>
    ggplot(aes(x = year, y = avg_agi, color = group, group = group)) +
    geom_line() +
    geom_point() +
    scale_color_viridis_d() +
    theme_classic()

## E[migration | exposure] over time
binsreg(
    y = returns,
    x = exposure,
    data = net_migration |> filter(year == 2017),
    nbins = 5
)

net_migration |> 
    filter(year == 2017) |>
    ggplot(aes(x = exposure)) +
    geom_histogram() +
    theme_classic()

net_migration |> 
    filter(year == 2017) |>
    ggplot(aes(x = exposure, y = returns)) +
    geom_point() +
    geom_smooth(method = "lm") +
    theme_classic()

net_migration |> 
    feols(returns ~ i(year, exposure)) |> 
    iplot()

migration |> 
    mutate(share = returns / total_filers_orig) |>
    filter(year == 2020) |>
    ggplot(aes(x = exposure, y = share)) +
    geom_point() +
    geom_smooth(method = "lm") +
    theme_classic()

migration |> 
    mutate(returns = returns - mean(returns), .by = c(origin_fips, destination_fips)) |>
    filter(year == 2020) |>
    ggplot(aes(x = exposure, y = returns)) +
    geom_point() +
    geom_smooth(method = "lm") +
    theme_classic()

gravity <- migration |>
    feols(returns ~ total_filers_dest + total_filers_orig)

migration |> 
    mutate(resid = resid(gravity, "pearson")) |>
    filter(year == 2017) |>
    ggplot(aes(x = exposure, y = resid)) +
    geom_point() +
    geom_smooth(method = "lm") +
    theme_classic()


########################################################
# County-level TWFE models
########################################################

migration |> 
    mutate(
        state_orig = origin_fips %/% 1000,
        state_dest = destination_fips %/% 1000
    ) |>
    feols(returns ~ i(year, exposure, ref = 2017) - 1 | origin_fips^destination_fips + state_orig^state_dest^year,
    data = _,
    cluster = ~ origin_fips^destination_fips,
    weights = ~ total_filers_dest
    ) |> iplot()

migration |> 
    arrange(year, exposure) |>
    mutate(
        cdf = cumsum(total_filers_dest) / sum(total_filers_dest),
        .by = year
    ) |>
    mutate(
        treated = case_when(cdf[year == 2017] < .5 ~ FALSE, TRUE ~ TRUE),
        state_orig = origin_fips %/% 1000,
        state_dest = destination_fips %/% 1000,
        .by = c(origin_fips, destination_fips)
    ) |>
    feols(returns ~ i(year, treated, ref = 2016) - 1 | origin_fips^destination_fips + state_orig^state_dest^year,
    data = _,
    cluster = ~ origin_fips^destination_fips,
    weights = ~ total_filers_dest
    ) |> iplot()

migration |> 
    mutate(
        state_orig = origin_fips %/% 1000,
        state_dest = destination_fips %/% 1000,
    ) |>
    feols(log(returns) ~ i(year, exposure, ref = 2017) - 1 | origin_fips^destination_fips + state_orig^state_dest^year,
    data = _,
    cluster = ~ origin_fips^destination_fips,
    weights = ~ total_filers_dest
    ) |> iplot()

migration |> 
    arrange(year, exposure) |>
    mutate(
        cdf = cumsum(total_filers_dest) / sum(total_filers_dest),
        .by = year
    ) |>
    mutate(
        treated = case_when(cdf[year == 2017] < .5 ~ FALSE, TRUE ~ TRUE),
        state_orig = origin_fips %/% 1000,
        state_dest = destination_fips %/% 1000,
        .by = c(origin_fips, destination_fips)
    ) |>
    feols(log(returns) ~ i(year, treated, ref = 2016) - 1 | origin_fips^destination_fips + state_orig^state_dest^year,
    data = _,
    cluster = ~ origin_fips^destination_fips,
    weights = ~ total_filers_dest
    ) |> iplot()

migration |> 
    mutate(
        state_orig = origin_fips %/% 1000,
        state_dest = destination_fips %/% 1000,
        share = returns / total_filers_dest
    ) |>
    feols(share ~ i(year, exposure, ref = 2017) - 1 | origin_fips^destination_fips + state_orig^state_dest^year,
    data = _,
    cluster = ~ origin_fips^destination_fips,
    weights = ~ total_filers_dest
    ) |> iplot()

migration |> 
    arrange(year, exposure) |>
    mutate(
        cdf = cumsum(total_filers_dest) / sum(total_filers_dest),
        .by = year
    ) |>
    mutate(
        treated = case_when(cdf[year == 2017] < .5 ~ FALSE, TRUE ~ TRUE),
        state_orig = origin_fips %/% 1000,
        state_dest = destination_fips %/% 1000,
        share = returns / total_filers_dest,
        .by = c(origin_fips, destination_fips)
    ) |>
    feols(share ~ i(year, treated, ref = 2016) - 1 | origin_fips^destination_fips + state_orig^state_dest^year,
    data = _,
    cluster = ~ origin_fips^destination_fips,
    weights = ~ total_filers_dest
    ) |> iplot()

migration |> 
    mutate(
        state_orig = origin_fips %/% 1000,
        state_dest = destination_fips %/% 1000,
        avg_agi = agi / returns
    ) |>
    feols(avg_agi ~ i(year, salt_differential, ref = 2016) - 1 | origin_fips^destination_fips + state_orig^state_dest^year,
    data = _,
    cluster = ~ origin_fips^destination_fips,
    weights = ~ total_filers_dest
    ) |> iplot()




########################################################
# Gravity models
########################################################

# Load data
migration <- dbGetQuery(con, "select * from migration_county_balanced")

# Check for coverage (answer: roughly 45%)
state_totals <- dbGetQuery(con, "select * from migration_state")
migration |> 
    mutate(
        origin_state = origin_fips %/% 1000, 
        destination_state = destination_fips %/% 1000
    ) |> 
    reframe(total = sum(returns), .by = c(origin_state, destination_state, year)) |> 
    left_join(
        state_totals, 
        by = c("origin_state", "destination_state", "year"),
        suffix = c("_county", "_state")
    ) |> 
    filter(origin_state != destination_state) |>
    filter(origin_state %in% 1:56 & destination_state %in% 1:56) |>
    reframe(
        state_total = sum(returns, na.rm = TRUE),
        county_total = sum(total),
        coverage = county_total / state_total,
        .by = year
    ) |>  
    arrange(year)

# Compute average financial gain from moving for each origin-destination pair
county_returns_agi <- county_returns_agi |>
    mutate(
        w_mfj    = joint_returns / returns,
        w_single = single_returns / returns,
        w_hoh    = head_of_household_returns / returns,
        mtr_single = case_when(
            agi_stub == 1 ~ 0.00,    # $0
            agi_stub == 3 ~ 0.05,    # $1-50K: blends 0% ($1-25K) and 10%/12% ($25-50K)
            agi_stub == 4 ~ 0.22,    # $50-75K
            agi_stub == 5 ~ 0.225,   # $75-100K: clips 24%
            agi_stub == 6 ~ 0.25,    # $100-200K: blends 24%/32%
            agi_stub == 8 ~ 0.365,   # $200K+: blends 35%/37%
            .default = NA_real_
        ),
        mtr_mfj = case_when(
            agi_stub == 1 ~ 0.00,    # $0
            agi_stub == 3 ~ 0.05,    # $1-50K: mostly below $25.9K std ded then 10%/12%
            agi_stub == 4 ~ 0.12,    # $50-75K: mostly 12%
            agi_stub == 5 ~ 0.22,    # $75-100K: solidly 22%
            agi_stub == 6 ~ 0.24,    # $100-200K
            agi_stub == 8 ~ 0.36,    # $200K+: blends 32%/35%/37%
            .default = NA_real_
        ),
        mtr_hoh = case_when(
            agi_stub == 1 ~ 0.00,    # $0
            agi_stub == 3 ~ 0.06,    # $1-50K: small taxable slice above $18.8K std ded, then 12%
            agi_stub == 4 ~ 0.14,    # $50-75K
            agi_stub == 5 ~ 0.22,    # $75-100K
            agi_stub == 6 ~ 0.25,    # $100-200K
            agi_stub == 8 ~ 0.365,   # $200K+: blends 35%/37%
            .default = NA_real_
        ),
        mtr_weighted = w_mfj * mtr_mfj + w_single * mtr_single + w_hoh * mtr_hoh,
        salt_savings = taxes_paid_amount * mtr_weighted
    )

salt_per_county_year <- county_returns_agi |>
    reframe(
        salt_pp           = sum(taxes_paid_amount, na.rm = TRUE) / sum(returns, na.rm = TRUE),
        salt_refund_pp    = sum(salt_savings,       na.rm = TRUE) / sum(returns, na.rm = TRUE),
        effective_salt_pp = salt_pp - salt_refund_pp,
        .by = c(county, year)
    )

county_returns_agi |> 
    reframe(total_salt_savings = sum(salt_savings, na.rm = TRUE), .by = year)

salt_differentials <- expand_grid(
    origin_county = unique(county_returns_agi$county),
    destination_county = unique(county_returns_agi$county),
    year = 2017:2018
) |> 
    left_join(salt_per_county_year |> rename_with(~ paste0(.x, "_origin"), -c(county, year)),
              by = c("origin_county" = "county", "year")) |>
    left_join(salt_per_county_year |> rename_with(~ paste0(.x, "_dest"), -c(county, year)),
              by = c("destination_county" = "county", "year")) |>
    mutate(salt_differential = effective_salt_pp_origin - effective_salt_pp_dest)   

salt_exposure <- salt_differentials |>
    reframe(
        exposure = salt_differential[year == 2018] - salt_differential[year == 2017],
        salt_origin = salt_pp_origin[year == 2017],
        salt_destination = salt_pp_dest[year == 2017],
        .by = c(origin_county, destination_county)
    )

migration <- migration |>
    inner_join(
        salt_exposure, 
        by = c("origin_fips" = "origin_county", "destination_fips" = "destination_county")
    )

# Raw trends in migration by exposure over time
migration <- migration |> 
    mutate(time_period = case_when(
        year < 2015 ~ "2011-2014",
        year >= 2015 & year < 2018 ~ "2015-2017",
        year >= 2018 ~ "2018-2021",
    )) 

binsreg(x = exposure, y = returns, data = migration, by = time_period) 

# After adding origin-destination FEs
migration |> 
    mutate(returns = returns - mean(returns), .by = c(origin_fips, destination_fips)) |> 
    binsreg(x = exposure, y = returns, data = _, randcut = 1, by = time_period)

# Adding population counts
# Load county-level population data from 2011 to 2022 using tidycensus
library(tidycensus)

# Ensure your Census API key is installed with census_api_key("YOUR_API_KEY", install = TRUE)
years <- 2011:2022

pop <- purrr::map_dfr(
  years,
  function(y) {
    tidycensus::get_acs(
      geography = "county",
      variables = "B01003_001",
      year = y,
      survey = "acs5",
      output = "wide"
    ) |>
      dplyr::transmute(
        county_fips = as.integer(GEOID),
        year = y,
        population = B01003_001E
      )
  }
)

# Merge population to migration data (for both origin and destination counties)
migration <- migration |>
    left_join(
        pop |> rename(population_orig = population),
        by = c("origin_fips" = "county_fips", "year")
    ) |>
    left_join(
        pop |> rename(population_dest = population),
        by = c("destination_fips" = "county_fips", "year")
    )

# Download US county shapefile and compute pairwise great-circle distances
# between county centroids (kilometers). `tigris` ships TIGER/Line shapefiles;
# `sf::st_distance` on a geographic CRS returns ellipsoidal distances.
library(tigris)
library(sf)

options(tigris_use_cache = TRUE)

counties_sf <- tigris::counties(cb = TRUE, year = 2020, class = "sf") |>
    sf::st_transform(crs = 4326) |>
    dplyr::transmute(county_fips = as.integer(GEOID), geometry)

county_centroids <- counties_sf |>
    sf::st_centroid()

origin_ids <- unique(migration$origin_fips)
dest_ids   <- unique(migration$destination_fips)

origin_sf <- county_centroids |> dplyr::filter(county_fips %in% origin_ids)
dest_sf   <- county_centroids |> dplyr::filter(county_fips %in% dest_ids)

dist_mat <- sf::st_distance(origin_sf, dest_sf)
dist_mat <- units::set_units(dist_mat, "km") |> units::drop_units()

county_distances <- tibble::tibble(
    origin_fips      = rep(origin_sf$county_fips, times = nrow(dest_sf)),
    destination_fips = rep(dest_sf$county_fips,   each  = nrow(origin_sf)),
    distance_km      = as.vector(dist_mat)
)

migration <- migration |>
    left_join(county_distances, by = c("origin_fips", "destination_fips"))

# Compute gravity panel model
migration |> 
    feols(log(returns) ~ log(population_orig) + log(population_dest) + log(distance_km) + i(year, exposure, ref = 2017),
    cluster = ~ origin_fips^destination_fips,
    ) |> iplot()

# Compute gravity panel model with initial tax differentials
migration |> 
    mutate(salt_differential = salt_origin - salt_destination) |>
    feols(log(returns) ~ log(population_orig) + log(population_dest) + log(distance_km) + i(year, exposure, ref = 2017) + salt_differential,
    cluster = ~ origin_fips^destination_fips,
    ) |> iplot()

# Compute gravity panel model with county-pair FEs
migration |> 
    mutate(salt_differential = salt_origin - salt_destination) |>
    feols(log(returns) ~ log(population_orig) + log(population_dest) + i(year, exposure, ref = 2017) | origin_fips^destination_fips + year,
    cluster = ~ origin_fips^destination_fips,
    ) |> iplot()

# Elasticity version
migration |>
    mutate(
        salt_differential = as.numeric(scale(salt_origin - salt_destination)),
    ) |>
    feols(
        log(returns) ~ log(population_orig) + log(population_dest) + log(distance_km) + 
        i(year, exposure, ref = 2017) + i(year, salt_differential, ref = 2017),
        cluster = ~ origin_fips^destination_fips
    ) |> iplot(i.select = 2)

# Gravity panel: event-study coefficients on SALT differential and exposure
gravity_fit <- migration |>
    mutate(
        salt_differential = salt_origin - salt_destination,
        exposure = exposure / salt_differential
        ) |>
    feols(
        log(returns) ~ log(population_orig) + log(population_dest) + log(distance_km) +
        i(year, salt_differential, ref = 2017) + i(year, exposure, ref = 2017),
        cluster = ~ origin_fips^destination_fips
    )

gravity_coefs <- broom::tidy(gravity_fit, conf.int = TRUE) |>
    dplyr::filter(stringr::str_detect(term, "^year::.*:exposure$")) |>
    dplyr::mutate(
        year = as.integer(stringr::str_extract(term, "(?<=year::)\\d+"))
    )

gravity_ref <- tibble::tibble(
    year = 2017, estimate = 0, conf.low = 0, conf.high = 0
)

gravity_plot_data <- dplyr::bind_rows(gravity_coefs, gravity_ref)

ggplot(gravity_plot_data,
       aes(x = year, y = estimate)) +
    geom_hline(yintercept = 0, linetype = "dotted", color = "grey30") +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey30") +
    geom_errorbar(aes(ymin = conf.low, ymax = conf.high),
                  width = 0.2, color = "#23373b") +
    geom_point(color = "#23373b") +
    labs(x = "", y = "Percent increase in migration") +
    theme(
        panel.background = element_rect(fill = "#fafafa", color = NA),
        plot.background = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        axis.line.x = element_line(color = "grey30"),
        axis.line.y = element_line(color = "grey30"),
        legend.position = "right",
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.box.background = element_rect(fill = "#fafafa", color = "grey30"),
        legend.key = element_rect(fill = "#fafafa", color = NA),
        legend.text = element_text(size = 7),
        axis.text.x = element_text(color = "grey20"),
        axis.text.y = element_text(color = "grey20"),
        text = element_text(color = "grey20"),
    ) +
    scale_x_continuous(breaks = seq(2012, 2022, by = 2))
ggsave("../../../output/fig_gravity_event_study.png", width = 12, height = 8, units = "cm")
