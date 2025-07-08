# Clean up
gc()
rm(list = ls())

# Load libraries
library(duckdb)
library(tidycensus)
library(dplyr)
library(purrr)
library(ggplot2)

# Connect to corelogic DB
hard_path <- "/gpfs/gibbs/pi/lapoint/corelogic/database/corelogic.db"
con <- dbConnect(duckdb::duckdb(), hard_path, read_only = TRUE)

######----- General coverage of CL tax data ----#####


## -- US totals -- ##
# Aggregate counts
q <- "select tax_year as year, count(1) as total from tax group by tax_year order by tax_year;"
cl_us_totals <- dbGetQuery(con, q) |>
    mutate(source = "corelogic") # 2013 - 2023

acs1_years <- c(2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2021, 2022, 2023)

# Function to safely get housing data for each year
get_housing_by_year <- function(year) {
  tryCatch({
    get_acs(
      geography = "county",
      variables = "B25001_001",  # Total housing units
      year = year,
      survey = "acs1"
    ) %>%
    mutate(year = year)
  }, error = function(e) {
    message(paste("Year", year, "not available:", e$message))
    return(NULL)
  })
}

# Collect data for all available years
all_housing_data <- map_dfr(acs1_years, get_housing_by_year)

# Collapse to year level and compare to corelogic
us_totals <- all_housing_data |>
    reframe(
        total = sum(estimate, na.rm = TRUE),
        source = "acs1",
        .by = year
    ) |>
    bind_rows(
        cl_us_totals |>
            filter(year %in% acs1_years)
    ) 

# Plot coverage over time
ggplot(us_totals, aes(x = year, y = total, color = source)) +
    geom_line() +
    labs(title = "US Housing Units Over Time",
         x = "Year",
         y = "Total Housing Units") +
    theme_classic()
ggsave("../output/coverage/us_totals.png", width = 8, height = 6)


## -- State level totals -- ##
q <- "select tax_year as year, fips_code // 1000 as state, count(1) as total from tax where tax_year between 2013 and 2023 group by state, year"
cl_state_totals <- dbGetQuery(con, q)

state_totals <- all_housing_data |>
    mutate(GEOID = as.integer(GEOID)) |>
    mutate(state = GEOID %/% 1000) |>
    reframe(
        total = sum(estimate, na.rm = TRUE),
        source = "acs1",
        .by = c(state, year)
    ) |>
    bind_rows(
        cl_state_totals |>
            filter(year %in% acs1_years) |>
            mutate(source = "corelogic")    
    )

# Plot coverage percentage by state over time
coverage_by_state <- state_totals |>
    tidyr::pivot_wider(names_from = source, values_from = total) |>
    mutate(
        coverage_pct = 100 * corelogic / acs1
    )

# Plot coverage percentage by state over time
ggplot(coverage_by_state, aes(x = year, y = coverage_pct, group = state, color = as.factor(state))) +
    geom_line(show.legend = FALSE) +
    labs(
        title = "CoreLogic Coverage as % of Census Housing Units by State",
        x = "Year",
        y = "Coverage (%)"
    ) +
    theme_classic() +
    xlim(2013, 2023)
ggsave("../output/coverage/state_coverage.png", width = 10, height = 6)





######----- Figure out how taxes paid are recorded across the contiguous US ----#####

# Which variables might be relevant?
q <- "describe tax;"
dbGetQuery(con, q)

# Which of these are most consistently populated?
q <- "select 
        fips_code // 1000 as state,
        count(1) as total,
        sum(case when total_tax_amount is not null then 1 else 0 end) / count(1) as total_amount,
        sum(case when net_tax_amount is not null then 1 else 0 end) / count(1) as net_amount,
        sum(case when total_property_tax_rate_percent is not null then 1 else 0 end) / count(1) as total_rate,
        sum(case when total_tax_exemption_amount is not null then 1 else 0 end) / count(1) as total_exemption_amount,
        sum(case when calculated_total_tax_exemption_amount is not null then 1 else 0 end) / count(1) as calculated_exemption_amount
    from tax 
    where tax_year between 2013 and 2023 and fips_code is not null
    group by state;"
dbGetQuery(con, q) # It's only total tax amount that is consistently populated


######----- Figure out how assessment valuations are recorded ----####
q <- "
    select 
        fips_code // 1000 as state,
        count(1) as total,
        sum(case when calculated_total_value is not null then 1 else 0 end) / count(1) as calculated_value,
        sum(case when calculated_land_value is not null then 1 else 0 end) / count(1) as calculated_land_value,
        sum(case when calculated_improvement_value is not null then 1 else 0 end) / count(1) as calculated_improvement_values,
        sum(case when calculated_total_value_source_code is not null then 1 else 0 end) / count(1) as calculated_value_source_code,
        sum(case when assessed_total_value is not null then 1 else 0 end) / count(1) as assessed_value,
        sum(case when assessed_land_value is not null then 1 else 0 end) / count(1) as assessed_land_value,
        sum(case when assessed_improvement_value is not null then 1 else 0 end) / count(1) as assessed_improvement_value,
        sum(case when market_total_value is not null then 1 else 0 end) / count(1) as market_value,
        sum(case when market_land_value is not null then 1 else 0 end) / count(1) as market_land_value,
        sum(case when market_improvement_value is not null then 1 else 0 end) / count(1) as market_improvement_value,
        sum(case when appraised_total_value is not null then 1 else 0 end) / count(1) as appraised_value,
        sum(case when appraised_land_value is not null then 1 else 0 end) / count(1) as appraised_land_value,
        sum(case when appraised_improvement_value is not null then 1 else 0 end) / count(1) as appraised_improvement_value,
        sum(case when assessed_year is not null then 1 else 0 end) / count(1) as assessed_year,
        sum(case when tax_area_code is not null then 1 else 0 end) / count(1) as tax_area_code,
        sum(case when taxable_improvement_value is not null then 1 else 0 end) / count(1) as taxable_improvement_value,
        sum(case when taxable_land_value is not null then 1 else 0 end) / count(1) as taxable_land_value,
        sum(case when taxable_other_value is not null then 1 else 0 end) / count(1) as taxable_other_value,
        sum(case when net_taxable_value is not null then 1 else 0 end) / count(1) as net_taxable_value
        from tax 
        where tax_year between 2013 and 2023 and fips_code is not null
        group by state;"
    dbGetQuery(con, q) 
# Only calculated total value, assessed total value and assessed year consistently populated
# Split into land value and improvement value somewhat populated but not always


######----- Assessment year vs. tax year ----#####
# My thought is assessment is year it was assessed, tax year is year it was paid
# Turns out it is usually just the tax year

# Get assessment year vs tax year distribution by state
q <- "select 
            fips_code // 1000 as state,
            tax_year - assessed_year as year_diff,
            count(1) as count
        from tax 
        where 
            tax_year between 2013 and 2023 
            and fips_code is not null 
            and assessed_year is not null
            and (tax_year - assessed_year) between -2 and 20
        group by state, year_diff
        order by state, year_diff;"

assessment_diff_data <- dbGetQuery(con, q)

# Create the distribution plot
ggplot(
    assessment_diff_data |> filter(state <= 56),
    aes(x = year_diff, weight = count)
) +
    geom_histogram(binwidth = 1, fill = "steelblue", color = "white", alpha = 0.7) +
    labs(
        title = "Distribution of Assessment Year vs Tax Year by State",
        subtitle = "Tax Year - Assessment Year (capped between -2 and 20)",
        x = "Years between Tax Year and Assessment Year",
        y = "Count of Properties"
    ) +
    theme_classic() +
    xlim(-2, 5) +
    facet_wrap(~ state, scales = "free_y")

ggsave("../output/assessment_vs_tax_year_distribution.png", width = 12, height = 8)


#######----- How are jurisdictions recorded? ----#####
# I suspect fips x tax area code

# Jurisdiction county code
q <- 'select fips_code // 1000 as state, sum(case when jurisdiction_county_code is not null then 1 else 0 end) / count(1) as pct_jurisdiction_county_code from tax where tax_year between 2013 and 2023 group by state;'
dbGetQuery(con, q)

# Tax rate area code - sometimes filled but not always
q <- 'select fips_code // 1000 as state, sum(case when tax_rate_area_code is not null then 1 else 0 end) / count(1) as pct_tax_rate_area_code from tax where tax_year between 2013 and 2023 group by state;'
dbGetQuery(con, q)

# Tax area code - mostly gives us the information needed
q <- 'select fips_code // 1000 as state, sum(case when tax_area_code is not null then 1 else 0 end) / count(1) as pct_tax_area_code from tax where tax_year between 2013 and 2023 group by state;'
dbGetQuery(con, q)
q <- 'select sum(case when tax_area_code is not null then 1 else 0 end) / count(1) as pct_tax_area_code from tax where tax_year between 2013 and 2023'
dbGetQuery(con, q) # abt 90% of properties are covered

#######----- Where do we have location data? ----#####

# Check which fields are populated
q <- "select 
        fips_code // 1000 as state,
        sum(case when block_level_latitude is not null then 1 else 0 end) / count(1) as pct_block_level_latitude,
        sum(case when block_level_longitude is not null then 1 else 0 end) / count(1) as pct_block_level_longitude,
        sum(case when parcel_level_latitude is not null then 1 else 0 end) / count(1) as pct_parcel_level_latitude,
        sum(case when parcel_level_longitude is not null then 1 else 0 end) / count(1) as pct_parcel_level_longitude,
    from tax
    where tax_year between 2013 and 2023 and fips_code is not null
    group by state;"
dbGetQuery(con, q)


#######----- What about property types? ----#####

q <- "select fips_code // 1000 as state, sum(case when property_indicator_code is not null then 1 else 0 end) / count(1) as pct_property_indicator_code from tax where tax_year between 2013 and 2023 group by state;"
dbGetQuery(con, q)

