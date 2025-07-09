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
dbExecute(con, "set temp_directory = '../temp/'")

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
q <- 'select sum(case when tax_area_code is not null then 1 else 0 end) / count(1) as pct_tax_area_code from tax where tax_year between 2013 and 2023 and property_indicator_code = 10;'
dbGetQuery(con, q) # abt 90% of residential properties are covered

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
dbGetQuery(con, q) # varies but is pretty good

q <- "select 
        sum(case when block_level_latitude is not null then 1 else 0 end) / count(1) as pct_block_level_latitude,
        sum(case when block_level_longitude is not null then 1 else 0 end) / count(1) as pct_block_level_longitude,
        sum(case when parcel_level_latitude is not null then 1 else 0 end) / count(1) as pct_parcel_level_latitude,
        sum(case when parcel_level_longitude is not null then 1 else 0 end) / count(1) as pct_parcel_level_longitude
    from tax
    where tax_year between 2013 and 2023 and fips_code is not null;"
dbGetQuery(con, q) 

q <- "select 
        sum(case when block_level_latitude is not null then 1 else 0 end) / count(1) as pct_block_level_latitude,
        sum(case when block_level_longitude is not null then 1 else 0 end) / count(1) as pct_block_level_longitude,
        sum(case when parcel_level_latitude is not null then 1 else 0 end) / count(1) as pct_parcel_level_latitude,
        sum(case when parcel_level_longitude is not null then 1 else 0 end) / count(1) as pct_parcel_level_longitude
    from tax
    where tax_year between 2013 and 2023 and fips_code is not null and property_indicator_code = 10;"
dbGetQuery(con, q) #Abt 95% of residential properties

#######----- What about property types? ----#####

q <- "select fips_code // 1000 as state, sum(case when property_indicator_code is not null then 1 else 0 end) / count(1) as pct_property_indicator_code from tax where tax_year between 2013 and 2023 group by state;"
dbGetQuery(con, q) # almost perfect coverage

q <- "select sum(case when property_indicator_code is not null then 1 else 0 end) / count(1) as pct_property_type_code from tax where tax_year between 2013 and 2023"
dbGetQuery(con, q) # abt 99% of properties have property type code



######----- How many sales transactions can we link to tax data? ----#####

# What is the sale date? sale_derived_date
q <- "select 
    sum(case when sale_derived_date is not null then 1 else 0 end) / count(1) as pct_sale_derived_date,
    sum(case when sale_derived_recording_date is not null then 1 else 0 end) / count(1) as pct_sale_derived_recording_date,
    from ownertransfer;"
dbGetQuery(con, q) 

# For which years do we have data?
q <- "select 
    year(sale_derived_date) as year,
    count(1) as n, 
    from ownertransfer
    where sale_derived_date is not null
    group by year
    order by year;"
dbGetQuery(con, q) # Roughly late 90s-2024

# Is the CLIP - composite property key link unique?
q <- "select count(1) from ownertransfer where clip is not null;"
dbGetQuery(con, q) 
q <- "select count(1) from ownertransfer where composite_property_linkage_key is not null;"
dbGetQuery(con, q) 

q <- "select 
    count(1) as total,
    count(distinct clip) as unique_clip,
    count(distinct composite_property_linkage_key) as unique_composite_property_linkage_key,
    from ownertransfer
    where clip is not null and composite_property_linkage_key is not null;"
dbGetQuery(con, q) # Looks onto

# How many sales transactions have a tax record the following year?
q <- "select
    count(1) as n,
    from ownertransfer o
    join tax t 
        on t.clip = o.clip and t.tax_year = year(o.sale_derived_date) + 1
    where 
        o.sale_derived_date is not null 
        and o.clip is not null 
        and year(o.sale_derived_date) between 2012 and 2022 
        and month(o.sale_derived_date) != 12
        and t.clip is not null
        and t.tax_year is not null;"
dbGetQuery(con, q) #124 million

q <- "select count(1) as n from ownertransfer where 
    sale_derived_date is not null 
    and clip is not null 
    and year(sale_derived_date) between 2012 and 2022 
    and month(sale_derived_date) != 12;"
dbGetQuery(con, q) # out of 130 million




######------ How long do we observe parcels for? ----#####

# Overall distribution
q <- "with counts as (
    select count(1) as n from tax where tax_year between 2013 and 2023 and clip is not null group by clip
)
select n, count(1) total from counts group by n order by n;"
dbGetQuery(con, q)

# Distribution by state - looks quite good
q <- "with counts as (
    select any_value(fips_code // 1000) as state, count(1) as n from tax where tax_year between 2013 and 2023 and clip is not null and fips_code is not null group by clip
    )
    select state, n, count(1) total from counts group by state, n order by state, n;
"
state_distribution <- dbGetQuery(con, q)
state_distribution |>
    filter(state <= 56) |> # Only contiguous US
    ggplot(aes(x = n, y = total)) +
    geom_bar(stat = "identity", fill = "steelblue") +
    labs(
        title = "Distribution of Number of Years Observed by State",
        x = "Number of Years Observed",
        y = "Count of Properties"
    ) +
    theme_classic() +
    xlim(0, 11) +
    facet_wrap(~ state, scales = "free_y") +
    expand_limits(y = 0)
ggsave("../output/coverage/state_observation_years.png", width = 12, height = 8)

# Are there any duplicates within clip by tax_year? - Yes
q <- "select
    clip, tax_year, count(1) as n
    from tax
    where tax_year between 2013 and 2023 and clip is not null
    group by clip, tax_year
    having n > 1
    order by clip, tax_year;"
duplicates <- dbGetQuery(con, q)

# Where are these duplicates coming from?
q <- "select
    any_value(fips_code) // 1000 as state,
    count(1) as n       
    from tax
    where tax_year between 2013 and 2023 and clip is not null
    group by clip, tax_year
    having n > 1
    order by state;"
duplicates_by_state <- dbGetQuery(con, q)
duplicates_by_state |> 
    reframe(total = sum(n), .by = state) |>
    arrange(state)

