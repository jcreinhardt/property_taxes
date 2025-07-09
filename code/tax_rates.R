# Clean up
gc()
rm(list = ls())

# Load libraries
library(duckdb)
library(tidycensus)
library(data.table)
library(dplyr)
library(purrr)
library(ggplot2)

# Connect to corelogic DB
hard_path <- "/gpfs/gibbs/pi/lapoint/corelogic/database/corelogic.db"
con <- dbConnect(duckdb::duckdb(), hard_path, read_only = TRUE)
dbExecute(con, "set temp_directory = '../temp/'")

# -- Step 1: Nominal tax rates -- #

# Average across time
q <- "select 
    tax_year, 
    avg(total_tax_amount / calculated_total_value) as avg_nominal_tax_rate, 
    median(total_tax_amount / calculated_total_value) as med_nominal_tax_rate
    from tax tablesample 1%
    where 
        tax_year between 2013 and 2023 and fips_code is not null
        and calculated_total_value between 0 and 1e8
        and total_tax_amount between 0 and 1e6
    group by tax_year
    order by tax_year"
dbGetQuery(con, q)

# Changes across states
q <- "select 
    tax_year, 
    fips_code // 1000 as state, 
    100 * median(total_tax_amount / calculated_total_value) as med_nominal_tax_rate
    from tax tablesample 10%
    where 
        tax_year between 2013 and 2023 and fips_code is not null
        and calculated_total_value between 0 and 1e8
        and total_tax_amount between 0 and 1e6
    group by tax_year, state"
dbGetQuery(con, q) |>
    arrange(tax_year, state) |>
    reframe(
        pre_covid_change = med_nominal_tax_rate[tax_year == 2019] - med_nominal_tax_rate[tax_year == 2013],
        post_covid_change = med_nominal_tax_rate[tax_year == 2023] - med_nominal_tax_rate[tax_year == 2019],
        pre_covid_rel_change = (med_nominal_tax_rate[tax_year == 2019] - med_nominal_tax_rate[tax_year == 2013]) / med_nominal_tax_rate[tax_year == 2013],
        post_covid_rel_change = (med_nominal_tax_rate[tax_year == 2023] - med_nominal_tax_rate[tax_year == 2019]) / med_nominal_tax_rate[tax_year == 2019],
        .by = state
    ) |>
    arrange(state)


# -- Step 2: Effective tax rates -- #

# Aggregate effective tax rate by year
q <- "select 
    year(o.sale_derived_date) as year,
    median(t.total_tax_amount / t.calculated_total_value) as med_effective_tax_rate,
    from ownertransfer o
    join tax t
        on t.clip = o.clip and t.tax_year = year(o.sale_derived_date) + 1
    where 
        o.sale_derived_date is not null 
        and o.clip is not null 
        and year(o.sale_derived_date) between 2012 and 2022 
        and month(o.sale_derived_date) != 12
        and t.clip is not null
        and t.tax_year between 2013 and 2023
        and t.calculated_total_value between 0 and 1e8
        and t.total_tax_amount between 0 and 1e6
    group by year
    order by year;"
dbGetQuery(con, q)

# Changes in ETR across time by states
q <- "select 
    year(o.sale_derived_date) as year,
    o.fips_code // 1000 as state,
    median(t.total_tax_amount / t.calculated_total_value) as med_effective_tax_rate
    from ownertransfer o
    join tax t
        on t.clip = o.clip and t.tax_year = year(o.sale_derived_date) + 1
    where 
        o.sale_derived_date is not null 
        and o.clip is not null 
        and year(o.sale_derived_date) between 2012 and 2022 
        and month(o.sale_derived_date) != 12
        and t.clip is not null
        and t.tax_year between 2013 and 2023
        and t.calculated_total_value between 0 and 1e8
        and t.total_tax_amount between 0 and 1e6
    group by year, state
    order by year, state;"
etr_state <- dbGetQuery(con, q) 
df |>
    arrange(year, state) |>
    reframe(
        pre_covid_change = med_effective_tax_rate[year == 2019] - med_effective_tax_rate[year == 2012],
        post_covid_change = med_effective_tax_rate[year == 2022] - med_effective_tax_rate[year == 2019],
        pre_covid_rel_change = (med_effective_tax_rate[year == 2019] - med_effective_tax_rate[year == 2012]) / med_effective_tax_rate[year == 2013],
        post_covid_rel_change = (med_effective_tax_rate[year == 2022] - med_effective_tax_rate[year == 2019]) / med_effective_tax_rate[year == 2019],
        .by = state
    ) |>
    arrange(state) 

# Compare nominal and effective tax rates
q <- "select 
    o.fips_code as fips_code,
    count(1) as n,
    100 * median(t1.total_tax_amount / t1.calculated_total_value) as med_effective_tax_rate,
    100 * median(t0.total_tax_amount / t0.calculated_total_value) as med_nominal_tax_rate
    from ownertransfer o
    join tax t1
        on t1.clip = o.clip and t1.tax_year = year(o.sale_derived_date) + 1
    join tax t0
        on t0.clip = o.clip and t0.tax_year = year(o.sale_derived_date) - 1
    where
        o.sale_derived_date is not null 
        and o.clip is not null 
        and year(o.sale_derived_date) between 2012 and 2022 
        and month(o.sale_derived_date) != 12
        and t0.clip is not null
        and t1.clip is not null
        and t0.tax_year between 2013 and 2023
        and t1.tax_year between 2013 and 2023
        and t0.calculated_total_value between 0 and 1e8
        and t1.calculated_total_value between 0 and 1e8
        and t0.total_tax_amount between 0 and 1e6
        and t1.total_tax_amount between 0 and 1e6
    group by o.fips_code
    order by o.fips_code;"
ntr_plus_etr_county <- dbGetQuery(con, q) 
ntr_plus_etr_county |>
    ggplot(aes(x = med_nominal_tax_rate, y = med_effective_tax_rate, size = n)) +
        geom_point(alpha = 0.6, color = "#0072B2", shape = 21, fill = "#56B4E9", stroke = 0.7) +
        scale_size_continuous(
            name = "Observations per county",
            range = c(2, 12),
            breaks = waiver(),
            guide = "legend"
        ) +
        labs(
            x = "Median Nominal Tax Rate",
            y = "Median Effective Tax Rate",
            title = "Nominal vs. Effective Tax Rates by County"
        ) +
        theme_classic() +
        xlim(0, 10) + 
        ylim(0, 10) + 
        geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "red")
ggsave("../output/ntr_vs_etr.png")





# -- Step 3: Effective capital gains taxes -- #

# Test 
q <- "select 
        clip, 
        tax_year,
        sum(total_tax_amount) over (partition by clip order by tax_year)
        from tax
        where tax_year between 2013 and 2023 and clip is not null and total_tax_amount is not null
        order by clip, tax_year
        limit 15"
dbGetQuery(con, q)

# Add cumulative taxes paid since 2013 to transactions
q <- "with cum_taxes as(
    select 
        clip, 
        tax_year,
        sum(total_tax_amount) over (partition by clip order by tax_year) as cum_taxes
        from tax
        where tax_year between 2013 and 2023 and clip is not null and total_tax_amount is not null
    )
select 
    o.fips_code as fips_code,
    o.sale_derived_date as sale_date,
    o.sale_amount as sale_amount,
    lead(o.sale_derived_date) over (partition by o.clip order by o.sale_derived_date) as next_sale_date,
    lead(o.sale_amount) over (partition by o.clip order by o.sale_derived_date) as next_sale_amount,
    c.cum_taxes as cum_taxes_paid
    from ownertransfer o
    join cum_taxes c
        on c.clip = o.clip and c.tax_year = year(o.sale_derived_date)
    where
        o.sale_derived_date is not null 
        and o.clip is not null 
        and year(o.sale_derived_date) between 2013 and 2023
        and o.sale_amount is not null"


transactions |>

# Query transactions
q <- "select 
        clip, 
        sale_derived_date as first_date,
        sale_amount as first_sale_amount, 
        lead(sale_derived_date) over(partition by clip order by sale_derived_date) as next_sale_date, 
        lead(sale_amount) over(partition by clip order by sale_derived_date) as next_sale_amount
        from ownertransfer
        where sale_derived_date is not null and sale_amount is not null and clip is not null and year(sale_derived_date) between 2013 and 2023" 
transactions <- dbGetQuery(con, q) |>
    filter(!is.na(next_sale_date) & !is.na(next_sale_amount))

# Function to query cumulative taxes paid
get_cum_taxes <- function(clip_id, start_date, end_date) {
    start_year <- as.integer(format(start_date, "%Y"))
    end_year <- as.integer(format(end_date, "%Y"))
    q <- sprintf("select sum(total_tax_amount) as cum_taxes
                  from tax
                  where clip = '%s' 
                  and tax_year > %d 
                  and tax_year <= %d", 
                 clip_id, start_year, end_year)
    dbGetQuery(con, q)$cum_taxes
}

# Query tax records
q <- "select clip, tax_year, total_tax_amount from tax tablesample 1% where clip is not null and total_tax_amount is not null and tax_year between 2013 and 2023"
taxes_paid <- dbGetQuery(con, q) |>
    as.data.table()
    
# Order transactions by clip and sale_derived_date
setorder(transactions, clip, sale_derived_date)

# For each clip, get previous and next sale date
transactions[, next_sale_date := shift(sale_derived_date, type = "lead"), by = clip]
transactions[, next_sale_amount := shift(sale_amount, type = "lead"), by = clip]

# Remove last transaction per clip (no next sale)
spells <- transactions[!is.na(next_sale_date)]

# Merge in all taxes paid for each spell
# First, add a year column to taxes_paid for easier filtering
taxes_paid[, year := tax_year]

# For each spell, sum taxes paid between sale_derived_date (exclusive) and next_sale_date (inclusive)
get_cumulative_taxes <- function(clip_id, start_date, end_date) {
    # Tax years to include: those after year(start_date) up to and including year(end_date)
    start_year <- as.integer(format(start_date, "%Y"))
    end_year <- as.integer(format(end_date, "%Y"))
    taxes_paid[clip == clip_id & year > start_year & year <= end_year, sum(total_tax_amount, na.rm = TRUE)]
}

spells[, cumulative_taxes_paid := mapply(
    get_cumulative_taxes,
    clip, sale_derived_date, next_sale_date
)]

# spells now contains, for each housing spell, the cumulative taxes paid between two transactions


