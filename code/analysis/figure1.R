# Clean up
gc()
rm(list = ls())
getwd()

# Load libraries
library(duckdb)
library(tidycensus)
library(data.table)
library(dplyr)
library(tidyr)
library(purrr)
library(ggplot2)
library(fixest)
library(stringr)
library(rdrobust)
library(binsreg)

# Connect to corelogic DB
db_path <- "/gpfs/gibbs/pi/lapoint/corelogic/database/corelogic.db"
con <- dbConnect(duckdb::duckdb(), db_path, read_only = TRUE)
dbExecute(con, "set temp_directory = '../../temp/'")

dbGetQuery(con, "select current_setting('threads') as threads,current_setting('memory_limit') as memory_limit;")

# Query panel of properties with 
# - taxes due
# - transactions
# - years 2014 through 20211
q <- "
with transacted_properties as (
    select clip, year(sale_derived_date) as year
    from ownertransfer
    where year between 2014 and 2021 and fips_code // 1000 = 33
    group by clip, year
)
select 
    t.clip,
    t.tax_year,
    t.fips_code,
    t.total_tax_amount,
    case when tp.year is not null then 1 else 0 end as transacted
from tax t
left join transacted_properties tp
    on t.clip = tp.clip and t.tax_year = tp.year
where 
    t.tax_year between 2014 and 2021 and
    t.property_indicator_code = 10 and
    t.total_tax_amount > 0 and
    t.fips_code // 1000 = 33
"
df <- dbGetQuery(con, q)

############################################################################################################################
# - Method 1: RDD over time - ##############################################################################################
############################################################################################################################

# For an RDD object, extract coefficients and confidence intervals
extract_coefs <- function(results) {
    c(
        coef = results$Estimate[1], 
        lower = results$ci[1,1],
        upper = results$ci[1,2]
    )
}

# Estimate RDD for each year
estimate_rd <- function(data, year) {
    rdrobust(
        y = 100 * data$transacted[data$tax_year == year],
        x = data$total_tax_amount[data$tax_year == year],
        c = 10000
    ) |> extract_coefs()
}

# Loop over years and store results
years <- 2014:2024
estimates <- map_df(years, function(year) {
    c(year = year, estimate_rd(df, year))
})

# Check
estimates

# Plot
ggplot(estimates, aes(x = year, y = coef)) +
    geom_point() +
    geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.2) +
    geom_hline(yintercept = 0, linetype = "solid", color = "black") +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "red") +
    labs(
        title = "RDD estimate at deductibility cutoff ($10,000)",
        x = "Year",
        y = "Change in transaction probability (%)"
    ) +
    theme_classic() +
    scale_x_continuous(breaks = seq(2014, 2024, by = 2)) +
    annotate("text", x = 2015, y = max(estimates$upper) * 0.9, 
             label = "Baseline outcome: 2%", 
             hjust = 0, vjust = 1, 
             size = 3, 
             box.colour = "black", 
             box.padding = unit(0.3, "lines"))
ggsave("../../output/RDD.png")



############################################################################################################################
# - Method 2: DiD or two-dimensional binscatter - ##############################################################################################
############################################################################################################################

df |> 
    mutate(
        transacted = 100 * transacted,
        period = ifelse(tax_year <= 2017, "Pre-2018", "Post-2018")
    ) |>
    filter(total_tax_amount > 1500 & total_tax_amount < 20000) |>
    filter(tax_year %in% 2014:2021) |>
    mutate(centered_transactions = transacted - mean(transacted), .by = period) |>
    as.data.frame() |>
    binsreg(
        y = centered_transactions, 
        x = total_tax_amount,
        plotxrange = c(1500, 15000),
        by = period,
        samebinsby = TRUE,
        ci = TRUE,
        data = _
    )
ggsave("../../output/binscatter.png")

recentered_df <- df |> 
    mutate(
        transacted = 100 * (transacted - mean(transacted)),
        .by = c(tax_year, fips_code)
    )

averages <- df |> 
    filter(total_tax_amount > 5000 & total_tax_amount < 20000) |>
    filter(tax_year %in% 2014:2021) |>
    mutate(group = ntile(total_tax_amount, 12)
    ) |>
    reframe(group_avg = mean(total_tax_amount), .by = group) |>
    arrange(group)

coefs <- recentered_df |> 
    filter(total_tax_amount > 5000 & total_tax_amount < 20000) |>
    filter(tax_year %in% 2014:2021) |>
    mutate(
        period = ifelse(tax_year <= 2017, "Pre-2018", "Post-2018"),
        group = ntile(total_tax_amount, 12)
    ) |>
    feols(
        transacted ~ i(group, period) - 1
    ) |> 
    coeftable() |>
    as.data.frame() |>
    mutate(
        lower = Estimate - 1.96 * `Std. Error`,
        upper = Estimate + 1.96 * `Std. Error`
    ) |>
    tibble::rownames_to_column("coef_name") |>
    mutate(
        group = as.numeric(str_extract(coef_name, "(?<=group::)\\d+")),
        period = str_extract(coef_name, "(?<=period::)[^:]+"),
    ) |>
    left_join(
        averages,
        by = "group"
    ) |>
    mutate(group_avg = group_avg + 100 * ifelse(period == "Pre-2018", -1, 1))

ggplot(coefs, aes(x = group_avg, y = Estimate, color = period)) +
    geom_point() +
    geom_errorbar(aes(ymin = lower, ymax = upper), width = 200) +
    geom_vline(xintercept = 10000, linetype = "dashed", color = "red") +
    labs(
        x = "Property tax due ($)",
        y = "Transaction probability (recentered, %)",
        color = "Period"
    ) +
    theme_classic() +
    theme(
        legend.position = "bottom",
        legend.title = NULL,
        panel.grid.major.y = element_line(color = "grey80", linewidth = 0.3)
    ) +
    scale_color_manual(values = c("Pre-2018" = "#FFC20A", "Post-2018" = "#0C7BDC"), labels = c("Pre-2018", "Post-2018")) 
ggsave("../../output/binscatter.png", width = 18, height = 12, units = "cm") 
    





############################################################################################################################
# - Method 3: IV for property prices - ##############################################################################################
############################################################################################################################

# Share of variation in taxes due to rates vs. assessments
q <- "
with tax_rates as (
    select 
        tax_year,
        tax_rate_area_code,
        median(total_tax_amount / calculated_total_value) as tax_rate
    from tax
    where 
        tax_year between 2014 and 2021 and
        property_indicator_code = 10 and
        total_tax_amount > 0
    group by tax_year, tax_rate_area_code
    having count(1) >= 100
)
select 
    t.clip, 
    t.tax_year,
    t.total_tax_amount as current_tax,
    tra.tax_rate as current_rate,
    t.calculated_total_value as current_value,
    lag(t.total_tax_amount) over (partition by t.clip order by t.tax_year) as lagged_tax,
    lag(tra.tax_rate) over (partition by t.clip order by t.tax_year) as lagged_rate,
    lag(t.calculated_total_value) over (partition by t.clip order by t.tax_year) as lagged_value
from tax t
join tax_rates tra
    on t.tax_year = tra.tax_year and t.tax_rate_area_code = tra.tax_rate_area_code
where 
    t.tax_year between 2014 and 2021 and
    t.property_indicator_code = 10 and
    t.total_tax_amount > 0
using sample 1 percent
"

df <- dbGetQuery(con, q)






























############################################################################################################################
# - Method 4: Spatial RDD at state borders - ##############################################################################################
############################################################################################################################


############################################################################################################################
# - Method 5: Variation in re-assessment timing - ##############################################################################################
############################################################################################################################




############################################################################################################################
# - Method 6: Correlation between changes in taxes and changes in transactions - ##############################################################################################
############################################################################################################################


# # Query total county-level housing stock
# q <- "
# select 
#     fips_code,
#     count(1) as n,
#     sum(calculated_total_value) as stock,
#     median(total_tax_amount / calculated_total_value) as tax_rate
# from tax
# where 
#     tax_year = 2022 and
#     property_indicator_code = 10 
# group by fips_code
# "
# df <- dbGetQuery(con, q) 

# # Query census data on county populations
# pop <- get_estimates(
#   geography = "county",
#   product = "population",
#   vintage = 2022,
#   geometry = FALSE  
# ) |>
#     mutate(GEOID = as.numeric(GEOID)) |>
#     filter(variable == "POPESTIMATE") |>
#     as.data.frame()

# # Merge the two datasets
# setdiff(df$fips_code, pop$GEOID)  
# setdiff(pop$GEOID, df$fips_code)

# df <- df %>%
#   inner_join(
#     pop,
#     by = c("fips_code" = "GEOID")
#   ) %>% 
#   mutate(
#     stock_pc = stock / n,
#     housing_pc = n / value
#   )

# # Calculate correlations
# df |>
#     filter(tax_rate <= .1) |>
#     filter(stock_pc < quantile(df$stock_pc, .99)) |>
#     feols(
#         stock_pc ~ tax_rate, 
#         weights = ~ value
#     )

# df |> 
#     filter(tax_rate <= .1) |>
#     filter(stock_pc < quantile(df$stock_pc, .99)) |>
#     feols(
#         housing_pc ~ tax_rate, 
#         weights = ~ value
#     )

# df |> 
#     filter(tax_rate <= .1) |>
#     mutate(group = ntile(tax_rate, 10)) |>
#     filter(stock_pc < quantile(df$stock_pc, .99)) |>
#     reframe(
#         rate = mean(tax_rate),
#         stock = mean(stock_pc),
#         housing = mean(housing_pc),
#         .by = group
#     ) |>
#     arrange(group) 

# # Compare changes in tax rate between 2013 and 2017
# # to changes in the stock and housing rate
# q <- "
# select 
#     fips_code,
#     tax_year,
#     count(1) as n,
#     sum(calculated_total_value) as stock,
#     100 * median(total_tax_amount / calculated_total_value) as tax_rate
# from tax
# where 
#     tax_year in (2013, 2017, 2018, 2022) and
#     property_indicator_code = 10 
# group by fips_code, tax_year
# "
# df <- dbGetQuery(con, q) 

# # Merge population data
# pop <- get_estimates(
#   geography = "county",
#   product = "population",
#   vintage = c(2013, 2017, 2018, 2022),
#   geometry = FALSE  
# ) |>
#     mutate(
#         GEOID = as.numeric(GEOID),
#         year = 2017
#     ) |>
#     filter(variable == "POPESTIMATE") |>
#     as.data.frame()

# df <- df %>%
#   inner_join(
#     pop,
#     by = c("fips_code" = "GEOID")
#   ) %>% 
#   mutate(
#     stock_pc = stock / n,
#     housing_pc = n / value
#   )

# # Calculate differences
# df <- df |>
#     pivot_wider(
#         names_from = tax_year,
#         values_from = c(n, stock, tax_rate),
#         names_sep = "_"
#     ) |>
#     mutate(
#         delta_stock_pre = stock_2017 - stock_2013,
#         delta_stock_post = stock_2022 - stock_2018,
#         delta_housing_pre = n_2017 - n_2013,
#         delta_housing_post = n_2022 - n_2018,
#         delta_tax_rate_pre = tax_rate_2017 - tax_rate_2013,
#         delta_tax_rate_post = tax_rate_2022 - tax_rate_2018,
#         n_pre = (n_2017 + n_2013) / 2,
#         n_post = (n_2022 + n_2018) / 2
#     ) |> 
#     select(
#         fips_code, 
#         delta_stock_pre, delta_stock_post, 
#         delta_housing_pre, delta_housing_post, 
#         delta_tax_rate_pre, delta_tax_rate_post,
#         n_pre, n_post
#     ) |>
#     filter(!is.na(delta_tax_rate_post) & !is.na(delta_stock_post) & !is.na(delta_housing_post)) |>
#     filter(!is.na(delta_tax_rate_pre) & !is.na(delta_stock_pre) & !is.na(delta_housing_pre))

# # Break out by tax change bin
# df |> 
#     mutate(group = ntile(delta_tax_rate_pre, 10)) |>
#     reframe(
#         delta_rate_pre = mean(delta_tax_rate_pre),
#         delta_stock_pre = mean(delta_stock_pre),
#         delta_housing_pre = mean(delta_housing_pre),
#         .by = group
#     ) |>
#     arrange(group)

# df |>
#     mutate(group = ntile(delta_tax_rate_post, 10)) |>
#     reframe(
#         delta_rate_post = mean(delta_tax_rate_post),
#         delta_stock_post = mean(delta_stock_post),
#         delta_housing_post = mean(delta_housing_post),
#         .by = group
#     ) |>
#     arrange(group)

# feols(delta_stock_pre ~ delta_tax_rate_pre, data = df, weights = ~ n_pre)
# feols(delta_stock_post ~ delta_tax_rate_post, data = df, weights = ~ n_post)
# feols(delta_housing_pre ~ delta_tax_rate_pre, data = df, weights = ~ n_pre)
# feols(delta_housing_post ~ delta_tax_rate_post, data = df, weights = ~ n_post)

