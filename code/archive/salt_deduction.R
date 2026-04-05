# Clean up
gc()
rm(list = ls())

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
library(viridis)

# Connect to corelogic DB
db_path <- "../../data/corelogic.db"
con <- dbConnect(duckdb::duckdb(), db_path)
dbExecute(con, "set temp_directory = '../../temp/'")

dbGetQuery(con, "select current_setting('threads') as threads,
 current_setting('memory_limit') as memory_limit;")

# How many properties have more than 10000 dollars in yearly taxes? About 7%.
q <- "select total_tax_amount from tax using sample 1 percent"
df <- dbGetQuery(con, q)
df |> 
    mutate(qtile = ntile(total_tax_amount, 50)) |>
    reframe(avg_tax = median(total_tax_amount, na.rm = TRUE), .by = qtile) |>
    arrange(qtile) 

# Percent of properties by state
q <- "select fips_code // 1000 as state, sum(case when total_tax_amount > 10000 then 1 else 0 end) / count(1) as share, count(1) as n_properties from tax group by state order by state;"
df <- dbGetQuery(con, q)
df

# For each tax year, share of properties transacting within a tax bill bin
q <- "
-- Step 1: Table of transacted years
with transacted_years as (
  select clip, year(sale_derived_date) as sale_year
  from ownertransfer
  where
    year(sale_derived_date) between 2014 and 2023 and
    fips_code // 1000 in (02, 12, 32, 33, 46, 47, 48, 53, 56)
  group by clip, year(sale_derived_date)
),
-- Step 2: Add quantile in the tax distribution
tax_with_quantiles as (
  select 
    t.tax_year,
    t.clip,
    ntile(100) over (order by t.total_tax_amount) as ntile
from tax t
where t.fips_code // 1000 in (02, 12, 32, 33, 46, 47, 48, 53, 56)
and t.tax_year between 2014 and 2023
) 
-- Step 3: Aggregate share sold by year and quantile
select
  t.tax_year,
  t.ntile,
  round(100 * avg(case when ty.sale_year is not null then 1 else 0 end), 2) as share_sold,
  count(1) as n_properties
from tax_with_quantiles t
left join transacted_years ty
  on t.clip = ty.clip
  and t.tax_year = ty.sale_year
group by t.tax_year, ntile
order by t.tax_year, ntile;
"
df <- dbGetQuery(con, q) 
df |> 
    mutate(tax_year = as.factor(tax_year)) |>
    ggplot(aes(x = ntile, y = share_sold, color = tax_year, group = tax_year)) +
    geom_line(linewidth = 1) +
    geom_point() +
    labs(
        title = "Share of Owner Change by Tax Bill Percentile Over Years",
        x = "Tax Bill Percentile",
        y = "Share of Owner Change (%)",
        color = "Tax Year"
    ) +
    theme_classic()
ggsave("../../output/share_sold_by.png")

# For each tax year, share of properties transacting within a tax bill bin
# But zeroed in 2017
df |> 
    mutate(tax_year = as.factor(tax_year)) |>
    group_by(ntile) |>
    mutate(share_sold_centered = share_sold - share_sold[tax_year == "2017"]) |>
    ungroup() |>
    ggplot(aes(x = ntile, y = share_sold_centered, color = tax_year, group = tax_year)) +
    geom_line(linewidth = 1) +
    geom_point() +
    labs(
        title = "Share of Owner Change by Tax Bill Percentile Over Years (Centered at 2017)",
        x = "Tax Bill Percentile",
        y = "Share of Owner Change (%) - Centered at 2017",
        color = "Tax Year"
    ) +
    theme_classic()
ggsave("../../output/share_sold_by_tax_bill_percentile_centered.png")

# Flipping the axes:
# For properties less than 5k, 5-12k or over 12k in tax bill
# Compute share of properties transacting by year
q <- "
-- Step 1: Table of transacted years
with transacted_years as (
  select clip, year(sale_derived_date) as sale_year
  from ownertransfer
  where
    year(sale_derived_date) between 2014 and 2022
  group by clip, year(sale_derived_date)
)
-- Step 2: Aggregate share sold by year and tax bin
select
  t.tax_year,  
  case when t.total_tax_amount < 5000 then 'low_tax'
       when t.total_tax_amount >= 5000 and t.total_tax_amount < 12000 then 'med_tax'
       else 'high_tax' end as tax_bin,
  case when t.fips_code // 1000 in (02, 12, 32, 33, 46, 47, 48, 53, 56) then 'low_tax'
       when t.fips_code // 1000 in (15, 36, 50, 23, 09, 06, 34, 17, 27, 41) then 'high_tax'
       end as tax_regime,
  round(100 * avg(case when ty.sale_year is not null then 1 else 0 end), 2) as share_sold,
  count(1) as n_properties
from tax t
left join transacted_years ty
  on t.clip = ty.clip
  and t.tax_year = ty.sale_year
where tax_year between 2014 and 2022 and tax_regime is not null
group by t.tax_year, tax_bin, tax_regime
order by tax_bin, t.tax_year, tax_regime;
"
df <- dbGetQuery(con, q)
df |> 
    mutate(share_sold = share_sold - share_sold[tax_year == 2017], .by = tax_bin)

# Flipping the axes:
# For properties less than 5k, 5-12k or over 12k in tax bill
# And further in high vs low tax states
# Compute share of properties transacting by year

df |> 
    mutate(tax_year = as.factor(tax_year)) |>
    group_by(ntile) |>
    mutate(share_sold_centered = share_sold - share_sold[tax_year == "2017"]) |>
    ungroup() |>
    ggplot(aes(x = ntile, y = share_sold_centered, color = tax_year, group = tax_year)) +
    geom_line(linewidth = 1) +
    geom_point() +
    labs(
        title = "Share of Owner Change by Tax Bill Percentile Over Years (Centered at 2017)",
        x = "Tax Bill Percentile",
        y = "Share of Owner Change (%) - Centered at 2017",
        color = "Tax Year"
    ) +
    theme_classic()
ggsave("../../output/share_sold_by_tax_bill_percentile_centered.png")

# For each tax year, share of properties transacting within a tax bill bin
# But zeroed in 2017 
# and fips x year FEs added
q <- "
-- Step 1: Table of transacted years
with transacted_years as (
  select clip, year(sale_derived_date) as sale_year
  from ownertransfer
  where
    year(sale_derived_date) between 2014 and 2023 and
    fips_code // 1000 in (02, 12, 32, 33, 46, 47, 48, 53, 56)
  group by clip, year(sale_derived_date)
),
-- Step 2: Add quantile in the tax distribution
tax_with_quantiles as (
  select 
    t.tax_year,
    t.fips_code,
    t.clip,
    t.total_tax_amount,
    ntile(5) over (order by t.total_tax_amount) as ntile
from tax t
where 
    t.tax_year between 2014 and 2023 and
    t.total_tax_amount between 5000 and 100000 and
    t.fips_code // 1000 in (02, 12, 32, 33, 46, 47, 48, 53, 56)
),
-- Step 3: Calculate fips by year FEs
FEs as (
  select 
    t.fips_code,
    t.tax_year,
    mean(case when ty.sale_year is not null then 1 else 0 end) as avg
  from tax t
  left join transacted_years ty
    on t.clip = ty.clip
    and t.tax_year = ty.sale_year
  where 
    t.tax_year between 2014 and 2023 and 
    t.total_tax_amount between 5000 and 100000 and
    t.fips_code // 1000 in (02, 12, 32, 33, 46, 47, 48, 53, 56)
  group by t.fips_code, t.tax_year
)
-- Step 4: Aggregate share sold by year and quantile
select
    t.tax_year,
    t.ntile,
    median(t.total_tax_amount) as median_tax,
    round(100 * avg(case when ty.sale_year is not null then 1 else 0 end - f.avg), 2)  as share_sold,
    count(1) as n_properties
from tax_with_quantiles t
left join transacted_years ty
    on t.clip = ty.clip
    and t.tax_year = ty.sale_year
left join FEs f
    on t.fips_code = f.fips_code
    and t.tax_year = f.tax_year
where t.total_tax_amount between 5000 and 100000
group by t.tax_year, ntile
order by t.tax_year, ntile;
"
df <- dbGetQuery(con, q)
df |> 
    ggplot(aes(x = tax_year, y = share_sold, color = as.factor(ntile), group = as.factor(ntile))) +
    geom_line(linewidth = 1) +
    geom_point() +
    labs(
        title = "Share of Owner Change by Tax Bill Percentile Over Years (Fips x Year FEs)",
        x = "Tax Year", 
        y = "Share of owner changes",
        color = "Tax Bill Percentile"
    ) +
    theme_classic()
ggsave("../../output/year_v_share_sold_FE.png")


# For each tax year, share of properties transacting within a tax bill bin
# But zeroed in 2017 
# and fips x year FEs added
# regression based version
q <- "
-- Step 1: Table of transacted years
with transacted_years as (
  select clip, year(sale_derived_date) as sale_year
  from ownertransfer
  where
    year(sale_derived_date) between 2013 and 2024 and 
    fips_code // 1000 in (02, 12, 32, 33, 46, 47, 48, 53, 56)
  group by clip, year(sale_derived_date)
),
-- Step 2: Add quantile in the tax distribution
tax_with_quantiles as (
  select 
    t.tax_year,
    t.fips_code // 1000 as state,
    t.clip,
    t.total_tax_amount,
    ntile(6) over (order by t.total_tax_amount) as ntile
from tax t
where 
    t.tax_year between 2013 and 2024 and
    t.total_tax_amount between 10000 and 100000 and 
    fips_code // 1000 in (02, 12, 32, 33, 46, 47, 48, 53, 56)
using sample 10 percent
),
-- Step 3: Calculate fips by year FEs
FEs as (
    select
        t.fips_code,
        t.tax_year,
        mean(case when ty.sale_year is not null then 1 else 0 end) as avg
    from tax t
    left join transacted_years ty
        on t.clip = ty.clip
        and t.tax_year = ty.sale_year   
    where 
        t.tax_year between 2013 and 2024 and
        t.total_tax_amount between 10000 and 100000 and 
        t.fips_code // 1000 in (02, 12, 32, 33, 46, 47, 48, 53, 56)
    group by t.fips_code, t.tax_year
)
-- Step 4: Merge
select
    t.tax_year,
    t.ntile,
    t.state,
    median(t.total_tax_amount) over (partition by t.ntile) as median_tax,
    case when ty.sale_year is not null then 1 else 0 end as transacted,
    transacted - f.avg as y
from tax_with_quantiles t
left join transacted_years ty
    on t.clip = ty.clip
    and t.tax_year = ty.sale_year
left join FEs f
    on t.fips_code = f.fips_code
    and t.tax_year = f.tax_year
where t.total_tax_amount between 10000 and 100000;
"
df <- dbGetQuery(con, q)

models <- list()
for (i in 1:5) {
    models[[i]] <- df |>
        filter(ntile == i | ntile == (i + 1)) |>
        feols(
            y ~ i(tax_year, ntile, ref = 2017) | tax_year + ntile,
            nthreads = 4,
            cluster = ~ state^tax_year
        )
}

df |>
    filter(ntile <= 2) |>
    feols(
        y ~ i(tax_year, ntile, ref = 2017) | tax_year + ntile,
        nthreads = 4
    ) |> etable()

lapply(models, summary)

collect_estimates <- function(model) {
    coefs <- coeftable(model) |> as.data.frame()
    coefs$year <- c(2013:2016, 2018:2024)
    coefs <- rbind(coefs, c(0, 0, 0, 0, 2017))
    return(coefs)
}

all_estimates <- map_dfr(models, collect_estimates, .id = "model")
all_estimates$group <- rep(2:6, each = 12)

all_estimates |> 
    rename(std_error = `Std. Error`) |>
    mutate(
        t = year - 2017 + 0.1 * (as.numeric(group) - 4),
        lower = Estimate - 1.96 * std_error,
        upper = Estimate + 1.96 * std_error
    ) |>
    ggplot(aes(x = t, y = Estimate, color = as.factor(group), group = as.factor(group))) +
    geom_line() +
    geom_point() +
    geom_errorbar(aes(ymin = lower, ymax = upper), width = 0.2) +
    geom_vline(xintercept = 0, linetype = "dashed", color = "red") +
    geom_hline(yintercept = 0, color = "black") +
    labs(
        title = "Event Study: Regression Based",
        x = "Years Since 2017",
        y = "Estimate",
        color = "Tax Bill Percentile Group"
    ) +
    scale_color_viridis_d() +
    theme_classic() +
    theme(legend.position = "bottom")

ggsave("../../output/event_study.png")



# For each tax year, share of properties transacting within a tax bill bin
# But zeroed in 2017 
# and fips x year FEs added
# and a linear pre-trend
pre_trend_model <- df |> 
    filter(tax_year < 2018) |>
    feols(share_sold ~ i(ntile, as.numeric(tax_year)))
summary(pre_trend_model)
predicted_trend <- predict(pre_trend_model, newdata = df |> filter(tax_year >= 2018))
fitted_trend <- predict(pre_trend_model, newdata = df |> filter(tax_year < 2018))
df$fitted_trend <- c(fitted_trend, predicted_trend)

df |> 
    filter(tax_year < 2021) |>
    mutate(
        tax_year = as.factor(tax_year),
        resid = share_sold - fitted_trend
    ) |>
    ggplot(aes(x = qtiles, y = resid, color = tax_year, group = tax_year)) +
    geom_line(linewidth = 1) +
    geom_point() + 
    scale_color_viridis_d() +
    scale_x_log10() +
    labs(
        title = "Share of Owner Change by Tax Bill Percentile Over Years (Centered at 2017)",
        x = "Tax Bill Percentile (Log Scale)",
        y = "Share of Owner Change (%) - Centered at 2017",
        color = "Tax Year"
    ) +
    theme_classic() +
    xlim(1000, 35000)
ggsave("../../output/share_sold_lin_trend.png")

# Flip the axes and do an event study plot for the top 5 quantiles:
q <- "
-- Step 1: Table of transacted years
with transacted_years as (
  select clip, year(sale_derived_date) as sale_year
  from ownertransfer
  where
    year(sale_derived_date) between 2014 and 2023 and
    fips_code // 1000 in (02, 12, 32, 33, 46, 47, 48, 53, 56)
  group by clip, year(sale_derived_date)
),
-- Step 2: Add quantile in the tax distribution
tax_with_quantiles as (
  select 
    t.tax_year,
    t.fips_code,
    t.clip,
    ntile(100) over (order by t.total_tax_amount) as ntile
from tax t
where t.fips_code // 1000 in (02, 12, 32, 33, 46, 47, 48, 53, 56)
and t.tax_year between 2014 and 2023
),
-- Step 3: Collect quantile values
ntiles as (
    select 
        unnest(generate_series(1, 100)) as ntile,
        unnest(quantile_disc(
            total_tax_amount, 
            list_apply(range(1, 101), x -> x::FLOAT / 100.0)
        )) as qtiles
    from tax
    where 
        fips_code // 1000 in (02, 12, 32, 33, 46, 47, 48, 53, 56)
        and tax_year between 2014 and 2023
    using sample 1 percent
)

# Simpler version: Only Alaska and New Hampshire, 24k as the threshold
q <- "
with transacted_years as (
  select clip, year(sale_derived_date) as sale_year
  from ownertransfer
  where
    year(sale_derived_date) between 2015 and 2023 and
    fips_code // 1000 in (33, 50)
  group by clip, year(sale_derived_date)
)
select
    t.tax_year, 
    sum(case when fips_code // 1000 = 33 then 1 else 0 end) as n_nh,
    sum(case when fips_code // 1000 = 50 then 1 else 0 end) as n_vt
from tax t
join transacted_years ty
    on t.clip = ty.clip
    and t.tax_year = ty.sale_year
where t.total_tax_amount > 24000
group by t.tax_year
order by t.tax_year;
"
dbGetQuery(con, q)

# Compare changes in turnover across bins of the yearly tax bill
q <- '
with transacted_years as (
    select clip, year(sale_derived_date) as sale_year
    from ownertransfer 
    group by clip, year(sale_derived_date)
)
select 
    t.tax_year,
    case 
        when t.total_tax_amount < 100 then 0
        when t.total_tax_amount < 350 and t.total_tax_amount > 100 then 1
        when t.total_tax_amount < 750 and t.total_tax_amount > 350 then 2
        when t.total_tax_amount < 1200 and t.total_tax_amount > 750 then 3
        when t.total_tax_amount < 1750 and t.total_tax_amount > 1200 then 4
        when t.total_tax_amount < 2500 and t.total_tax_amount > 1750 then 5
        when t.total_tax_amount < 3400 and t.total_tax_amount > 2500 then 6
        when t.total_tax_amount < 4800 and t.total_tax_amount > 3400 then 7
        when t.total_tax_amount < 7500 and t.total_tax_amount > 4800 then 8
        else 9
    end as quantile,
    round(100 * mean(case when ty.sale_year is not null then 1 else 0 end), 2) as share_sold,
    count(1) as n_properties
from tax t
left join transacted_years ty on t.clip = ty.clip and t.tax_year = ty.sale_year
where t.tax_year < 2023
group by t.tax_year, quantile
order by t.tax_year, quantile;
'
df <- dbGetQuery(con, q) |>
    mutate(quantile = as.factor(quantile))

ggplot(df, aes(x = tax_year, y = share_sold, color = quantile, group = quantile)) +
    geom_line(size = 1) +
    geom_point() +
    labs(
        title = "Share of Owner Change by Tax Bill Group Over Years",
        x = "Tax Year",
        y = "Share of Owner Change (%)",
        color = "Tax Bill Group"
    ) +
    theme_classic()

ggsave("../../output/share_sold_by_tax_bill_group.png", width = 8, height = 5)

# Re-do this with slightly different bins
q <- "
with sale_years as (
  select clip, year(sale_derived_date) as sale_year
  from ownertransfer 
  where fips_code // 1000 = 1
  group by clip, sale_derived_date
), 
tax_quantiles as (
  select 
    quantile(total_tax_amount, .1) as q10,
    quantile(total_tax_amount, .2) as q20,
    quantile(total_tax_amount, .3) as q30,
    quantile(total_tax_amount, .4) as q40,
    quantile(total_tax_amount, .5) as q50,
    quantile(total_tax_amount, .6) as q60,
    quantile(total_tax_amount, .7) as q70,
    quantile(total_tax_amount, .8) as q80,
    quantile(total_tax_amount, .9) as q90,
    quantile(total_tax_amount, .95) as q95,
    quantile(total_tax_amount, .99) as q99,
    quantile(total_tax_amount, .995) as q995
  from tax
  using sample 1 percent
)
select 
  case when t.total_tax_amount <= q.q10 then 'q0'
       when t.total_tax_amount <= q.q20 then 'q10'
       when t.total_tax_amount <= q.q30 then 'q20'
       when t.total_tax_amount <= q.q40 then 'q30'
       when t.total_tax_amount <= q.q50 then 'q40'
       when t.total_tax_amount <= q.q60 then 'q50'
       when t.total_tax_amount <= q.q70 then 'q60'
       when t.total_tax_amount <= q.q80 then 'q70'
       when t.total_tax_amount <= q.q90 then 'q80'
       when t.total_tax_amount <= q.q95 then 'q90'
       when t.total_tax_amount <= q.q99 then 'q95'
       when t.total_tax_amount <= q.q995 then 'q99'
       else 'q995' end as tax_quantile,
  sum(case when s.sale_year is not null then 1 end) / count(1) as share_sold,
  tax_year as year
from tax t
left join sale_years s on t.clip = s.clip and s.sale_year = t.tax_year
cross join tax_quantiles q
where tax_year between 2013 and 2023 and t.fips_code // 1000 = 1
group by tax_quantile, year
order by tax_quantile, year;
"

df <- dbGetQuery(con, q) |>
    mutate(tax_quantile = as.factor(tax_quantile))
ggplot(df, aes(x = year, y = share_sold, color = tax_quantile, group = tax_quantile)) +
    geom_line(linewidth = 1) +
    geom_point() +
    labs(
        title = "Share of Owner Change by Tax Bill Quantile Over Years",
        x = "Tax Year",
        y = "Share of Owner Change",
        color = "Tax Bill Quantile"
    ) +
    theme_classic() 
ggsave("../../output/share_sold_by_tax_bill_quantile.png", width = 8, height = 5)

# Re-do with very broad bins
q <- "
with sale_years as (
  select clip, year(sale_derived_date) as sale_year
  from ownertransfer 
  group by clip, sale_derived_date
)
select 
  case when t.total_tax_amount <= 5000 then 'low_tax'
       when t.total_tax_amount <= 10000 then 'med_tax'
       else 'high_tax' end as tax_bin,
  sum(case when s.sale_year is not null then 1 end) / count(1) as share_sold,
  tax_year as year
from tax t
left join sale_years s on t.clip = s.clip and s.sale_year = t.tax_year
where tax_year between 2013 and 2023
group by tax_bin, year
order by tax_bin, year;
"

df <- dbGetQuery(con, q) |>
    mutate(tax_bin = as.factor(tax_bin))
ggplot(df, aes(x = year, y = share_sold, color = tax_bin, group = tax_bin)) +
    geom_line(linewidth = 1) +
    geom_point() +
    labs(
        title = "Share of Owner Change by Tax Bill Bin Over Years",
        x = "Tax Year",
        y = "Share of Owner Change",
        color = "Tax Bill Bin"
    ) +
    theme_classic() 
ggsave("../../output/share_sold_by_tax_bill_bin.png", width = 8, height = 5)

# Run a DiD of turnover on tax bill with 2017 as the treatment date
q <- '
with transacted_years as (
    select clip, year(sale_derived_date) as sale_year
    from ownertransfer 
    group by clip, sale_year
)
select 
    tax_year - 2017 as t, 
    case when t.total_tax_amount < 2500 then 1
         when t.total_tax_amount >= 2500 and t.total_tax_amount < 5000 then 2
         when t.total_tax_amount >= 5000 and t.total_tax_amount < 10000 then 3
         when t.total_tax_amount >= 10000 and t.total_tax_amount < 20000 then 4
         else 5 end as buckets,
    case when ty.sale_year is not null then 1 else 0 end as sold
from tax t tablesample 10%
left join transacted_years ty on t.clip = ty.clip and t.tax_year = ty.sale_year
where 
    t.total_tax_amount < 40000 and 
    t.total_tax_amount > 100;
'
df <- dbGetQuery(con, q)

df |> 
    mutate(diff = sold - mean(sold), .by = t) |>
    mutate(second_diff = diff - mean(diff), .by = buckets) |>
    reframe(estimate = mean(second_diff), .by = c(t, buckets)) |>
    mutate(estimate_centered = estimate - estimate[t == -1], .by = buckets) |>
    ggplot(aes(x = t, y = estimate_centered, color = buckets, group = buckets)) +
    geom_line() +
    geom_point() +
    geom_vline(xintercept = - 1, linetype = "dashed", color = "red") +
    labs(
        title = "Event Study: Simple Differences by Year",
        x = "Years Since 2017",
        y = "Estimate"
    ) +
    theme_classic() 
ggsave("../../output/event_study_simple.png", width = 8, height = 5)

# Run a DiD for each state, collect coefficients, group them according to state taxes
q <- "
with transacted_years as (
    select clip, year(sale_derived_date) as sale_year
    from ownertransfer 
    group by clip, year(sale_derived_date)
),
sampled_clips as (
    select distinct clip, random() as rand from tax
)
select 
    tax_year - 2017 as t, 
    fips_code // 1000 as state_fips,
    CASE 
        -- Group 1: No State Income Tax
        WHEN state_fips IN (2, 12, 32, 33, 46, 47, 48, 53, 56) 
            THEN 'No Income Tax'
        
        -- Group 2: Low Income Tax States  
        WHEN state_fips IN (4, 8, 17, 18, 25, 26, 37, 38, 42, 45, 49, 54, 55)
            THEN 'Low Income Tax'
        
        -- Group 3: Moderate Income Tax States
        WHEN state_fips IN (1, 5, 10, 13, 16, 19, 20, 21, 22, 28, 29, 30, 31, 35, 40)
            THEN 'Moderate Income Tax'
        
        -- Group 4: High Income Tax States (including DC)
        WHEN state_fips IN (6, 9, 11, 15, 23, 24, 27, 34, 36, 41, 44, 50, 51)
            THEN 'High Income Tax'
        
        ELSE 'Unknown'
    END AS income_tax_group,
    total_tax_amount as tax_bill, 
    t.clip,
    case when ty.sale_year is not null then 1 else 0 end as sold,
    sold - mean(sold) over (partition by t.clip) as sold_demeaned
from tax t
left join transacted_years ty on t.clip = ty.clip and t.tax_year = ty.sale_year
join sampled_clips sc on t.clip = sc.clip
where 
    rand < 0.1 and
    t.fips_code // 1000 = 34 and
    t.total_tax_amount < 40000 and 
    t.total_tax_amount > 100;
"
df <- dbGetQuery(con, q) |>
    mutate(income_tax_group  = as.factor(income_tax_group))

did <- df |>
    feols(sold ~ i(t, tax_bill, ref = -1) | t + )
summary(did)
