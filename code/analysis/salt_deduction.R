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

# Connect to corelogic DB
db_path <- "../../data/corelogic.db"
con <- dbConnect(duckdb::duckdb(), db_path)
dbExecute(con, "set temp_directory = '../../temp/'")

dbGetQuery(con, "select current_setting('threads') as threads,
 current_setting('memory_limit') as memory_limit;")

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

# Run a DiD of turnover on tax bill with 2017 as the treatment date
q <- '
with transacted_years as (
    select clip, year(sale_derived_date) as sale_year
    from ownertransfer 
    group by clip, year(sale_derived_date)
)
select 
    tax_year - 2017 as t, 
    total_tax_amount as tax_bill, 
    case when ty.sale_year is not null then 1 else 0 end as sold
from tax t tablesample 10%
left join transacted_years ty on t.clip = ty.clip and t.tax_year = ty.sale_year
where 
    t.total_tax_amount < 40000 and 
    t.total_tax_amount > 100;
'
df <- dbGetQuery(con, q) |>
    mutate(buckets = case_when(
        tax_bill < 2500 ~ 1,
        tax_bill >= 2500 & tax_bill < 5000 ~ 2,
        tax_bill >= 5000 & tax_bill < 10000 ~ 3,
        tax_bill >= 10000 & tax_bill < 20000 ~ 4,
        tax_bill >= 20000 ~ 5
    ) |> as.factor())

did <- feols(sold ~ i(t, buckets, ref = -1) | tax_bill + t, data = df)
summary(did)

did |> 
    coeftable() |>
    as.data.frame() %>%
    mutate(
        time = as.numeric(str_extract(rownames(.), "(?<=t::)-?\\d+")),
        buckets = as.numeric(str_extract(rownames(.), "(?<=buckets::)[0-9]+"))
    ) |>
    rbind(expand_grid(
        Estimate = 0, 
        `Std. Error` = 0, 
        `t value` = 0, 
        `Pr(>|t|)` = 0,
        time = -1, 
        buckets = unique(df$buckets))
    ) |>
    rename(std_error = `Std. Error`) |> arrange(buckets, time)
    filter(buckets != 5) |>
    ggplot(aes(x = time, y = Estimate, color = buckets, group = buckets)) +
    geom_line() +
    geom_point() +
    geom_errorbar(aes(ymin = Estimate - 1.96 * std_error, ymax = Estimate + 1.96 * std_error), width = 0.2) +
    geom_vline(xintercept = - 1, linetype = "dashed", color = "red") +
    labs(
        title = "Event Study: DiD Coefficients by Year",
        x = "Years Since 2017",
        y = "DiD Estimate"
    ) +
    theme_classic() +
    ylim(-.05, .05)

ggsave("../../output/event_study_did.png", width = 8, height = 5)

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
