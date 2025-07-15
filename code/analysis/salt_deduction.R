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
db_path <- "../../data/corelogic.db"
con <- dbConnect(duckdb::duckdb(), db_path)
dbExecute(con, "set temp_directory = '../../temp/'")

dbGetQuery(con, "select current_setting('threads') as threads,
 current_setting('memory_limit') as memory_limit;")

# Compare changes in tenure across bins of the yearly tax bill
q <- '
with bill_groups_2017 as (
    select 
        clip, 
        case 
            when total_tax_amount < 2500 then 0
            when total_tax_amount < 5000 then 1 
            when total_tax_amount >= 5000 and total_tax_amount < 10000 then 2
            when total_tax_amount >= 10000 and total_tax_amount < 20000 then 3
            when total_tax_amount >= 20000 and total_tax_amount < 30000 then 4
            when total_tax_amount >= 30000 and total_tax_amount < 50000 then 5
            when total_tax_amount >= 50000 then 6
        end as bill_group
    from tax
    where tax_year = 2017 and windsorized = true
),
owner_changes as (
    select 
        t.clip, 
        case when 
            lead(owner_1_full_name) over (partition by t.clip order by t.tax_year) != t.owner_1_full_name 
            then 1 else 0 end as owner_change
    from tax t
    where t.windsorized = true
) 
select 
    bg.bill_group, 
    oc.tax_year,
    round(100 * mean(oc.owner_change), 2) as share_owner_change,
    count(1) as n_properties
from owner_changes oc
join bill_groups_2017 bg on oc.clip = bg.clip
group by bg.bill_group, oc.tax_year
order by bg.bill_group, oc.tax_year;
'
dbGetQuery(con, q)