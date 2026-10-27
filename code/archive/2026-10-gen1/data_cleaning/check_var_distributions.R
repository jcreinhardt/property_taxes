# Set up
gc()
rm(list = ls())

# Load libraries
library(duckdb)
library(tidycensus)
library(data.table)
library(dplyr)
library(purrr)
library(ggplot2)

# Connect to DB
db_path <- "../../data/corelogic.db"
con <- dbConnect(duckdb::duckdb(), db_path)
dbExecute(con, "set temp_directory = '../../temp/'")

#####################
##### -- TAX -- #####
#####################
# FIPS codes
q <- 'select fips_code, count(1) as n from tax group by fips_code order by fips_code;'
dbGetQuery(con, q)

q <- 'select fips_code, count(1) as n from ownertransfer group by fips_code order by fips_code;'
dbGetQuery(con, q)

# Total tax amount
q <- 'select fips_code // 1000 as state, mean(total_tax_amount) as avg_tax from tax group by state order by state;'
dbGetQuery(con, q)

q <- 'select tax_year, mean(total_tax_amount) as avg_tax from tax group by tax_year order by tax_year;'
dbGetQuery(con, q)

q <- 'select fips_code // 1000 as state, fips_code, tax_year, median(total_tax_amount) as med from tax group by fips_code, tax_year order by fips_code, tax_year;'
df <- dbGetQuery(con, q)
ggplot(df, aes(x = med)) + 
    geom_histogram() + 
    facet_wrap(~ state) +
    xlim(0, 10000) 

q <- 'select fips_code // 1000 as state, fips_code, tax_year, total_tax_amount from tax order by total_tax_amount desc limit 50;'
dbGetQuery(con, q)

# Calculated total value
q <- 'select tax_year, mean(calculated_total_value) as avg_value from tax group by tax_year order by tax_year;'
dbGetQuery(con, q)

q <- 'select fips_code // 1000 as state, mean(calculated_total_value) as avg_value from tax group by state order by state;'
dbGetQuery(con, q)

q <- 'select fips_code // 1000 as state, fips_code, tax_year, median(calculated_total_value) as med from tax group by fips_code, tax_year order by fips_code, tax_year;'
df <- dbGetQuery(con, q)
ggplot(df, aes(x = med)) + 
    geom_histogram() + 
    facet_wrap(~ state) +
    xlim(0, 10000)


