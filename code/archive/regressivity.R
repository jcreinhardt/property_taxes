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


