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
library(tidyverse)
library(purrr)
library(ggplot2)
library(fixest)
library(stringr)
library(rdrobust)
library(binsreg)
library(sf)
library(tigris)

# Read SOI migration data