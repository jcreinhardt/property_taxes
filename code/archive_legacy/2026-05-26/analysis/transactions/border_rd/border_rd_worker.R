#!/usr/bin/env Rscript
# Worker script for dSQ job array
# Each invocation processes one border-year combination
# Usage: Rscript border_rd_worker.R <task_id>

args <- commandArgs(trailingOnly = TRUE)
task_id <- as.integer(args[1])

# Load libraries
library(duckdb)
library(sf)
library(tigris)
library(dplyr)
library(purrr)
library(rdrobust)

cat("Task", task_id, "starting at", format(Sys.time()), "\n")

# Setup paths
db_path <- "/gpfs/gibbs/pi/lapoint/corelogic/database/corelogic.db"
output_dir <- "/vast/palmer/home.grace/jr2728/projects/property_taxes/output/border_rd"
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

# Check if this task was already completed
output_file <- file.path(output_dir, paste0("result_", task_id, ".rds"))
if (file.exists(output_file)) {
    cat("Task", task_id, "already completed, skipping\n")
    quit(save = "no", status = 0)
}

# Load states and create borders (same as main script)
options(tigris_use_cache = TRUE)
us_states <- states(year = 2017, cb = TRUE) |>
    filter(!STUSPS %in% c("AK", "HI", "PR", "VI", "GU", "AS", "MP"))

adjacency <- st_touches(us_states)
adjacency <- map2(seq_along(adjacency), adjacency, ~ .y[.y > .x])

extract_border <- function(index_1, index2) {
    border_geom <- st_intersection(
        st_boundary(st_geometry(us_states[index_1, ])),
        st_boundary(st_geometry(us_states[index2, ]))
    )
    st_sf(
        state1 = us_states$STUSPS[index_1],
        state2 = us_states$STUSPS[index2],
        fips1 = us_states$STATEFP[index_1],
        fips2 = us_states$STATEFP[index2],
        geometry = border_geom
    ) |> st_set_crs(4326)
}

borders <- map2(seq_along(adjacency), adjacency,
    ~ map(.y, function(idx2) extract_border(.x, idx2))) |> flatten()

# Create task grid: all combinations of borders and years
years <- 2014:2023
tasks_df <- expand.grid(border_idx = seq_along(borders), year = years)

# Validate task_id
if (task_id < 1 || task_id > nrow(tasks_df)) {
    cat("Invalid task_id:", task_id, "- must be between 1 and", nrow(tasks_df), "\n")
    quit(save = "no", status = 1)
}

# Get this task's assignment
border_idx <- tasks_df$border_idx[task_id]
year <- tasks_df$year[task_id]
border <- borders[[border_idx]]

cat("Processing border", border$state1, "-", border$state2, "for year", year, "\n")

# Connect to DB
con <- dbConnect(duckdb::duckdb(), db_path, read_only = TRUE)

# Define border_rd function
border_rd <- function(border, year, con) {
    fips1 <- border$fips1
    fips2 <- border$fips2
    
    q <- paste0(
        " with transacted_properties as (
            select clip, year(sale_derived_date) as year
            from ownertransfer
            where year = ", year, " and fips_code // 1000 in (",
            fips1, ", ", fips2, ")
            group by clip, year
        )
        select 
            t.clip,
            t.parcel_level_latitude,
            t.parcel_level_longitude,
            t.tax_year,
            t.fips_code,
            t.total_tax_amount,
            case when tp.year is not null then 1 else 0 end as transacted
        from tax t
        left join transacted_properties tp
            on t.clip = tp.clip and t.tax_year = tp.year
        where 
            t.tax_year = ", year, " and
            t.property_indicator_code = 10 and
            t.total_tax_amount > 0 and
            t.fips_code // 1000 in (", fips1, ", ", fips2, ")
        "
    )
    df <- dbGetQuery(con, q)
    
    df_sf <- df |>
        filter(!is.na(parcel_level_longitude) & !is.na(parcel_level_latitude)) |>
        st_as_sf(coords = c("parcel_level_longitude", "parcel_level_latitude"),
                 crs = 4326, remove = TRUE)
    
    n_state1 <- nrow(df_sf |> filter(fips_code %/% 1000 == as.numeric(fips1)))
    n_state2 <- nrow(df_sf |> filter(fips_code %/% 1000 == as.numeric(fips2)))
    
    if(n_state1 < 1000 | n_state2 < 1000) {
        return(list(
            coef = NA, coef_bc = NA, lower = NA, upper = NA, 
            lower_bc = NA, upper_bc = NA, bw = NA,
            n_state1 = n_state1, n_state2 = n_state2
        ))
    }
    
    df_sf <- df_sf |> 
        mutate(dist = st_distance(geometry, border)) |>
        mutate(dist = as.numeric(dist)) |>
        mutate(dist = ifelse(fips_code %/% 1000 == as.numeric(fips1), -dist, dist))
    
    result <- rdrobust(y = 100 * df_sf$transacted, x = df_sf$dist, c = 0)
    
    list(
        coef = result$coef[1], 
        coef_bc = result$coef[2],
        lower = result$ci[1,1], 
        upper = result$ci[1,2],
        lower_bc = result$ci[2,1], 
        upper_bc = result$ci[2,2],
        bw = (result$bws[1] + result$bws[2]) / 2,
        n_state1 = n_state1,
        n_state2 = n_state2
    )
}

# Run with error handling
result <- tryCatch({
    border_rd(border, year, con)
}, error = function(e) {
    cat("Error:", e$message, "\n")
    list(
        coef = NA, coef_bc = NA, lower = NA, upper = NA, 
        lower_bc = NA, upper_bc = NA, bw = NA,
        n_state1 = NA, n_state2 = NA,
        error = e$message
    )
})

dbDisconnect(con, shutdown = TRUE)

# Save result
out_df <- data.frame(
    task_id = task_id,
    border_idx = border_idx,
    year = year,
    state1 = border$state1,
    state2 = border$state2,
    fips1 = border$fips1,
    fips2 = border$fips2,
    coef = result$coef,
    coef_bc = result$coef_bc,
    lower = result$lower,
    upper = result$upper,
    lower_bc = result$lower_bc,
    upper_bc = result$upper_bc,
    bw = result$bw,
    n_state1 = result$n_state1,
    n_state2 = result$n_state2
)

saveRDS(out_df, output_file)
cat("Task", task_id, "complete at", format(Sys.time()), "\n")
