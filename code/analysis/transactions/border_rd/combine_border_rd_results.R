#!/usr/bin/env Rscript
# Combine all dSQ job array results into a single file
# Run this after all jobs have completed

library(dplyr)
library(duckdb)

output_dir <- "/vast/palmer/home.grace/jr2728/projects/property_taxes/output/border_rd"
files <- list.files(output_dir, pattern = "result_.*\\.rds", full.names = TRUE)

cat("Found", length(files), "result files\n")

if (length(files) == 0) {
    stop("No result files found in ", output_dir)
}

# Load and combine all results
results <- lapply(files, function(f) {
    tryCatch(readRDS(f), error = function(e) NULL)
}) |> 
    Filter(Negate(is.null), x = _) |>
    bind_rows()

cat("Combined", nrow(results), "results\n")

# Check for missing tasks
expected_tasks <- 1:max(results$task_id, na.rm = TRUE)
missing_tasks <- setdiff(expected_tasks, results$task_id)
if (length(missing_tasks) > 0) {
    cat("Warning: Missing tasks:", paste(missing_tasks, collapse = ", "), "\n")
}

# Summary statistics
cat("\nSummary:\n")
cat("  Successful estimates:", sum(!is.na(results$coef)), "\n
")
cat("  Failed/skipped:", sum(is.na(results$coef)), "\n")
cat("  Years covered:", paste(sort(unique(results$year)), collapse = ", "), "\n")
cat("  Unique borders:", length(unique(results$border_idx)), "\n")

# Save as RDS
rds_file <- "/vast/palmer/home.grace/jr2728/projects/property_taxes/output/border_rd_results.rds"
saveRDS(results, rds_file)
cat("\nResults saved to:", rds_file, "\n")

# Also save to DuckDB for easy querying
db_file <- "/vast/palmer/home.grace/jr2728/projects/property_taxes/output/border_rd_results.duckdb"
con <- dbConnect(duckdb::duckdb(), db_file)
dbWriteTable(con, "results", results, overwrite = TRUE)
dbDisconnect(con, shutdown = TRUE)
cat("Results also saved to:", db_file, "\n")

# Print preview
cat("\nPreview of results:\n")
print(head(results |> select(state1, state2, year, coef, coef_bc, bw)))
