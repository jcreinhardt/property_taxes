gc()
rm(list = ls())

library(duckdb)
library(DBI)

con <- dbConnect(duckdb::duckdb(), dbdir = "../../../data/database.db")

# Resolve absolute path for DuckDB SQL (read_csv_auto needs absolute or cwd-relative)
csv_path <- normalizePath("../../../data/ACS_microdata/usa_00004.csv", mustWork = TRUE)

# Import via DuckDB native CSV reader — avoids loading 17GB into R memory
message("Importing ACS microdata (this may take several minutes)...")
dbExecute(con, sprintf(
  "CREATE OR REPLACE TABLE acs_microdata AS SELECT * FROM read_csv_auto('%s')",
  csv_path
))

# Rename all columns to lowercase to match project conventions
cols <- dbListFields(con, "acs_microdata")
for (col in cols[cols != tolower(cols)]) {
  dbExecute(con, sprintf('ALTER TABLE acs_microdata RENAME "%s" TO "%s"', col, tolower(col)))
}

# ---- Validation ----
cat("\nTotal rows:\n")
print(dbGetQuery(con, "SELECT COUNT(*) AS n FROM acs_microdata"))

cat("\nRows by year:\n")
print(dbGetQuery(con, "
  SELECT year, COUNT(*) AS n_rows, COUNT(DISTINCT serial) AS n_households
  FROM acs_microdata
  GROUP BY year ORDER BY year
"))

cat("\nOwnership status distribution:\n")
print(dbGetQuery(con, "
  SELECT ownershp, COUNT(*) AS n
  FROM acs_microdata
  GROUP BY ownershp ORDER BY ownershp
"))

cat("\nProperty tax (proptx99) summary for owner-occupied (ownershp = 1):\n")
print(dbGetQuery(con, "
  SELECT
    MIN(proptx99) AS min,
    MAX(proptx99) AS max,
    AVG(proptx99) AS mean,
    COUNT(*) AS n,
    SUM(CASE WHEN proptx99 = 0 THEN 1 ELSE 0 END) AS n_zero
  FROM acs_microdata
  WHERE ownershp = 1
"))

message("Done. Table 'acs_microdata' written to database.db.")
dbDisconnect(con, shutdown = TRUE)
