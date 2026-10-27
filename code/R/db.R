# code/R/db.R -- DuckDB connections (loaded by code/R/paths.R).
#   db_open("database.db")   read-only connection to a DuckDB file under data/ (database.db = SOI returns, ACS imputations,
#                            HUD crosswalk; census_finances_test.db = Census of Governments). Never opened read-write here.
#   db_mem()                 in-memory connection for querying parquet/CSV files.
#   dq(sql, con)             run a query, return a data.table; opens and closes an in-memory connection when con is missing.
# The cluster databases (corelogic.db on Bouchet, corelogic_public.db on SOM) are opened from SQL run there, not from R.
db_open <- function(name) {
  f <- file.path(DATA, name)
  if (!file.exists(f)) stop("no such database: ", f)
  DBI::dbConnect(duckdb::duckdb(), f, read_only = TRUE)
}
db_mem <- function() DBI::dbConnect(duckdb::duckdb())
dq <- function(sql, con = NULL) {
  if (is.null(con)) { con <- db_mem(); on.exit(DBI::dbDisconnect(con, shutdown = TRUE)) }
  data.table::as.data.table(DBI::dbGetQuery(con, sql))
}
