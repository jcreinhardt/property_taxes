# Test of the median-regression rate that needs no published rate: if a cell has one flat rate (plus lump-sum exemption or
# fixed charge), most bills lie exactly on the fitted line. Share of parcels with |bill - (a + r*AV)| <= 1% of bill, per state.
# Cells = fips x tax area code (fips x municipality_name where no tac: MA, RI, MI). Calibrated against states where the slope
# matched the published rate (NJ, CT, NH, ME). 1% building sample, 2016, residential.
suppressPackageStartupMessages({library(dplyr); library(duckdb); library(quantreg)})
source("code/R/paths.R"); OUT <- out_dir("validation/corelogic")
con <- dbConnect(duckdb())
d <- dbGetQuery(con, sprintf("SELECT st, fips, coalesce(nullif(trim(tac),''), 'M:' || upper(trim(muni_name)), '_') cell, bill16 bill, av16 av, hs16
  FROM '%s/data/built/corelogic/parcel_1pct/parcels.parquet' WHERE bill16 > 0 AND av16 > 0", ROOT))
dbDisconnect(con, shutdown = TRUE)
fit <- function(g) {
  r <- tryCatch(coef(rq(bill ~ av, tau = .5, data = g)), error = function(e) c(NA, NA))
  e <- abs(g$bill - (r[1] + r[2] * g$av)) / g$bill
  tibble(n = nrow(g), slope = r[2], on1 = mean(e <= .01), on5 = mean(e <= .05))
}
cells <- d |> group_by(st, fips, cell) |> filter(n() >= 30) |> group_modify(~ fit(.x)) |> ungroup()
cov <- d |> count(st, name = "parcels_all")
tab <- cells |> group_by(st) |> summarise(cells = n(), parcels = sum(n), on_line_1pct = round(100 * weighted.mean(on1, n)),
  on_line_5pct = round(100 * weighted.mean(on5, n)), cells_half_on_line = round(100 * mean(on1 >= .5)),
  med_slope_pct = round(100 * median(slope, na.rm = TRUE), 2)) |>
  left_join(cov, by = "st") |> mutate(covered = round(100 * parcels / parcels_all)) |> select(-parcels_all) |> arrange(desc(on_line_1pct))
saveRDS(cells, file.path(OUT, "rate_fit_cells_1pct.rds"))
sink(file.path(OUT, "rate_fit_all_states.txt"))
cat("on_line_1pct: % of parcels within 1% of the cell's median-regression line (parcel-weighted); cells_half_on_line: % of cells\n",
    "where >= half the parcels are on the line; covered: % of the state's parcels in cells with >= 30 parcels\n\n")
print(as.data.frame(tab), row.names = FALSE)
sink(); cat(readLines(file.path(OUT, "rate_fit_all_states.txt")), sep = "\n")
