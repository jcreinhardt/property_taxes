# Can a median regression bill ~ a + r * AV within a tax-area cell recover the nominal rate?
# Test on the 1% building sample (2016, residential): compare the slope with the published total_property_tax_rate_percent
# (18 states). Alternatives: median of bill/AV (no intercept) and the 90th percentile of bill/AV ("full rate").
suppressPackageStartupMessages({library(dplyr); library(duckdb); library(quantreg); library(tidyr)})
source("code/R/paths.R")
OUT  <- out_dir("validation/corelogic")
con <- dbConnect(duckdb())
d <- dbGetQuery(con, sprintf("SELECT st, fips, coalesce(nullif(trim(tac),''),'_') tac, bill16 bill, av16 av, rate_pct, hs16, oo16
  FROM '%s/data/built/corelogic/parcel_1pct/parcels.parquet' WHERE bill16 > 0 AND av16 > 0 AND st <> 'VA'", ROOT))
dbDisconnect(con, shutdown = TRUE)
# published-rate unit by state (checked against median bill/AV): percent in NC, NJ, IN, IL, MO, DC; mills elsewhere
unit <- d |> filter(rate_pct > 0) |> distinct(st) |> mutate(div = ifelse(st %in% c("NC", "NJ", "IN", "IL", "MO", "DC"), 100, 1000))
d <- d |> left_join(unit, by = "st") |> mutate(pub = rate_pct / div)

fit <- function(g) {
  r <- tryCatch(coef(rq(bill ~ av, tau = .5, data = g)), error = function(e) c(NA, NA))
  tibble(n = nrow(g), a = r[1], slope = r[2], med_ratio = median(g$bill / g$av), p90_ratio = unname(quantile(g$bill / g$av, .9)),
         pub = median(g$pub, na.rm = TRUE), pub_disp = if (all(is.na(g$pub))) NA else mad(g$pub, na.rm = TRUE) / median(g$pub, na.rm = TRUE),
         hs_share = mean(g$hs16, na.rm = TRUE))
}
cells <- d |> group_by(st, fips, tac) |> filter(n() >= 30) |> group_modify(~ fit(.x)) |> ungroup()
saveRDS(cells, file.path(OUT, "rate_cells_1pct.rds"))

within <- function(x, y, tol) mean(abs(x / y - 1) <= tol, na.rm = TRUE)
cmp <- cells |> filter(!is.na(pub), pub > 0) |> group_by(st) |> summarise(
  cells = n(), parcels = sum(n), pub_rate = round(100 * median(pub), 3), pub_varies_in_cell = round(mean(pub_disp > .02), 2),
  slope_med = round(100 * median(slope / pub), 1), slope_5 = round(100 * within(slope, pub, .05)), slope_10 = round(100 * within(slope, pub, .10)),
  ratio_5 = round(100 * within(med_ratio, pub, .05)), p90_5 = round(100 * within(p90_ratio, pub, .05)),
  neg_intercept = round(100 * mean(a < 0, na.rm = TRUE))) |> arrange(desc(parcels))
sink(file.path(OUT, "nominal_rate_test.txt"))
cat("Cells (fips x tac, >= 30 parcels):", nrow(cells), " parcels:", sum(cells$n), "of", nrow(d), "\n")
cat("slope_med = median slope/published (%); *_5/_10 = % of cells within 5/10% of published; pub_varies_in_cell = share of cells where the published rate itself varies > 2% (MAD/median)\n\n")
print(as.data.frame(cmp), row.names = FALSE)
cat("\nIL by county (slope / published rate; Cook's 2016 state equaliser is 2.8032):\n")
print(as.data.frame(cells |> filter(st == "IL", !is.na(pub)) |> group_by(fips) |> summarise(cells = n(), slope_over_pub = round(median(slope / pub), 3)) |> arrange(desc(cells)) |> head(6)), row.names = FALSE)
sink()
cat(readLines(file.path(OUT, "nominal_rate_test.txt")), sep = "\n")
