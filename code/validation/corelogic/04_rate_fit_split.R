# Fit test of 03 with cells split by exemption status: (a) homestead_exempt_indicator, (b) owner-occupancy code O,
# (c) both. Share of parcels within 1% of their cell-group's median-regression line. 1% sample, 2016, residential;
# groups need >= 30 parcels; the share is computed over parcels in groups that qualify, and coverage is reported.
suppressPackageStartupMessages({library(dplyr); library(tidyr); library(duckdb); library(quantreg)})
source("code/R/paths.R"); OUT <- out_dir("validation/corelogic")
con <- dbConnect(duckdb())
d <- dbGetQuery(con, sprintf("SELECT clip, st, fips, coalesce(nullif(trim(tac),''), 'M:' || upper(trim(muni_name)), '_') cell, bill16 bill, av16 av,
  coalesce(hs16, false) hs, coalesce(occ = 'O', false) oo FROM '%s/data/built/corelogic/parcel_1pct/parcels.parquet' WHERE bill16 > 0 AND av16 > 0", ROOT))
dbDisconnect(con, shutdown = TRUE)
onl <- function(g) {
  r <- tryCatch(coef(rq(bill ~ av, tau = .5, data = g)), error = function(e) c(NA, NA))
  tibble(clip = g$clip, on1 = abs(g$bill - (r[1] + r[2] * g$av)) / g$bill <= .01, slope = r[2])
}
run <- function(split) {
  d |> mutate(grp = case_when(split == "none" ~ "", split == "hs" ~ as.character(hs), split == "oo" ~ as.character(oo),
                              TRUE ~ paste(hs, oo))) |>
    group_by(st, fips, cell, grp) |> filter(n() >= 30) |> group_modify(~ onl(.x)) |> ungroup() |> mutate(split = split)
}
res <- bind_rows(lapply(c("none", "hs", "oo", "hs_oo"), run))
saveRDS(res, file.path(OUT, "rate_fit_split_1pct.rds"))
n_st <- d |> count(st, name = "N")
tab <- res |> group_by(st, split) |> summarise(on = round(100 * mean(on1, na.rm = TRUE)), cov = n(), .groups = "drop") |>
  left_join(n_st, by = "st") |> mutate(cov = round(100 * cov / N)) |> select(-N) |>
  pivot_wider(names_from = split, values_from = c(on, cov)) |>
  mutate(best = pmax(on_none, on_hs, on_oo, on_hs_oo, na.rm = TRUE)) |> arrange(desc(best))
# slope ratio homestead vs not (proportional relief shows up as ratio < 1), median over cells with both groups
hs_ratio <- d |> mutate(grp = hs) |> group_by(st, fips, cell, grp) |> filter(n() >= 30) |>
  summarise(slope = tryCatch(coef(rq(bill ~ av, tau = .5))[2], error = function(e) NA), .groups = "drop") |>
  pivot_wider(names_from = grp, values_from = slope, names_prefix = "hs_") |> filter(!is.na(hs_TRUE), !is.na(hs_FALSE)) |>
  group_by(st) |> summarise(cells_both = n(), slope_hs_over_non = round(median(hs_TRUE / hs_FALSE), 3))
sink(file.path(OUT, "rate_fit_split.txt"))
cat("on_*: % of parcels within 1% of their group's line; cov_*: % of the state's parcels in groups with >= 30 parcels.\n",
    "Splits: none = cell; hs = cell x homestead flag; oo = cell x occupancy code O; hs_oo = both.\n\n")
print(as.data.frame(tab), row.names = FALSE)
cat("\nMedian slope ratio homestead / non-homestead within cells (proportional relief < 1):\n")
print(as.data.frame(hs_ratio |> arrange(slope_hs_over_non)), row.names = FALSE)
sink(); cat(readLines(file.path(OUT, "rate_fit_split.txt")), sep = "\n")
