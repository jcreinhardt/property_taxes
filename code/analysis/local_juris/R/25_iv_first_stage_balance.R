# Border-distance IV: first stage and balance. Tract data from R/24 (iv_tract.parquet). For each tract and T in {30, 60}:
#   tax_gap  = own median effective rate - 10th pct of reachable tracts' medians (a cheaper outside option in reach)
#   sch_gap  = 90th pct of reachable tracts' district achievement - own district achievement (a better school in reach, SD units)
#   x_cheap  = 1 if some reachable tract in another state is >= 0.5 pp cheaper (cross-border outside option)
# Regressions on distance-to-border bins (ref: 50+ mi), state FE (and county FE), SE clustered by county.
# Balance: own-tract characteristics (achievement, SES, income, home value, education, race, tenure, age of stock, own rate).
suppressPackageStartupMessages({library(data.table); library(duckdb); library(fixest); library(ggplot2)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/local_juris")
con <- dbConnect(duckdb())
t <- as.data.table(dbGetQuery(con, sprintf("SELECT * FROM '%s/data/local_juris/iv_tract.parquet'", ROOT)))
duckdb_register(con, "tt", t[, .(tract, med, ach)])
p <- as.data.table(dbGetQuery(con, sprintf("SELECT m.o, m.m, b.med med_d, b.ach ach_d, left(m.o, 2) <> left(m.d, 2) xst
  FROM '%s/data/external/csds_ttm/tract_drive_le90.parquet' m JOIN tt a ON a.tract = m.o JOIN tt b ON b.tract = m.d
  WHERE m.o <> m.d AND m.m <= 60", ROOT)))
dbDisconnect(con, shutdown = TRUE)
p <- merge(p, t[, .(o = tract, med_o = med, ach_o = ach)], by = "o")
nb <- rbindlist(lapply(c(30, 60), \(T) p[m <= T, .(n_nb = .N, tax_gap = med_o[1] - quantile(med_d, .1),
  sch_gap = quantile(ach_d, .9, na.rm = TRUE) - ach_o[1], sch_best = max(ach_d, na.rm = TRUE) - ach_o[1],
  x_cheap = as.integer(any(xst & med_d <= med_o - .5)), x_better_sch = as.integer(any(xst & ach_d >= ach_o + .25, na.rm = TRUE)),
  sh_x = mean(xst)), by = .(tract = o)][, T := T]))
d <- merge(t, nb, by = "tract")
d[, dbin := cut(dist_mi, c(0, 5, 10, 20, 30, 50, Inf), right = FALSE)][, dbin := relevel(dbin, ref = "[50,Inf)")]
d[, `:=`(linc = log(inc), lhval = log(hval), lpop = log(pmax(pop, 1)))]
saveRDS(d, file.path(ROOT, "data/local_juris/iv_tract_nb.rds"))
fs <- list(); for (T0 in c(30, 60)) for (fe in c("st", "cty")) for (y in c("tax_gap", "x_cheap", "sch_gap", "x_better_sch", "sh_x")) {
  f <- feols(as.formula(sprintf("%s ~ i(dbin, ref = '[50,Inf)') | %s", y, fe)), d[T == T0], cluster = ~cty)
  ct <- as.data.table(coeftable(f), keep.rownames = "term")[, `:=`(outcome = y, T = T0, fe = fe, n = nobs(f), mean_far = d[T == T0 & dist_mi >= 50, mean(get(y), na.rm = TRUE)])]
  fs[[length(fs) + 1]] <- ct }
fs <- rbindlist(fs)[, term := sub("dbin::", "", term)]
bal <- rbindlist(lapply(c("ach", "ses", "linc", "lhval", "sh_ba", "sh_white", "sh_own", "yrbuilt", "lpop", "med"), \(y) {
  rbindlist(lapply(c("st", "cty"), \(fe) { f <- feols(as.formula(sprintf("%s ~ i(dbin, ref = '[50,Inf)') | %s", y, fe)), d[T == 30], cluster = ~cty)
    as.data.table(coeftable(f), keep.rownames = "term")[, `:=`(outcome = y, fe = fe, sd_y = sd(d[T == 30][[y]], na.rm = TRUE))] })) }))
bal[, term := sub("dbin::", "", term)][, std := Estimate / sd_y]
fwrite(fs, file.path(OUT, "iv_first_stage.csv")); fwrite(bal, file.path(OUT, "iv_balance.csv"))
sink(file.path(OUT, "iv_first_stage_balance.txt"))
cat("Tracts:", d[T == 30, .N], "(30 min) | distance bins (miles), ref 50+; SE clustered by county\n")
print(d[T == 30, .N, by = dbin][order(dbin)])
for (k in split(fs, by = c("T", "fe"))) { cat("\n== First stage / school access | T =", k$T[1], "min | FE:", k$fe[1], "\n")
  print(dcast(k, term ~ outcome, value.var = "Estimate")[, lapply(.SD, \(x) if (is.numeric(x)) round(x, 3) else x)])
  cat("SE:\n"); print(dcast(k, term ~ outcome, value.var = "Std. Error")[, lapply(.SD, \(x) if (is.numeric(x)) round(x, 3) else x)])
  cat("mean at 50+ mi:", paste(unique(k[, .(outcome, m = round(mean_far, 3))])[, paste(outcome, m)], collapse = "; "), "\n") }
for (fe0 in c("st", "cty")) { cat("\n== Balance (own tract), T = 30 sample, FE:", fe0, "| coefficient in SD units (SE in SD units)\n")
  b <- bal[fe == fe0]; b[, cell := sprintf("%.3f (%.3f)", std, `Std. Error` / sd_y)]
  print(dcast(b, outcome ~ term, value.var = "cell")) }
sink(); cat(readLines(file.path(OUT, "iv_first_stage_balance.txt")), sep = "\n")
