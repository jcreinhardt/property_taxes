# Border IV, directional version (split out of local_juris/R/26_iv_high_side.R, 2026-10-27; the tract -> border pair / side
# assignment it used is now code/build/geo/06_tract_border_side.R). Only the higher-tax side of a state border gains a cheaper
# outside option from being near it; both sides share whatever makes border areas different. Instrument: near-border bins x
# high side. Spec: Y ~ i(dbin, high, ref = 50+) | pair^high + pair^dbin, SE clustered by county; plus pooled near (< 10 mi)
# x high. Y: first stage (x_cheap, tax_gap, sh_x), school access (sch_gap, x_better_sch) and own-tract balance covariates.
suppressPackageStartupMessages({library(data.table); library(fixest)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_iv")
d <- readRDS(file.path(DATA, "built/geo/iv_tract_side.rds"))
ys <- c("x_cheap", "tax_gap", "sh_x", "sch_gap", "x_better_sch", "ach", "ses", "linc", "lhval", "sh_ba", "sh_white", "sh_own", "yrbuilt", "lpop")
res <- rbindlist(lapply(c(30, 60), \(T0) rbindlist(lapply(ys, \(y) {
  dd <- d[T == T0]; sdy <- sd(dd[[y]], na.rm = TRUE)
  f1 <- feols(as.formula(sprintf("%s ~ i(dbin, high, ref = '[50,Inf)') | pair^high + pair^dbin", y)), dd, cluster = ~cty)
  f2 <- feols(as.formula(sprintf("%s ~ near:high | pair^high + pair^near", y)), dd, cluster = ~cty)
  rbind(as.data.table(coeftable(f1), keep.rownames = "term"), as.data.table(coeftable(f2), keep.rownames = "term"))[,
    `:=`(outcome = y, T = T0, sd_y = sdy, n = nobs(f2))] }))))
res[, term := gsub('dbin::|:high|"|\\)', "", term)]
fwrite(res, file.path(OUT, "iv_high_side.csv"))
sink(file.path(OUT, "iv_high_side.txt"))
cat("Directional border IV: near-border x high-tax side; pair x side FE + pair x distance-bin FE; SE by county\n")
cat("tracts:", d[T == 30, .N], "| pairs:", uniqueN(d$pair), "| high-side tracts:", d[T == 30, sum(high)], "\n")
for (T0 in c(30, 60)) { cat("\n== T =", T0, "min. Cells: coefficient (SE); last column also in SD units of Y\n")
  r <- res[T == T0]; r[, cell := sprintf("%.3f (%.3f)", Estimate, `Std. Error`)]
  w <- dcast(r, outcome ~ term, value.var = "cell"); w <- merge(w, unique(r[term == "near", .(outcome, near_x_high_sd = round(Estimate / sd_y, 3))]), by = "outcome")
  print(w[match(ys, outcome)]) }
sink(); cat(readLines(file.path(OUT, "iv_high_side.txt")), sep = "\n")
