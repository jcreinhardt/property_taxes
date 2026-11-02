# Does balance in distance to the border get better the more rural the sample? Sweep of the density cutoff (tracts below
# D per sq mi kept; D = none, 5,000, 2,000, 1,000, 500, 250, 100) on both sides within 50 mi, all pairs, same regressions
# as 23: standardised covariate on Z with pair FE (raw and with log density) and on Z + Z x E + E + log density (Z x E),
# Z = -distance / 10 mi or near < 10 mi. Reported per cutoff: tracts, pairs, controls at p < .05 and after Holm (of 15),
# median and max |b| over the controls, the three worst controls; figure of the summary against the cutoff.
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); MX <- 50
CUTS <- c(Inf, 5000, 2000, 1000, 500, 250, 100)
tz <- fread(file.path(DATA, "built/exposure/salt_exposure_tract_zip2016.csv"), colClasses = c(tract = "character", zip = "character"))[, .(tract, zip)]
cz <- fread(file.path(DATA, "built/exposure/exposure_components_zip.csv"), colClasses = c(geo = "character"))[returns16 >= 100 & is.finite(d_salt_own), .(zip = geo, d_salt_own)]
load_side <- function(s) { d <- readRDS(file.path(DATA, sprintf("built/geo/%s_side_balance_tracts.rds", s)))[dist_mi < MX & is.finite(ldens)]
  d <- merge(d, merge(tz, cz, by = "zip")[, .(tract, d_salt_own)], by = "tract", all.x = TRUE)
  d[, `:=`(side = s, dens = exp(ldens), z = -dist_mi / 10, near = as.integer(dist_mi < 10), ex = (d_salt_own - mean(d_salt_own, na.rm = TRUE)) / sd(d_salt_own, na.rm = TRUE))] }
D <- list(low = load_side("low"), high = load_side("high"))
v <- c(med = "effective tax rate", lp50 = "log sale price 2016", lhval = "log home value", moveout = "move-out rate", sh_moved = "moved last year", turnover = "sales per 100 parcels",
       d_salt_own = "own lost subsidy", linc = "log income", sh_agi100 = "share AGI >= 100k", sh_item = "itemiser share", age = "median age", hhsize = "household size", sh_u18 = "share under 18",
       sh_hh_kids = "share HHs with kids", sh_own = "owner share", sh_ba = "share BA+", sh_white = "share white", yrbuilt = "year built", lpop = "log population", ach = "test scores", ses = "district SES", ldens = "log density")
ctrl <- names(v)[8:21]; ctrl_d <- c(ctrl, "ldens")   # 14 controls + density (raw only)
specs <- list(raw = "%s", dens = "%s + ldens", inter = "%s + %s:ex + ex + ldens")
run <- function(x, sds, k, zz, sp) { if (k == "ldens" && sp != "raw") return(NULL)
  x <- x[is.finite(get(k))][, y := get(k) / sds[k]]; if (sp == "inter") x <- x[is.finite(ex)]
  m <- feols(as.formula(sprintf("y ~ %s | pair", gsub("%s", zz, specs[[sp]], fixed = TRUE))), x, cluster = ~cty); ct <- as.data.table(coeftable(m), keep.rownames = "term")
  ct[, .(var = k, instrument = zz, spec = sp, term, b = Estimate, se = `Std. Error`, p = `Pr(>|t|)`, n = nobs(m))] }
res <- list()
for (s in names(D)) { sds <- sapply(names(v), \(k) sd(D[[s]][[k]], na.rm = TRUE))
  for (D0 in CUTS) { x <- D[[s]][dens < D0]; cat(sprintf("%s side, density < %s: %d tracts, %d pairs\n", s, format(D0, big.mark = ","), nrow(x), uniqueN(x$pair)))
    for (zz in c("z", "near")) for (sp in names(specs)) for (k in names(v)) { r <- run(x, sds, k, zz, sp); if (!is.null(r)) res[[length(res) + 1]] <- r[, `:=`(side = s, cut = D0, tracts = nrow(x), pairs = uniqueN(x$pair))] } } }
res <- rbindlist(res)[, term := fcase(term == instrument, "Z", term == "ex", "E", term == "ldens", "density", default = "Z x E")]
res <- res[(spec == "raw" & term == "Z") | (spec == "dens" & term == "Z") | (spec == "inter" & term == "Z x E")]
res[var %in% ctrl_d, p_holm := p.adjust(p, "holm"), by = .(side, cut, instrument, spec)]
res[, series := factor(fcase(spec == "raw", "Z, pair FE", spec == "dens", "Z, pair FE + log density", default = "Z x E, given E and density"), c("Z, pair FE", "Z, pair FE + log density", "Z x E, given E and density"))]
fwrite(res, file.path(OUT, "c37_density_cutoff_sweep_all.csv"))
sm <- res[var %in% ctrl_d, .(tracts = tracts[1], pairs = pairs[1], k = .N, p05 = sum(p < .05), holm05 = sum(p_holm < .05), gt_0.1 = sum(abs(b) > .1), median_abs = median(abs(b)), max_abs = max(abs(b)),
                             worst = paste(sprintf("%s %+.2f", v[var[order(-abs(b))][1:3]], b[order(-abs(b))][1:3]), collapse = ", "),
                             rate = b[var == "med"][1], price = b[var == "lp50"][1]), by = .(side, instrument, series, cut)]
# rate / price are not controls: pull them separately
op <- res[var %in% c("med", "lp50"), .(side, instrument, series, cut, var, b)]; sm[, c("rate", "price") := NULL]
sm <- merge(sm, dcast(op, side + instrument + series + cut ~ var, value.var = "b")[, .(side, instrument, series, cut, rate = med, price = lp50)], by = c("side", "instrument", "series", "cut"))
fwrite(sm, file.path(OUT, "c37_density_cutoff_sweep.csv"))
sink(file.path(OUT, "c37_density_cutoff_sweep.txt"), split = TRUE)
cat("Density cutoff sweep, both sides, all pairs within 50 mi. Controls: 14 (+ log density in the raw spec). Cells: controls at p < .05 / Holm, median |b|, max |b| (SD units), three worst; rate and price = coefficient on the rate and 2016 price.\n")
for (zz in c("z", "near")) for (s in names(D)) { cat("\n==", ifelse(zz == "z", "per 10 mi closer", "near < 10 mi"), "|", s, "side\n")
  print(as.data.frame(sm[instrument == zz & side == s][order(series, -cut), .(series, cut, tracts, pairs, p05, holm05, gt_0.1, median_abs = round(median_abs, 3), max_abs = round(max_abs, 2), rate = round(rate, 3), price = round(price, 3), worst)]), row.names = FALSE) }
sink()
pl <- melt(sm, id.vars = c("side", "instrument", "series", "cut", "tracts"), measure.vars = c("p05", "median_abs", "max_abs"), variable.name = "stat")
pl[, `:=`(stat = factor(stat, c("p05", "median_abs", "max_abs"), c("controls at p < .05 (of 14-15)", "median |b|, SD units", "max |b|, SD units")), cutl = factor(ifelse(is.finite(cut), format(cut, big.mark = ","), "none"), c("none", format(CUTS[-1], big.mark = ","))),
          instrument = factor(instrument, c("z", "near"), c("per 10 mi closer", "near < 10 mi")), side = factor(side, c("low", "high"), c("low-tax side", "high-tax side")))]
g <- ggplot(pl, aes(cutl, value, colour = series, group = series)) + geom_line() + geom_point(size = 1.4) +
  facet_grid(stat ~ side + instrument, scales = "free_y") + scale_colour_manual(NULL, values = c("grey45", "#2a64a8", "#c0392b")) +
  labs(x = "density cutoff: tracts below D per sq mi kept", y = NULL, title = "Balance in distance to the border as the sample gets more rural: density cutoff sweep, all pairs within 50 mi",
       subtitle = sprintf("tracts kept, low / high side: %s", paste(sprintf("%s: %s / %s", levels(pl$cutl), format(sm[side == "low" & instrument == "z" & series == levels(sm$series)[1]][order(-cut), tracts], big.mark = ","), format(sm[side == "high" & instrument == "z" & series == levels(sm$series)[1]][order(-cut), tracts], big.mark = ",")), collapse = "; "))) +
  theme_minimal(base_size = 8) + theme(legend.position = "bottom", axis.text.x = element_text(angle = 40, hjust = 1))
ggsave(file.path(OUT, "figures/c37_density_cutoff_sweep.png"), g, width = 12, height = 7, dpi = 150)
