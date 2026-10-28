# A pair-level rural rule instead of a tract-level density cut: each border pair's window density = population / land area
# of all tracts within 50 mi on both sides (also by side, and the population-weighted mean tract density); keep every tract
# of the pairs whose window density is below D (D = none, 2,000, 1,000, 500, 250 per sq mi), dense tracts included, and
# run the balance regressions of 23 on both sides: standardised covariate on Z with pair FE (raw, with log density) and
# on Z + Z x E + E + log density (Z x E); Z = -distance / 10 mi or near < 10 mi. Same summary and figure as 24.
# PAIR_DENS=pw ranks pairs by the population-weighted mean tract density instead (thresholds 4,000 / 2,000 / 1,000 / 500; outputs _pw).
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); MX <- 50; PM <- Sys.getenv("PAIR_DENS", "window"); SFX <- ifelse(PM == "pw", "_pw", "")
CUTS <- if (PM == "pw") c(Inf, 4000, 2000, 1000, 500) else c(Inf, 2000, 1000, 500, 250)
tz <- fread(file.path(DATA, "built/exposure/salt_exposure_tract_zip2016.csv"), colClasses = c(tract = "character", zip = "character"))[, .(tract, zip)]
cz <- fread(file.path(DATA, "built/exposure/exposure_components_zip.csv"), colClasses = c(geo = "character"))[returns16 >= 100 & is.finite(d_salt_own), .(zip = geo, d_salt_own)]
load_side <- function(s) { d <- readRDS(file.path(DATA, sprintf("built/geo/%s_side_balance_tracts.rds", s)))[dist_mi < MX & is.finite(ldens)]
  d <- merge(d, merge(tz, cz, by = "zip")[, .(tract, d_salt_own)], by = "tract", all.x = TRUE)
  d[, `:=`(side = s, dens = exp(ldens), z = -dist_mi / 10, near = as.integer(dist_mi < 10), ex = (d_salt_own - mean(d_salt_own, na.rm = TRUE)) / sd(d_salt_own, na.rm = TRUE))] }
D <- list(low = load_side("low"), high = load_side("high")); all <- rbindlist(D, fill = TRUE)[is.finite(pop) & is.finite(aland)]
pd <- all[, .(tracts = .N, pop_m = sum(pop) / 1e6, dens_window = sum(pop) / sum(aland / 2589988), dens_pw = weighted.mean(dens, pop)), by = pair]
pd <- merge(pd, dcast(all[, .(d = sum(pop) / sum(aland / 2589988)), by = .(pair, side)], pair ~ side, value.var = "d")[, .(pair, dens_low = low, dens_high = high)], by = "pair")[order(dens_window)]
fwrite(pd, file.path(OUT, "c39_pair_density.csv"))
v <- c(med = "effective tax rate", lp50 = "log sale price 2016", lhval = "log home value", moveout = "move-out rate", sh_moved = "moved last year", turnover = "sales per 100 parcels",
       d_salt_own = "own lost subsidy", linc = "log income", sh_agi100 = "share AGI >= 100k", sh_item = "itemiser share", age = "median age", hhsize = "household size", sh_u18 = "share under 18",
       sh_hh_kids = "share HHs with kids", sh_own = "owner share", sh_ba = "share BA+", sh_white = "share white", yrbuilt = "year built", lpop = "log population", ach = "test scores", ses = "district SES", ldens = "log density")
ctrl_d <- c(names(v)[8:21], "ldens")
specs <- list(raw = "%s", dens = "%s + ldens", inter = "%s + %s:ex + ex + ldens")
run <- function(x, sds, k, zz, sp) { if (k == "ldens" && sp != "raw") return(NULL)
  x <- x[is.finite(get(k))][, y := get(k) / sds[k]]; if (sp == "inter") x <- x[is.finite(ex)]
  m <- feols(as.formula(sprintf("y ~ %s | pair", gsub("%s", zz, specs[[sp]], fixed = TRUE))), x, cluster = ~cty); ct <- as.data.table(coeftable(m), keep.rownames = "term")
  ct[, .(var = k, instrument = zz, spec = sp, term, b = Estimate, se = `Std. Error`, p = `Pr(>|t|)`, n = nobs(m))] }
res <- list()
for (s in names(D)) { sds <- sapply(names(v), \(k) sd(D[[s]][[k]], na.rm = TRUE))
  for (D0 in CUTS) { keep <- pd[get(paste0("dens_", PM)) < D0, pair]; x <- D[[s]][pair %in% keep]
    cat(sprintf("%s side, pairs with %s density < %s: %d pairs, %d tracts, %.0f%% of tracts above 1,000 per sq mi\n", s, PM, format(D0, big.mark = ","), uniqueN(x$pair), nrow(x), 100 * mean(x$dens >= 1000)))
    for (zz in c("z", "near")) for (sp in names(specs)) for (k in names(v)) { r <- run(x, sds, k, zz, sp); if (!is.null(r)) res[[length(res) + 1]] <- r[, `:=`(side = s, cut = D0, tracts = nrow(x), pairs = uniqueN(x$pair))] } } }
res <- rbindlist(res)[, term := fcase(term == instrument, "Z", term == "ex", "E", term == "ldens", "density", default = "Z x E")]
res <- res[(spec == "raw" & term == "Z") | (spec == "dens" & term == "Z") | (spec == "inter" & term == "Z x E")]
res[var %in% ctrl_d, p_holm := p.adjust(p, "holm"), by = .(side, cut, instrument, spec)]
res[, series := factor(fcase(spec == "raw", "Z, pair FE", spec == "dens", "Z, pair FE + log density", default = "Z x E, given E and density"), c("Z, pair FE", "Z, pair FE + log density", "Z x E, given E and density"))]
fwrite(res, file.path(OUT, paste0("c39_pair_density_rule", SFX, "_all.csv")))
sm <- res[var %in% ctrl_d, .(tracts = tracts[1], pairs = pairs[1], k = .N, p05 = sum(p < .05), holm05 = sum(p_holm < .05), gt_0.1 = sum(abs(b) > .1), median_abs = median(abs(b)), max_abs = max(abs(b)),
                             worst = paste(sprintf("%s %+.2f", v[var[order(-abs(b))][1:3]], b[order(-abs(b))][1:3]), collapse = ", ")), by = .(side, instrument, series, cut)]
op <- dcast(res[var %in% c("med", "lp50"), .(side, instrument, series, cut, var, b)], side + instrument + series + cut ~ var, value.var = "b")[, .(side, instrument, series, cut, rate = med, price = lp50)]
sm <- merge(sm, op, by = c("side", "instrument", "series", "cut")); fwrite(sm, file.path(OUT, paste0("c39_pair_density_rule", SFX, ".csv")))
sink(file.path(OUT, paste0("c39_pair_density_rule", SFX, ".txt")), split = TRUE)
cat("Pair-level rural rule: pairs kept when the", PM, "density (population / land area of all tracts within 50 mi, both sides) is below D; all their tracts used.\n\nPairs by window density (per sq mi), with by-side densities and the population-weighted mean tract density:\n")
print(as.data.frame(pd[, .(pair, tracts, pop_m = round(pop_m, 2), dens_window = round(dens_window), dens_low = round(dens_low), dens_high = round(dens_high), dens_pw = round(dens_pw))]), row.names = FALSE)
cat("\nControls: 14 (+ log density in the raw spec). Cells: controls at p < .05 / Holm, median |b|, max |b| (SD units), three worst; rate and price = coefficient on the rate and 2016 price.\n")
for (zz in c("z", "near")) for (s in names(D)) { cat("\n==", ifelse(zz == "z", "per 10 mi closer", "near < 10 mi"), "|", s, "side\n")
  print(as.data.frame(sm[instrument == zz & side == s][order(series, -cut), .(series, cut, tracts, pairs, p05, holm05, gt_0.1, median_abs = round(median_abs, 3), max_abs = round(max_abs, 2), rate = round(rate, 3), price = round(price, 3), worst)]), row.names = FALSE) }
sink()
pl <- melt(sm, id.vars = c("side", "instrument", "series", "cut", "tracts", "pairs"), measure.vars = c("p05", "median_abs", "max_abs"), variable.name = "stat")
pl[, `:=`(stat = factor(stat, c("p05", "median_abs", "max_abs"), c("controls at p < .05 (of 14-15)", "median |b|, SD units", "max |b|, SD units")), cutl = factor(ifelse(is.finite(cut), format(cut, big.mark = ","), "none"), c("none", format(CUTS[-1], big.mark = ","))),
          instrument = factor(instrument, c("z", "near"), c("per 10 mi closer", "near < 10 mi")), side = factor(side, c("low", "high"), c("low-tax side", "high-tax side")))]
lab <- sm[instrument == "z" & series == levels(sm$series)[1]][order(-cut)]
g <- ggplot(pl, aes(cutl, value, colour = series, group = series)) + geom_line() + geom_point(size = 1.4) +
  facet_grid(stat ~ side + instrument, scales = "free_y") + scale_colour_manual(NULL, values = c("grey45", "#2a64a8", "#c0392b")) +
  labs(x = paste("pairs kept:", ifelse(PM == "pw", "population-weighted tract", "window"), "density below D per sq mi (all their tracts)"), y = NULL, title = paste0("Balance in distance to the border under a pair-level rural rule: pairs whose 50-mi ", ifelse(PM == "pw", "population-weighted tract", "window"), " density is below D"),
       subtitle = sprintf("pairs / tracts kept, low side: %s; high side: %s", paste(sprintf("%s: %d / %s", levels(pl$cutl), lab[side == "low", pairs], format(lab[side == "low", tracts], big.mark = ",")), collapse = "; "), paste(sprintf("%d / %s", lab[side == "high", pairs], format(lab[side == "high", tracts], big.mark = ",")), collapse = "; "))) +
  theme_minimal(base_size = 8) + theme(legend.position = "bottom", axis.text.x = element_text(angle = 40, hjust = 1))
ggsave(file.path(OUT, paste0("figures/c39_pair_density_rule", SFX, ".png")), g, width = 12, height = 7, dpi = 150)
