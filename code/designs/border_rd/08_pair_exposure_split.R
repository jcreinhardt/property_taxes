# Which side of each state border did the SALT cap hit harder? Sides in the border designs are ranked by the effective
# property-tax rate (build/geo/06: median of tract medians within 20 mi). This script ranks the same sides by SALT-cap
# exposure (build/exposure/05: lost federal subsidy per owner household, county SOI, d_salt_own; also the property-tax part
# d_prop_own) and labels each pair "agree" (the high-rate side is also the high-exposure side) or "disagree" (the high-rate
# side is the low-exposure side, typically a no-income-tax state next to an income-tax state). The difference in
# discontinuities of designs/border_rd/04 (RD of the within-tract change since 2017, h = 15 mi, pair-demeaned, SE by county)
# is then rerun separately for the two groups. Capitalisation of the cap predicts the high-exposure side loses value: in
# "agree" pairs the effective-rate gap (high minus low) should widen, in "disagree" pairs it should narrow.
suppressPackageStartupMessages({library(data.table); library(rdrobust); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); H <- 15; MX <- 100; YRS <- 2013:2024
s <- unique(readRDS(file.path(DATA, "built/geo/iv_tract_side.rds"))[T == 30, .(tract, leaid, pair, high, dist_mi, cty, st, side_med)])
ex <- fread(file.path(DATA, "built/exposure/exposure_components_county.csv"))[, cty := sprintf("%05d", geo)]
s <- merge(s, ex[, .(cty, returns16, d_salt_own, d_prop_own, d_nonprop_own)], by = "cty", all.x = TRUE)
fips <- unique(as.data.table(tigris::fips_codes)[, .(st = state_code, ab = state)]); s <- merge(s, fips, by = "st")
# ---- pair-level ranking by exposure (tracts within 20 mi, as for the rate ranking)
ps <- s[dist_mi < 20, .(st = ab[1], n20 = .N, eff = 100 * side_med[1], salt = median(d_salt_own, na.rm = TRUE), prop = median(d_prop_own, na.rm = TRUE),
                        nonprop = median(d_nonprop_own, na.rm = TRUE)), by = .(pair, high)]
pw <- dcast(ps, pair ~ high, value.var = c("st", "eff", "salt", "prop", "nonprop", "n20"))
setnames(pw, c("st_1", "st_0", "eff_1", "eff_0", "salt_1", "salt_0", "prop_1", "prop_0", "nonprop_1", "nonprop_0", "n20_1", "n20_0"),
             c("st_high", "st_low", "eff_high", "eff_low", "salt_high", "salt_low", "prop_high", "prop_low", "nonprop_high", "nonprop_low", "n_high", "n_low"))
cat("pairs with tracts within 20 mi on one side only (dropped):", paste(pw[is.na(st_low) | is.na(st_high), pair], collapse = " "), "\n")
pw <- pw[!is.na(st_low) & !is.na(st_high)]
pw[, `:=`(d_eff = eff_high - eff_low, d_salt = salt_high - salt_low, d_prop = prop_high - prop_low, d_nonprop = nonprop_high - nonprop_low)]
pw[, group := fifelse(d_salt > 0, "agree: high-rate side is high-exposure side", "disagree: high-rate side is low-exposure side")]
fwrite(pw[order(d_salt)], file.path(OUT, "c21_pair_exposure_sides.csv"))
s <- merge(s, pw[, .(pair, group, d_salt)], by = "pair")
# ---- tract-year panel as in 04
s[, sd_mi := ifelse(high == 1, dist_mi, -dist_mi)]; s <- s[abs(sd_mi) <= MX]
ty <- readRDS(file.path(DATA, "built/corelogic/tract_year.rds"))[y %in% YRS & tract %in% s$tract]
ty <- ty[, .(tract, y, eff = 100 * eff50, lnom = log(med_nom_res), lbill = log(bill / n_bill), lp50 = log(p50), lratio = log(ratio50))]
for (k in c("lnom", "lbill", "lp50", "lratio")) ty[!is.finite(get(k)), (k) := NA]
fin <- readRDS(file.path(DATA, "built/census_finances/district_finance.rds"))[enr > 0]
fin[, lenr_g := log(enr) - log(enr[y == 2017][1]), by = leaid]
fin <- fin[y %in% YRS, .(leaid, y, lspend = log(spend / enr), lptax = fifelse(ptax > 0, log(ptax / enr), NA_real_), lenr_g)]
d <- merge(CJ(tract = s$tract, y = YRS), s, by = "tract")
d <- merge(d, ty, by = c("tract", "y"), all.x = TRUE)
d <- merge(d, fin, by = c("leaid", "y"), all.x = TRUE)
v <- c(eff = "effective tax rate, sales (pp)", lbill = "log mean bill per parcel", lp50 = "log median sale price", lratio = "log median assessment ratio",
       lnom = "log nominal rate (bill / AV), roll", lspend = "district log spending per pupil", lptax = "district log property tax per pupil",
       lenr_g = "district log enrolment, change since FY2017")
for (k in names(v)) d[, (paste0(k, "_d")) := get(k) - get(k)[y == 2017][1], by = tract]
AG <- grep("^agree", unique(pw$group), value = TRUE)
sel <- function(x, g) if (g == "agree, excluding NJ-NY") x$group == AG & x$pair != "NJ-NY" else x$group == g   # NYC suburbs dominate the agree group
rund <- function(k, yy, g) {
  kd <- paste0(k, "_d"); x <- d[y == yy & sel(d, g) & is.finite(get(kd))]
  if (nrow(x) < 500) return(NULL)
  yv <- x[[kd]] - ave(x[[kd]], x$pair)
  r <- tryCatch(rdrobust(yv, x$sd_mi, c = 0, h = H, cluster = x$cty, vce = "hc1"), error = \(e) NULL)
  if (is.null(r)) return(NULL)
  data.table(var = k, label = v[k], group = g, y = yy, est = r$coef[1], lo = r$ci[3, 1], hi = r$ci[3, 2], p = r$pv[3], n_eff = sum(r$N_h)) }
grps <- c(sort(unique(pw$group)), "agree, excluding NJ-NY")
cat("\ntracts within 15 mi by group and pair (top 8 pairs):\n")
print(s[abs(sd_mi) <= H, .N, by = .(group, pair)][order(group, -N), head(.SD, 8), by = group])
cat("\ntracts within 15 mi by group:\n"); print(rbindlist(lapply(grps, \(g) data.table(group = g, tracts = sum(abs(s$sd_mi) <= H & sel(s, g)), pairs = uniqueN(s[abs(sd_mi) <= H & sel(s, g), pair])))))
res <- rbindlist(lapply(names(v), \(k) rbindlist(lapply(grps, \(g) { z <- rbindlist(lapply(setdiff(YRS, 2017), \(yy) rund(k, yy, g)))
  if (nrow(z)) rbind(z, data.table(var = k, label = v[k], group = g, y = 2017L, est = 0, lo = 0, hi = 0, p = NA_real_, n_eff = NA_integer_)) }))))[order(var, group, y)]
fwrite(res, file.path(OUT, "c21_diff_in_disc_by_exposure_side.csv"))
sink(file.path(OUT, "c21_pair_exposure_sides.txt"), split = TRUE)
cat("Border pairs: side ranked by effective rate (high = 1) vs by SALT-cap exposure (lost subsidy per owner household, $/yr, median of tracts within 20 mi)\n")
cat("pairs:", nrow(pw), "| agree:", sum(pw$d_salt > 0), "| disagree:", sum(pw$d_salt <= 0), "\n")
cat("no-income-tax states on the high-rate side:", sum(pw$st_high %in% c("FL", "NV", "SD", "TX", "WA", "WY", "NH", "TN")), "of", nrow(pw), "\n\n")
print(as.data.frame(pw[order(d_salt), .(pair, st_high, st_low, eff_high = round(eff_high, 2), eff_low = round(eff_low, 2), salt_high = round(salt_high), salt_low = round(salt_low),
  d_salt = round(d_salt), d_prop = round(d_prop), d_nonprop = round(d_nonprop), n_high, n_low)]), row.names = FALSE)
cat("\nDifference in discontinuities (h =", H, "mi) of the within-tract change since 2017, by pair group. Cells: estimate [robust 95% CI]\n")
for (g in grps) { cat("\n==", g, "\n"); print(dcast(res[group == g, .(var, y, cell = sprintf("%.3f [%.3f, %.3f]", est, lo, hi))], y ~ var, value.var = "cell")) }
sink()
res[, label := factor(label, v)]; res[, group := factor(group, grps)]
g <- ggplot(res, aes(y, est, colour = group, fill = group)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey40") +
  geom_ribbon(aes(ymin = lo, ymax = hi), alpha = .12, colour = NA) + geom_line() + geom_point(size = 1) +
  facet_wrap(~ label, scales = "free_y", ncol = 4) + scale_x_continuous(breaks = seq(2013, 2024, 2)) +
  scale_colour_manual(values = c("#d7301f", "#2a64a8", "#f4a582")) + scale_fill_manual(values = c("#d7301f", "#2a64a8", "#f4a582")) +
  labs(x = "year", y = "change in the border discontinuity since 2017 (high-rate minus low-rate side)", colour = NULL, fill = NULL,
       title = "Difference in discontinuities by whether the high-rate side is also the high-SALT-exposure side",
       subtitle = sprintf("rdrobust, h = %d mi, pair-demeaned, robust 95%% CI, SE by county; %d agree pairs, %d disagree pairs", H, sum(pw$d_salt > 0), sum(pw$d_salt <= 0))) +
  theme_minimal(base_size = 9) + theme(legend.position = "bottom")
ggsave(file.path(OUT, "figures/c21_diff_in_disc_by_exposure_side.png"), g, width = 13, height = 7.5, dpi = 150)
