# District exposure from a direct ZIP -> school-district merge, and the exposure x year event study under different fixed
# effects. Exposure = the lost federal subsidy per return 2016-18 (build/exposure/05: change in SALT deducted x assumed bracket
# marginal rate, net of AMT, SOI ZIP x AGI stub; d_salt per return, d_salt_own per owner household; ZIPs with >= 100 returns), carried to
# districts through the ZCTA x district area crosswalk (build/geo/08): district exposure = sum_z sh_zcta(z,d) x returns_z x
# ex_z / sum_z sh_zcta(z,d) x returns_z, i.e. return-weighted over the ZIPs' area shares. Compared with the tract-based district
# exposure of 19 (population-weighted mean over the district's tracts, each at its modal sales ZIP).
# Event study: y_it = sum_t g_t exposure_i x 1[t] + district FE + {year | state x year | CBSA x year | county x year} FE,
# 2017 = 0, enrolment weights, SE by county; all F-33 districts FY2012-23; outcomes per pupil, logged. CBSA = the F-33 CBSA
# code (districts outside a CBSA form one group per state); county = CONUM.
suppressPackageStartupMessages({library(data.table); library(DBI); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); REF <- 2017
lg <- function(x) ifelse(is.finite(x) & x > 0, log(x), NA_real_)
# ---- ZIP -> district exposure
cz <- fread(file.path(DATA, "built/exposure/exposure_components_zip.csv"), colClasses = c(geo = "character"))[, .(zcta = geo, returns16, d_salt, d_salt_own, d_prop_own, clean)]
xw <- fread(file.path(DATA, "built/geo/zcta_district_area.csv"), colClasses = c(zcta = "character", leaid = "character"))
ok <- cz[returns16 >= 100 & is.finite(d_salt) & is.finite(d_salt_own) & is.finite(d_prop_own)]   # all ZIPs with >= 100 returns (clean = uncensored stubs, a subset)
m <- merge(xw, ok, by = "zcta")[, w := sh_zcta * returns16]
dz <- m[, .(ex_zip = weighted.mean(d_salt, w), ex_zip_own = weighted.mean(d_salt_own, w), ex_zip_prop = weighted.mean(d_prop_own, w), n_zip = .N, returns = sum(w), sh_main = max(w) / sum(w)), by = leaid][is.finite(ex_zip)]
cat("SOI ZIPs 2016:", nrow(cz), "| with >= 100 returns and finite measures:", nrow(ok), "| of which clean (no censored stub):", ok[clean == TRUE, .N], "| ZCTAs in the crosswalk:", uniqueN(xw$zcta), "| districts in the crosswalk:", uniqueN(xw$leaid), "| districts with a ZIP exposure:", nrow(dz), "\n")
cat("ZIPs per district (clean, area share >= 1%): mean", round(mean(dz$n_zip), 2), "median", median(dz$n_zip), "| share of the district's returns from its largest ZIP: median", round(median(dz$sh_main), 2), "\n")
print(dz[, .(districts = .N, mean_ex_per_return = round(mean(ex_zip)), mean_ex_per_owner = round(mean(ex_zip_own)), sd_per_owner = round(sd(ex_zip_own)))])
# tract-based version (19)
tz <- fread(file.path(DATA, "built/exposure/salt_exposure_tract_zip2016.csv"), colClasses = c(tract = "character", zip = "character"))[, .(tract, zip)]
tr <- merge(readRDS(file.path(DATA, "built/geo/iv_tract_side.rds"))[T == 30 & !is.na(leaid), .(tract, leaid, pop)], merge(tz, ok[, .(zip = zcta, d_salt_own)], by = "zip")[, .(tract, d_salt_own)], by = "tract")
dt <- tr[, .(ex_tract_own = weighted.mean(d_salt_own, pop, na.rm = TRUE)), by = leaid][is.finite(ex_tract_own)]
cmp <- merge(dz, dt, by = "leaid")
cat("districts with both:", nrow(cmp), "| corr(ZIP-area, tract-based) per owner HH:", round(cor(cmp$ex_zip_own, cmp$ex_tract_own), 3), "| rank corr:", round(cor(cmp$ex_zip_own, cmp$ex_tract_own, method = "spearman"), 3), "\n")
fwrite(dz, file.path(DATA, "built/exposure/district_exposure_zip.csv"))
# ---- panel
f <- readRDS(file.path(DATA, "built/census_finances/district_finance.rds"))[enr > 0]
fv <- c(ptax = "property tax per pupil", loc = "local revenue per pupil", strev = "state revenue per pupil", spend = "current spending per pupil", inst = "instruction per pupil",
        salaries = "salaries & wages per pupil", benefits = "employee benefits per pupil", capout = "capital outlay per pupil", debt_end = "LT debt outstanding per pupil", lenr = "enrolment")
f[, lenr := enr]; for (k in names(fv)) f[, (k) := lg(get(k) / ifelse(k == "lenr", 1, enr))]
f <- merge(f[, c("leaid", "y", "conum", "cbsa", "enr", names(fv)), with = FALSE], dz[, .(leaid, ex_raw = ex_zip_own)], by = "leaid")
f[, `:=`(st = substr(leaid, 1, 2), cty = conum, msa = ifelse(cbsa %in% c("N", "") | is.na(cbsa), paste0("none_", substr(leaid, 1, 2)), cbsa))]
f[, ex := (ex_raw - weighted.mean(ex_raw, enr)) / sqrt(weighted.mean((ex_raw - weighted.mean(ex_raw, enr))^2, enr))]
cat("districts in the panel:", uniqueN(f$leaid), "| CBSAs:", uniqueN(f$msa), "| counties:", uniqueN(f$cty), "| singleton-county districts (one district in the county):", f[y == 2016, .N, by = cty][N == 1, .N], "\n")
fes <- c(`year` = "leaid + y", `state x year` = "leaid + st^y", `CBSA x year` = "leaid + msa^y", `county x year` = "leaid + cty^y")
pp <- function(r) r[, .(post_min_pre = mean(b[y > REF]) - mean(b[y >= 2013 & y < REF]), se = sqrt(mean(se[y > REF]^2) + mean(se[y >= 2013 & y < REF]^2) / sum(y >= 2013 & y < REF)))]
res <- list(); ser <- list()
for (k in names(fv)) { x <- f[is.finite(get(k))][, yv := get(k)]; sdy <- sd(x[y == 2016, yv], na.rm = TRUE)
  for (fe in names(fes)) { m <- feols(as.formula(sprintf("yv ~ i(y, ex, ref = %d) | %s", REF, fes[fe])), x, cluster = ~cty, weights = ~enr)
    r <- as.data.table(coeftable(m), keep.rownames = "term")[grepl("^y::\\d+:ex$", term), .(y = as.integer(sub("y::(\\d+):.*", "\\1", term)), b = Estimate, se = `Std. Error`)]
    r <- rbind(r, data.table(y = REF, b = 0, se = 0))[, `:=`(b = b / sdy, se = se / sdy, outcome = fv[k], fe = fe, n = nobs(m))]
    ser[[length(ser) + 1]] <- r; res[[length(res) + 1]] <- pp(r)[, `:=`(outcome = fv[k], fe = fe, n = nobs(m), pre_2012 = r[y == 2012, b])] } }
res <- rbindlist(res)[, t := post_min_pre / se]; ser <- rbindlist(ser)
fwrite(res, file.path(OUT, "c33_district_exposure_fe_variants.csv")); fwrite(ser, file.path(OUT, "c33_district_exposure_fe_variants_series.csv"))
sink(file.path(OUT, "c33_district_exposure_fe_variants.txt"), split = TRUE)
cat("District exposure = lost subsidy per owner HH, ZIP -> district by ZCTA area shares x returns; all F-33 districts, enrolment-weighted, SE by county.\n",
    "post - pre (mean 2018+ minus mean 2013-16) in SD units of the outcome per SD of exposure, by fixed effects (district FE always). Cells: estimate (t) [2012 coefficient].\n\n")
w <- dcast(res[, .(outcome, fe = factor(fe, names(fes)), cell = sprintf("%.3f (%.1f) [%.3f]", post_min_pre, t, pre_2012))], outcome ~ fe, value.var = "cell")
print(as.data.frame(w[match(fv, outcome)]), row.names = FALSE)
cat("\nobservations by FE set (property tax):"); print(res[outcome == fv["ptax"], .(fe, n)])
sink()
pl <- copy(ser)[, `:=`(outcome = factor(outcome, fv), fe = factor(fe, names(fes)))]
g <- ggplot(pl, aes(y, b, colour = fe)) + geom_hline(yintercept = 0, colour = "grey70") + geom_vline(xintercept = REF + .5, linetype = "22", colour = "grey50") +
  geom_pointrange(aes(ymin = b - 1.96 * se, ymax = b + 1.96 * se), position = position_dodge(width = .6), size = .18, linewidth = .3) + geom_line(position = position_dodge(width = .6), linewidth = .3) +
  facet_wrap(~ outcome, scales = "free_y", ncol = 5, labeller = label_wrap_gen(26)) + scale_x_continuous(breaks = seq(2012, 2024, 4)) +
  scale_colour_manual("fixed effects (with district FE)", values = c("#555555", "#2a64a8", "#2e8b57", "#c0392b")) +
  labs(x = NULL, y = "exposure x year, SD units of the outcome per SD of exposure, 2017 = 0; 95% CI, SE by county",
       title = "Exposure event studies on all school districts, by fixed effects; exposure from a direct ZIP -> district merge",
       subtitle = sprintf("%d districts; exposure = lost federal subsidy per owner household 2016-18, ZCTA x district area shares weighted by 2016 returns; enrolment weights", uniqueN(f$leaid))) +
  theme_minimal(base_size = 8) + theme(legend.position = "bottom", strip.text = element_text(size = 6.5))
ggsave(file.path(OUT, "figures/c33_district_exposure_fe_variants.png"), g, width = 12, height = 6, dpi = 150)
