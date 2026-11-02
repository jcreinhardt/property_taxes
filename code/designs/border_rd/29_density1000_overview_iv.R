# The density-selected sample (tracts below 1,000 per sq mi within 50 mi of a ranked border, T = 30, both sides; 23-24 found
# distance balanced here once log density is controlled): three things.
#  A. near vs inland over time: y_it = sum_t b_t Z_i 1[t] + sum_t l_t log density_i 1[t] + unit FE + pair x year FE (pair x side
#     x year when both sides are pooled), 2017 = 0, SE by county; Z = -distance / 10 mi or near < 10 mi; with and without the
#     density term. Core outcomes (tract CoreLogic / ACS / Data Axle, F-33 district, CoG county), in SD units of 2016.
#  B. tax competition instrumented by distance: competition_i = SD of the 2016 effective rate across sample tracts (both
#     sides) within 30 mi of i (>= 5 neighbours), standardised; 2SLS of y_it on sum_t g_t competition_i 1[t] with Z_i 1[t] as
#     instruments, same FE and density terms. Cross-sectional first stage reported.
#  C. balance of Z (raw and given density) and of competition on the pre-treatment covariates of 23, by side.
suppressPackageStartupMessages({library(data.table); library(DBI); library(fixest); library(ggplot2); library(sf)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); MX <- 50; REF <- 2017; DCUT <- as.numeric(Sys.getenv("DENS_CUT", 1000)); RAD <- 30
source("code/designs/border_rd/nb_panels.R")
tz <- fread(file.path(DATA, "built/exposure/salt_exposure_tract_zip2016.csv"), colClasses = c(tract = "character", zip = "character"))[, .(tract, zip)]
cz <- fread(file.path(DATA, "built/exposure/exposure_components_zip.csv"), colClasses = c(geo = "character"))[returns16 >= 100 & is.finite(d_salt_own), .(zip = geo, d_salt_own)]
d <- rbindlist(lapply(c("low", "high"), \(s) readRDS(file.path(DATA, sprintf("built/geo/%s_side_balance_tracts.rds", s)))[dist_mi < MX & is.finite(ldens)][, side := s]), fill = TRUE)[exp(ldens) < DCUT & is.finite(med)]
d <- merge(d, merge(tz, cz, by = "zip")[, .(tract, d_salt_own)], by = "tract", all.x = TRUE)
d[, `:=`(z = -dist_mi / 10, near10 = as.integer(dist_mi < 10), ps = paste(pair, side))]
# competition: SD of the 2016 effective rate over sample tracts within RAD mi (both sides), excluding the tract itself
pts <- st_transform(st_as_sf(d[, .(tract, lon, lat)], coords = c("lon", "lat"), crs = 4326), 5070); nb <- st_is_within_distance(pts, dist = RAD * 1609.34)
d[, `:=`(n_nb30 = sapply(seq_len(.N), \(i) length(nb[[i]]) - 1), comp_raw = sapply(seq_len(.N), \(i) { k <- setdiff(nb[[i]], i); if (length(k) < 5) NA_real_ else sd(med[k]) }))]
d[, comp := (comp_raw - mean(comp_raw, na.rm = TRUE)) / sd(comp_raw, na.rm = TRUE)]
cat(sprintf("sample: %d tracts (%d low, %d high), %d pairs; competition defined for %d (median SD of rates within %d mi: %.2f pp, IQR %.2f-%.2f)\n", nrow(d), sum(d$side == "low"), sum(d$side == "high"), uniqueN(d$pair), sum(is.finite(d$comp)), RAD, median(d$comp_raw, na.rm = TRUE), quantile(d$comp_raw, .25, na.rm = TRUE), quantile(d$comp_raw, .75, na.rm = TRUE)))
fwrite(d[, .(tract, side, pair, dist_mi, med, ldens, n_nb30, comp_raw, comp)], file.path(OUT, "c41_sample_competition.csv"))
samples <- list(`low-tax side` = d[side == "low"], `high-tax side` = d[side == "high"], `both sides pooled` = copy(d)[, pair := ps])
vars <- list(cl = c(lp50 = "log median sale price", lbill = "log tax bill per parcel", eff50 = "effective tax rate", turn = "sales per 100 parcels"),
             ac = c(lhval = "log median home value (ACS)", lpop = "log population (ACS)"), mv = c(moveout = "move-out rate (Data Axle)"),
             f = c(lptax = "log district property tax per pupil", lspend = "log district spending per pupil", lenr = "log district enrolment"), sd = c(lptax = "log county school property tax (CoG)"))
units <- c(cl = "tract", ac = "tract", mv = "tract", f = "leaid", sd = "cty"); Zs <- c(z = "per 10 mi closer", near10 = "near < 10 mi")
pp <- function(r) r[, .(post_min_pre = mean(b_sd[y > REF]) - mean(b_sd[y < REF & y >= 2012]), se = sqrt(mean(se_sd[y > REF]^2) + mean(se_sd[y < REF & y >= 2012]^2) / sum(y < REF & y >= 2012)))]   # SD units of the outcome; pre = 2012-16 (as 31, 32)
iv <- function(p, zz, fe_unit, dens) { x <- p[is.finite(yv) & is.finite(comp)]
  m <- feols(as.formula(sprintf("yv ~ %s | %s + pair^y | i(y, comp, ref = %d) ~ i(y, %s, ref = %d)", ifelse(dens, sprintf("i(y, ldens, ref = %d)", REF), "1"), fe_unit, REF, zz, REF)), x, cluster = ~cty)
  ct <- as.data.table(coeftable(m), keep.rownames = "term")[grepl("^fit_y::\\d+:comp$", term)]
  rbind(ct[, .(y = as.integer(sub("fit_y::(\\d+):.*", "\\1", term)), b = Estimate, se = `Std. Error`)], data.table(y = REF, b = 0, se = 0))[, n := nobs(m)] }
ser <- list(); res <- list(); fs <- list()
for (s in names(samples)) { P <- nb_panels(samples[[s]], c("z", "near10", "comp", "ldens"))
  for (zz in names(Zs)) { f1 <- feols(as.formula(sprintf("comp ~ %s + ldens | pair", zz)), samples[[s]][is.finite(comp)], cluster = ~cty); f0 <- feols(as.formula(sprintf("comp ~ %s | pair", zz)), samples[[s]][is.finite(comp)], cluster = ~cty)
    fs[[length(fs) + 1]] <- data.table(sample = s, Z = Zs[zz], b_dens = coef(f1)[1], t_dens = tstat(f1)[1], b_raw = coef(f0)[1], t_raw = tstat(f0)[1], n = nobs(f1)) }
  for (tb in names(vars)) for (k in names(vars[[tb]])) { p <- P[[tb]][, yv := get(k)]; sdy <- sd(p[y == 2016, yv], na.rm = TRUE)
    for (zz in names(Zs)) for (dens in c(TRUE, FALSE)) { ex <- ifelse(dens, sprintf(" + i(y, ldens, ref = %d)", REF), "")
      a <- es(p, zz, units[tb], "cty", REF, ex); if (!is.null(a)) { a[, `:=`(design = "near x year", sample = s, Z = Zs[zz], density = dens, outcome = vars[[tb]][k], b_sd = b / sdy, se_sd = se / sdy)]; ser[[length(ser) + 1]] <- a }
      b <- iv(p, zz, units[tb], dens); b[, `:=`(design = "IV: competition x year", sample = s, Z = Zs[zz], density = dens, outcome = vars[[tb]][k], b_sd = b / sdy, se_sd = se / sdy)]; ser[[length(ser) + 1]] <- b } } }
ser <- rbindlist(ser, fill = TRUE)[, z := NULL]; res <- ser[, pp(.SD), by = .(design, sample, Z, density, outcome)][, t := post_min_pre / se]; fs <- rbindlist(fs)
fwrite(ser, file.path(OUT, "c41_density1000_series.csv")); fwrite(res, file.path(OUT, "c41_density1000_post_pre.csv")); fwrite(fs, file.path(OUT, "c41_first_stage.csv"))
# ---- C. balance
v <- c(med = "effective tax rate, % (CoreLogic)", comp_raw = "tax competition: SD of rates within 30 mi, pp",
       lp50 = "log median sale price 2016 (CoreLogic)", lhval = "log median home value (ACS)", moveout = "move-out rate 2014-17 (Data Axle)", sh_moved = "moved in the last year (ACS)", turnover = "sales per 100 parcels 2016 (CoreLogic)",
       d_salt_own = "own lost SALT subsidy per owner HH (SOI ZIP)", linc = "log median HH income (ACS)", sh_agi100 = "share of returns AGI >= $100k (SOI ZIP)", sh_item = "itemiser share (SOI ZIP)", age = "median age (ACS)",
       hhsize = "household size (ACS)", sh_u18 = "share under 18 (ACS)", sh_hh_kids = "share of HHs with children (ACS)", sh_own = "owner share (ACS)", sh_ba = "share BA+ (ACS)", sh_white = "share white (ACS)",
       yrbuilt = "median year built (ACS)", lpop = "log population (ACS)", ldens = "log population density (ACS, TIGER)", ach = "district test scores (SEDA)", ses = "district SES (SEDA)")
grp <- c(med = "first stage", comp_raw = "first stage", lp50 = "outcomes", lhval = "outcomes", moveout = "outcomes", sh_moved = "outcomes", turnover = "outcomes", d_salt_own = "exposure")
grp <- c(grp, setNames(rep("controls", length(v) - length(grp)), setdiff(names(v), names(grp))))
bal <- list()
for (s in names(samples)[1:2]) { x0 <- samples[[s]]; sds <- sapply(names(v), \(k) sd(x0[[k]], na.rm = TRUE))
  for (zz in names(Zs)) for (sp in c("raw", "dens")) for (k in names(v)) { if (k == "ldens" && sp == "dens") next; x <- x0[is.finite(get(k))][, yb := get(k) / sds[k]]
    m <- feols(as.formula(sprintf("yb ~ %s%s | pair", zz, ifelse(sp == "dens", " + ldens", ""))), x, cluster = ~cty); ct <- coeftable(m)
    bal[[length(bal) + 1]] <- data.table(var = k, sample = s, Z = Zs[zz], spec = sp, b = ct[1, 1], se = ct[1, 2], p = ct[1, 4], n = nobs(m)) } }
bal <- rbindlist(bal); bal[grp[var] == "controls", p_holm := p.adjust(p, "holm"), by = .(sample, Z, spec)]; fwrite(bal, file.path(OUT, "c41_balance.csv"))
# ---- text
sink(file.path(OUT, "c41_density1000_overview_iv.txt"), split = TRUE)
cat(sprintf("Sample: tracts below %s per sq mi within %d mi of a ranked border; %d tracts (%d low, %d high), %d pairs. Unit FE + pair x year FE (pair x side x year pooled), SE by county.\n", format(DCUT, big.mark = ","), MX, nrow(d), sum(d$side == "low"), sum(d$side == "high"), uniqueN(d$pair)))
cat("\nFirst stage (cross-section): competition (SD units) on Z, pair FE, with / without log density: b (t)\n"); print(as.data.frame(fs[, .(sample, Z, dens = sprintf("%.3f (%.1f)", b_dens, t_dens), raw = sprintf("%.3f (%.1f)", b_raw, t_raw), n)]), row.names = FALSE)
for (dd in c("near x year", "IV: competition x year")) for (dens in c(TRUE, FALSE)) { cat("\n==", dd, ifelse(dens, "| with log density x year", "| without density"), "\n  post - pre (mean 2018+ minus 2012-16), SD units of the outcome per 10 mi closer / near / per SD of competition; cells: estimate (t)\n")
  w <- dcast(res[design == dd & density == dens, .(outcome, col = paste(sample, "|", Z), cell = sprintf("%.3f (%.1f)", post_min_pre, t))], outcome ~ col, value.var = "cell"); print(as.data.frame(w[match(unlist(vars), outcome)]), row.names = FALSE) }
cat("\n== balance: controls at p < .05 / Holm (of 15 raw, 14 given density), median |b|\n"); print(as.data.frame(bal[grp[var] == "controls", .(p05 = sum(p < .05), holm05 = sum(p_holm < .05), median_abs = round(median(abs(b)), 3), max_abs = round(max(abs(b)), 2)), by = .(sample, Z, spec)]), row.names = FALSE)
sink()
# ---- figures
ser[, `:=`(outcome = factor(outcome, unlist(vars)), sample = factor(sample, names(samples)), dl = factor(ifelse(density, "with log density x year", "without"), c("with log density x year", "without")))]
for (dd in c("near x year", "IV: competition x year")) for (zz in names(Zs)) { pl <- ser[design == dd & Z == Zs[zz]]
  g <- ggplot(pl, aes(y, b_sd, colour = sample, alpha = dl, linetype = dl)) + geom_hline(yintercept = 0, colour = "grey70") + geom_vline(xintercept = REF + .5, linetype = "22", colour = "grey50") +
    geom_pointrange(aes(ymin = b_sd - 1.96 * se_sd, ymax = b_sd + 1.96 * se_sd), position = position_dodge(width = .7), size = .18, linewidth = .3) + geom_line(position = position_dodge(width = .7), linewidth = .35) +
    facet_wrap(~ outcome, scales = "free_y", ncol = 4, labeller = label_wrap_gen(30)) + scale_x_continuous(breaks = seq(2012, 2024, 2)) + scale_colour_manual(NULL, values = c("#2a64a8", "#c0392b", "grey30")) +
    scale_alpha_manual(NULL, values = c(1, .35)) + scale_linetype_manual(NULL, values = c("solid", "dotted")) +
    labs(x = NULL, y = sprintf("%s, SD units of the outcome (2016), 2017 = 0; 95%% CI, SE by county", ifelse(dd == "near x year", paste("Z x year,", Zs[zz]), "competition x year (per SD of the within-30-mi SD of rates), instrumented by Z x year")),
         title = sprintf("%s: tracts below %s per sq mi within 50 mi of a ranked border | Z = %s", ifelse(dd == "near x year", "Near the border vs inland over time", "Tax competition instrumented by distance"), format(DCUT, big.mark = ","), Zs[zz]),
         subtitle = "unit FE + pair x year FE (pair x side x year pooled); solid = log density x year included, dotted = not") + theme_minimal(base_size = 8) + theme(legend.position = "bottom", strip.text = element_text(size = 6.5))
  ggsave(file.path(OUT, sprintf("figures/c41_%s_%s.png", ifelse(dd == "near x year", "near_es", "iv_es"), zz)), g, width = 11, height = 8, dpi = 150) }
bal[, `:=`(label = factor(v[var], rev(v)), group = factor(grp[var], c("first stage", "outcomes", "exposure", "controls")), spec = factor(spec, c("raw", "dens"), c("pair FE only", "pair FE + log density")), sample = factor(sample, names(samples)[1:2]))]
g <- ggplot(bal, aes(b, label, colour = spec, shape = Z)) + geom_vline(xintercept = 0, colour = "grey50") + geom_pointrange(aes(xmin = b - 1.96 * se, xmax = b + 1.96 * se), position = position_dodge(width = .7), size = .2, linewidth = .35) +
  facet_grid(group ~ sample, scales = "free", space = "free_y") + scale_colour_manual(NULL, values = c("grey55", "#2a64a8")) + scale_shape_manual(NULL, values = c(16, 17)) +
  labs(x = "coefficient on Z, SD units of the covariate; 95% CI, SE by county", y = NULL, title = sprintf("Balance of distance to the border: tracts below %s per sq mi within 50 mi, by side", format(DCUT, big.mark = ",")),
       subtitle = sprintf("%d low-side and %d high-side tracts; first-stage rows: the 2016 rate and the competition measure (SD of rates within 30 mi)", sum(d$side == "low"), sum(d$side == "high"))) + theme_minimal(base_size = 8) + theme(legend.position = "bottom", strip.text.y = element_text(angle = 0))
ggsave(file.path(OUT, "figures/c41_balance.png"), g, width = 10, height = 8.5, dpi = 150)
