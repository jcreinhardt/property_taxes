# Household composition and age as outcomes, two sources and two designs.
# Sources: SOI ZIP (build/soi/12: individuals and dependents per return, filing-status shares, elderly share 2015-18, log returns
# and individuals; dependents end in 2018, elderly only 2015-18, "individuals" switches from exemptions to persons in 2018) and
# ACS 5-year tracts (build/acs/08: shares under 18, 65+, householders living alone, households with own children, household
# size; plus median age from the main panel). Outcomes in SD units of their 2016 cross-section.
# Design 1, exposure DiD on all units: y_it = sum_t g_t exposure_i 1[t] + unit FE + {state, county} x year FE, exposure = own
#   lost subsidy per owner HH (ZIP; tracts via modal sales ZIP), standardised; weights = 2016 returns (tracts: ZIP returns x
#   population share); SE by county (ZIP -> county: HUD modal residential county).
# Design 2, the density-selected border sample of 29 (tracts < 1,000 per sq mi within 50 mi, both sides): Z x year with
#   log density x year (Z = -distance / 10 mi) and the Z x exposure x year interaction, unit FE + pair x year FE, by side and
#   pooled; ZIP-level outcomes on ZIPs with >= 50% of their population in sample tracts (population-weighted Z, density).
suppressPackageStartupMessages({library(data.table); library(DBI); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); REF <- 2017
# ---- outcome panels
soi <- fread(file.path(DATA, "built/soi/zip_composition.csv"), colClasses = c(zip = "character"))[y >= 2012]
sv <- c(ind_per_ret = "individuals per return (SOI)", dep_per_ret = "dependents per return (SOI, to 2018)", sh_joint = "joint filers, share (SOI)", sh_hoh = "head-of-household filers, share (SOI)",
        sh_single = "single filers, share (SOI)", sh_elderly = "elderly (60+) returns, share (SOI, 2015-18)", lret = "log returns (SOI)", lind = "log individuals (SOI)")
acf <- file.path(DATA, "built/acs/acs_tract_composition_year.rds"); HAVE_ACS <- file.exists(acf)
if (HAVE_ACS) { ac <- merge(readRDS(acf), readRDS(file.path(DATA, "built/acs/acs_tract_year.rds"))[, .(tract, y, age)], by = c("tract", "y"), all.x = TRUE) } else cat("ACS composition panel not built yet: tract outcomes skipped\n")
av <- c(sh_u18 = "share under 18 (ACS)", sh_65 = "share 65+ (ACS)", age = "median age (ACS)", sh_hh_kids = "households with children, share (ACS)", sh_alone = "one-person households, share (ACS)", hhsize = "household size (ACS)")
# ---- exposure, geography
tz <- fread(file.path(DATA, "built/exposure/salt_exposure_tract_zip2016.csv"), colClasses = c(tract = "character", zip = "character"))[, .(tract, zip)]
cz <- fread(file.path(DATA, "built/exposure/exposure_components_zip.csv"), colClasses = c(geo = "character"))[returns16 >= 100 & is.finite(d_salt_own), .(zip = geo, d_salt_own, returns16, st)]
con <- db_open("database.db"); hud <- as.data.table(dbGetQuery(con, "SELECT lpad(zipcode::VARCHAR, 5, '0') zip, lpad(county::VARCHAR, 5, '0') cty, res_ratio FROM hud_zip_to_county WHERE year = 2016")); dbDisconnect(con, shutdown = TRUE)
hud <- hud[order(zip, -res_ratio)][!duplicated(zip), .(zip, cty)]
tp <- fread(file.path(DATA, "external/acs/acs5_tract_2017.csv"), colClasses = c(GEOID = "character"))[, .(tract = GEOID, pop = popE)]
zipx <- merge(cz, hud, by = "zip")[, ex := (d_salt_own - mean(d_salt_own)) / sd(d_salt_own)][, `:=`(st = substr(cty, 1, 2), w = returns16)]
trx <- merge(merge(tz, zipx[, .(zip, ex, returns16)], by = "zip"), tp, by = "tract")[is.finite(pop)][, w := as.numeric(returns16) * pop / sum(pop), by = zip][, `:=`(cty = NA_character_)]
trx[, `:=`(cty = substr(tract, 1, 5), st = substr(tract, 1, 2))]
pp <- function(r) { pre <- r[y < REF & y >= 2012]; post <- r[y > REF]; if (!nrow(pre) || !nrow(post)) return(NULL); data.table(post_min_pre = mean(post$b) - mean(pre$b), se = sqrt(mean(post$se^2) + mean(pre$se^2) / nrow(pre))) }
getc <- function(m, pat, nm) { ct <- as.data.table(coeftable(m), keep.rownames = "term")[grepl(pat, term)]; rbind(ct[, .(y = as.integer(sub(".*y::(\\d+):.*", "\\1", term)), b = Estimate, se = `Std. Error`)], data.table(y = REF, b = 0, se = 0))[, `:=`(coef = nm, n = nobs(m))] }
res <- list(); ser <- list()
# ---- design 1: exposure DiD
d1 <- function(p, vars, unit, src, wts = TRUE) { for (k in names(vars)) { x <- p[is.finite(get(k))][, yv := get(k)]; if (uniqueN(x$y) < 4) next; sdy <- sd(x[y == 2016, yv]); if (!is.finite(sdy) || sdy == 0) next
    for (fe in c("state x year", "county x year")) { f <- sprintf("yv ~ i(y, ex, ref = %d) | %s + %s^y", REF, unit, ifelse(fe == "state x year", "st", "cty"))
      m <- feols(as.formula(f), x, cluster = ~cty, weights = ~w); r <- getc(m, "^y::\\d+:ex$", "exposure x year")[, `:=`(design = paste("exposure DiD,", fe), sample = "all units", src = src, outcome = vars[k], b_sd = b / sdy, se_sd = se / sdy)]
      ser[[length(ser) + 1]] <<- r; q <- pp(r); if (!is.null(q)) res[[length(res) + 1]] <<- q[, `:=`(design = paste("exposure DiD,", fe), sample = "all units", src = src, outcome = vars[k], b_sd = post_min_pre / sdy, se_sd = se / sdy, n = r$n[1])] } } }
d1(merge(soi, zipx, by = "zip"), sv, "zip", "SOI ZIP")
if (HAVE_ACS) d1(merge(ac, trx[, .(tract, ex, w, cty, st)], by = "tract"), av, "tract", "ACS tract")
# ---- design 2: border sample
bd <- fread(file.path(OUT, "c41_sample_competition.csv"), colClasses = c(tract = "character", pair = "character"))
bd <- merge(bd, rbindlist(lapply(c("low", "high"), \(s) readRDS(file.path(DATA, sprintf("built/geo/%s_side_balance_tracts.rds", s)))[, .(tract, cty, pop)])), by = "tract")
bd <- merge(bd, merge(tz, cz[, .(zip, d_salt_own)], by = "zip"), by = "tract", all.x = TRUE)[, `:=`(z = -dist_mi / 10, ps = paste(pair, side))]
bd[, ex := (d_salt_own - mean(d_salt_own, na.rm = TRUE)) / sd(d_salt_own, na.rm = TRUE)][, zx := z * ex]
# ZIP-level instruments: population-weighted over sample tracts, ZIPs with >= 50% of population in the sample
zp <- merge(tz, tp, by = "tract")[is.finite(pop), .(pop_all = sum(pop)), by = zip]
bz <- bd[is.finite(pop) & !is.na(zip), .(pop_in = sum(pop), z = weighted.mean(z, pop), ldens = weighted.mean(ldens, pop), ex = ex[1], zx = weighted.mean(z, pop) * ex[1], pair = names(which.max(table(pair))), ps = names(which.max(table(ps))), side = names(which.max(table(side))), cty = names(which.max(table(cty)))), by = zip]
bz <- merge(bz, zp, by = "zip")[pop_in / pop_all >= .5]; cat("border ZIPs:", nrow(bz), "(low", sum(bz$side == "low"), "high", sum(bz$side == "high"), ")\n")
d2 <- function(p, vars, unit, src) { for (s in c("low-tax side", "high-tax side", "both sides pooled")) { x0 <- if (s == "low-tax side") p[side == "low"] else if (s == "high-tax side") p[side == "high"] else copy(p)[, pair := ps]
    for (k in names(vars)) { x <- x0[is.finite(get(k))][, yv := get(k)]; if (uniqueN(x$y) < 4) next; sdy <- sd(x[y == 2016, yv]); if (!is.finite(sdy) || sdy == 0) next
      m1 <- feols(as.formula(sprintf("yv ~ i(y, z, ref = %d) + i(y, ldens, ref = %d) | %s + pair^y", REF, REF, unit)), x, cluster = ~cty)
      r1 <- getc(m1, "^y::\\d+:z$", "Z x year")[, `:=`(design = "border: Z x year", sample = s, src = src, outcome = vars[k], b_sd = b / sdy, se_sd = se / sdy)]
      xx <- x[is.finite(ex)]; m2 <- feols(as.formula(sprintf("yv ~ i(y, z, ref = %d) + i(y, zx, ref = %d) + i(y, ex, ref = %d) + i(y, ldens, ref = %d) | %s + pair^y", REF, REF, REF, REF, unit)), xx, cluster = ~cty)
      r2 <- getc(m2, "^y::\\d+:zx$", "Z x exposure x year")[, `:=`(design = "border: Z x exposure x year", sample = s, src = src, outcome = vars[k], b_sd = b / sdy, se_sd = se / sdy)]
      for (r in list(r1, r2)) { ser[[length(ser) + 1]] <<- r; q <- pp(r); if (!is.null(q)) res[[length(res) + 1]] <<- q[, `:=`(design = r$design[1], sample = s, src = src, outcome = vars[k], b_sd = post_min_pre / sdy, se_sd = se / sdy, n = r$n[1])] } } } }
d2(merge(soi, bz, by = "zip"), sv, "zip", "SOI ZIP")
if (HAVE_ACS) d2(merge(ac, bd[, .(tract, side, pair, ps, cty, z, zx, ex, ldens)], by = "tract"), av, "tract", "ACS tract")
res <- rbindlist(res)[, t := b_sd / se_sd]; ser <- rbindlist(ser)
fwrite(res, file.path(OUT, "c44_composition_post_pre.csv")); fwrite(ser, file.path(OUT, "c44_composition_series.csv"))
sink(file.path(OUT, "c44_composition_outcomes.txt"), split = TRUE)
cat("Household composition outcomes. post - pre (mean 2018+ minus 2012-16) in SD units of the outcome's 2016 cross-section: per SD of exposure (exposure DiD; Z x exposure: per 10 mi closer per SD of exposure) or per 10 mi closer (Z x year). Cells: estimate (t), SE by county.\n")
for (dd in unique(res$design)) { cat("\n==", dd, "\n"); w <- dcast(res[design == dd, .(src, outcome, sample, cell = sprintf("%.3f (%.1f)", b_sd, t))], src + outcome ~ sample, value.var = "cell"); print(as.data.frame(w), row.names = FALSE) }
cat("\nunits: "); print(res[, .(n = n[1]), by = .(design, sample, src)][order(design, src, sample)], nrows = 40)
sink()
# ---- figures: event studies
ser[, `:=`(outcome = factor(outcome, c(av, sv)), src = factor(src, c("ACS tract", "SOI ZIP")))]
plot_es <- function(pl, ttl, fn, colvar) { g <- ggplot(pl, aes(y, b_sd, colour = .data[[colvar]])) + geom_hline(yintercept = 0, colour = "grey70") + geom_vline(xintercept = REF + .5, linetype = "22", colour = "grey50") +
    geom_pointrange(aes(ymin = b_sd - 1.96 * se_sd, ymax = b_sd + 1.96 * se_sd), position = position_dodge(width = .6), size = .18, linewidth = .3) + geom_line(position = position_dodge(width = .6), linewidth = .35) +
    facet_wrap(~ outcome, scales = "free_y", ncol = 4, labeller = label_wrap_gen(32)) + scale_x_continuous(breaks = seq(2012, 2024, 2)) + scale_colour_manual(NULL, values = c("#2a64a8", "#c0392b", "grey30")) +
    labs(x = NULL, y = "SD units of the outcome (2016), 2017 = 0; 95% CI, SE by county", title = ttl) + theme_minimal(base_size = 8) + theme(legend.position = "bottom", strip.text = element_text(size = 6.5))
  ggsave(file.path(OUT, fn), g, width = 11, height = 7.5, dpi = 150) }
plot_es(ser[grepl("exposure DiD", design)][, fe := sub("exposure DiD, ", "", design)], "Exposure DiD on all ZIPs / tracts: composition outcomes, exposure x year (per SD of lost subsidy per owner HH); unit FE + state or county x year FE, 2016-return weights", "figures/c44_composition_exposure_did.png", "fe")
plot_es(ser[design == "border: Z x year"], "Border sample (< 1,000 per sq mi): composition outcomes, Z x year per 10 mi closer; log density x year, unit FE + pair x year FE", "figures/c44_composition_border_near.png", "sample")
plot_es(ser[design == "border: Z x exposure x year"], "Border sample (< 1,000 per sq mi): composition outcomes, Z x exposure x year per 10 mi closer per SD of exposure; exposure x year and log density x year, unit FE + pair x year FE", "figures/c44_composition_border_interaction.png", "sample")
