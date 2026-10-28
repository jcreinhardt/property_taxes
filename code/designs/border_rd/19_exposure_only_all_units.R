# The same district and local-government outcomes as 18, but on ALL districts and counties with the SALT exposure itself as
# the treatment (no border instrument): y_it = sum_t g_t exposure_i x 1[t] + unit FE + state x year FE, 2017 = 0, SE by county,
# as designs/border_iv/03 and designs/exposure_did. Exposure = lost federal subsidy per owner household 2016-18 (build/
# exposure/05): districts = population-weighted mean over their tracts (modal sales ZIP, clean ZIPs; iv_tract_side gives the
# tract -> district link), counties = exposure_components_county.csv; standardised across units. Weights: enrolment (districts),
# 2016 returns (counties). Post - pre = mean(g_t, 2018+) - mean(g_t, 2013-16) (census-year panel: 2022 - 2012), SD units of the
# outcome's 2016 cross-section per SD of exposure. Outcomes: F-33 (5 basic + 39 of 18), SEDA, CoG school districts by county
# (3 basic + 7), CoG all local governments in census years (3 basic + 27). Coverage reported as in 18.
suppressPackageStartupMessages({library(data.table); library(DBI); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); REF <- 2017
lg <- function(x) ifelse(is.finite(x) & x > 0, log(x), NA_real_)
# ---- exposure by district and county
tz <- fread(file.path(DATA, "built/exposure/salt_exposure_tract_zip2016.csv"), colClasses = c(tract = "character", zip = "character"))[, .(tract, zip)]
cz <- fread(file.path(DATA, "built/exposure/exposure_components_zip.csv"), colClasses = c(geo = "character"))[clean == TRUE, .(zip = geo, d_salt_own)]
tr <- merge(readRDS(file.path(DATA, "built/geo/iv_tract_side.rds"))[T == 30 & !is.na(leaid), .(tract, leaid, pop)], merge(tz, cz, by = "zip")[, .(tract, d_salt_own)], by = "tract")
dex <- tr[, .(ex_raw = weighted.mean(d_salt_own, pop, na.rm = TRUE), n_tr = .N), by = leaid][is.finite(ex_raw)]
cex <- fread(file.path(DATA, "built/exposure/exposure_components_county.csv"), colClasses = c(geo = "character"))[, .(cty = geo, ex_raw = d_salt_own, returns16)]
cex <- cex[!substr(cty, 1, 2) %in% c("02", "15", "72") & is.finite(ex_raw)]
dex[, ex := (ex_raw - mean(ex_raw)) / sd(ex_raw)]; cex[, ex := (ex_raw - weighted.mean(ex_raw, returns16)) / sqrt(weighted.mean((ex_raw - weighted.mean(ex_raw, returns16))^2, returns16))]
cat("districts with exposure:", nrow(dex), "| counties:", nrow(cex), "| district exposure mean", round(mean(dex$ex_raw)), "SD", round(sd(dex$ex_raw)), "| county (return-weighted) mean", round(weighted.mean(cex$ex_raw, cex$returns16)), "\n")
# ---- panels
f <- readRDS(file.path(DATA, "built/census_finances/district_finance.rds"))[enr > 0]
fv <- c(ptax = "property tax", loc = "local revenue", strev = "state revenue", spend = "current spending", totrev = "total revenue", fedrev = "federal revenue", title1 = "Title I (C14)",
        idea = "IDEA special ed (C15)", nutrition = "child nutrition (C25)", st_formula = "state formula aid (C01)", st_sped = "state special ed (C05)", st_transp = "state transportation (C06)",
        st_comp = "state compensatory (C07)", loc_parent = "parent govt contributions (T02)", loc_othertax = "other local taxes (T99)", loc_cities = "from cities/counties (D23)",
        tuition = "tuition from pupils (A07)", lunch = "school lunch charges (A09)", activity = "district activity receipts (A13)", fees = "student fees (A15)", private_contrib = "private contributions (U50)",
        totexp = "total expenditure", inst = "instruction", ssvc = "support services", pupil_support = "pupil support (E17)", staff_support = "instr. staff support (E07)",
        gen_admin = "general administration (E08)", sch_admin = "school administration (E09)", om = "operations & maintenance (V40)", transp = "pupil transportation (V45)",
        food = "food services (E11)", capout = "capital outlay", construction = "construction (F12)", equip = "equipment (K09+K10)", pay_private = "payments to private schools (V91)",
        pay_charter = "payments to charter schools (V92)", pay_othersys = "payments to other school systems (Q11)", interest = "interest on debt (I86)", salaries = "salaries & wages (Z32)",
        benefits = "employee benefits (Z33)", inst_salaries = "instruction salaries (V11)", debt_end = "long-term debt outstanding (_41F)", debt_issued = "long-term debt issued (_21F)",
        cash = "cash & securities (W01+W31+W61)")
for (k in names(fv)) f[, (k) := lg(get(k) / enr)]
f[, lenr := log(enr)]; fv <- c(fv, lenr = "enrolment (log)")
f <- merge(f[, c("leaid", "y", "conum", "enr", names(fv)), with = FALSE], dex[, .(leaid, ex)], by = "leaid"); setnames(f, "conum", "cty"); f[, st := substr(leaid, 1, 2)][, w := enr]
se <- merge(readRDS(file.path(DATA, "built/acs/seda_district_year.rds"))[y >= 2012, .(leaid, y, ach, ses)], dex[, .(leaid, ex)], by = "leaid")
se <- merge(se, unique(f[, .(leaid, cty, w = enr)])[!duplicated(leaid)], by = "leaid", all.x = TRUE)[is.na(cty), cty := substr(leaid, 1, 2)][is.na(w), w := 1][, st := substr(leaid, 1, 2)]
sv <- c(ach = "district test scores (SEDA, SD)", ses = "district SES (SEDA)")
con <- db_open("census_finances_test.db")
sd <- as.data.table(dbGetQuery(con, "SELECT lpad(county_fips::VARCHAR, 5, '0') cty, year y, property_tax_rev, total_tax_rev, total_expenditure, federal_ig_rev, state_ig_rev, total_capital_outlay, lt_debt_outstanding, lt_debt_issued, cash_securities, interest_on_debt FROM govt_finances_sd_by_county"))
ar <- as.data.table(dbGetQuery(con, "SELECT lpad(county_fips::VARCHAR, 5, '0') cty, year y, property_tax_rev, total_tax_rev, total_expenditure, general_sales_tax_rev, ind_income_tax_rev, federal_ig_rev, state_ig_rev, other_own_source, enterprise_rev,
  total_capital_outlay, lt_debt_outstanding, lt_debt_issued, cash_securities, interest_on_debt, n_units, n_municipal, n_special_dist,
  amount_e12 e_educ, amount_e24 e_fire, amount_e32 e_health, amount_e36 e_hospitals, amount_e44 e_highways, amount_e50 e_housing, amount_e52 e_libraries, amount_e61 e_parks, amount_e62 e_police,
  amount_e74 + amount_e75 + amount_e77 + amount_e79 e_welfare, amount_e80 e_sewer, amount_e81 e_solidwaste, amount_e89 e_general
  FROM govt_finances_county_area WHERE year IN (2012, 2017, 2022)")); dbDisconnect(con, shutdown = TRUE)
cpop <- fread(file.path(DATA, "external/acs/acs5_tract_2017.csv"), colClasses = c(GEOID = "character"))[, .(cpop = sum(popE, na.rm = TRUE)), by = .(cty = substr(GEOID, 1, 5))]
sdv <- c(property_tax_rev = "school districts: property tax", total_tax_rev = "school districts: total tax", total_expenditure = "school districts: expenditure", federal_ig_rev = "school districts: federal revenue",
         state_ig_rev = "school districts: state revenue", total_capital_outlay = "school districts: capital outlay", lt_debt_outstanding = "school districts: LT debt outstanding",
         lt_debt_issued = "school districts: LT debt issued", cash_securities = "school districts: cash & securities", interest_on_debt = "school districts: interest on debt")
arv <- c(property_tax_rev = "all local govts: property tax", total_tax_rev = "all local govts: total tax", total_expenditure = "all local govts: expenditure",
         general_sales_tax_rev = "all local govts: general sales tax", ind_income_tax_rev = "all local govts: individual income tax", federal_ig_rev = "all local govts: federal revenue",
         state_ig_rev = "all local govts: state revenue", other_own_source = "all local govts: other own-source revenue", enterprise_rev = "all local govts: enterprise revenue",
         total_capital_outlay = "all local govts: capital outlay", lt_debt_outstanding = "all local govts: LT debt outstanding", lt_debt_issued = "all local govts: LT debt issued",
         cash_securities = "all local govts: cash & securities", interest_on_debt = "all local govts: interest on debt",
         e_educ = "spending: education (E12)", e_fire = "spending: fire (E24)", e_health = "spending: health (E32)", e_hospitals = "spending: hospitals (E36)", e_highways = "spending: highways (E44)",
         e_housing = "spending: housing & comm. dev. (E50)", e_libraries = "spending: libraries (E52)", e_parks = "spending: parks (E61)", e_police = "spending: police (E62)",
         e_welfare = "spending: public welfare (E74-79)", e_sewer = "spending: sewerage (E80)", e_solidwaste = "spending: solid waste (E81)", e_general = "spending: general government (E89)",
         n_units = "number of local governments", n_municipal = "number of municipalities", n_special_dist = "number of special districts")
sd <- merge(merge(sd, cpop, by = "cty"), cex[, .(cty, ex, w = returns16)], by = "cty")[, st := substr(cty, 1, 2)]; for (k in names(sdv)) sd[, (k) := lg(get(k) / cpop)]
ar <- merge(merge(ar, cpop, by = "cty"), cex[, .(cty, ex, w = returns16)], by = "cty")[, st := substr(cty, 1, 2)]; for (k in setdiff(names(arv), c("n_units", "n_municipal", "n_special_dist"))) ar[, (k) := lg(get(k) / cpop)]
# ---- estimation
panels <- list(list(f, fv, "leaid"), list(se, sv, "leaid"), list(sd, sdv, "cty"), list(ar, arv, "cty"))
src <- c("F-33 district, log per pupil (enrolment-weighted)", "SEDA district (enrolment-weighted)", "CoG school districts by county, log per capita (return-weighted)", "CoG all local govts, census years, log per capita (return-weighted)")
pp <- function(r) { pre <- if (any(r$y >= 2013 & r$y < REF)) r$y >= 2013 & r$y < REF else r$y < REF
  r[, .(post_min_pre = mean(b[y > REF]) - mean(b[pre]), se = sqrt(mean(se[y > REF]^2) + mean(se[pre]^2) / sum(pre)), n = n[1])] }
cov <- list(); res <- list(); ser <- list()
for (i in seq_along(panels)) { p <- panels[[i]][[1]]; vv <- panels[[i]][[2]]; fe <- panels[[i]][[3]]
  for (k in names(vv)) { lab <- vv[k]; p[, yv := get(k)]; x <- p[is.finite(yv) & is.finite(ex) & is.finite(w) & w > 0]
    byy <- p[, .(sh = mean(is.finite(yv))), by = y][order(y)]
    cov[[length(cov) + 1]] <- data.table(source = src[i], outcome = lab, units = uniqueN(p[[fe]]), unit_years = nrow(p), share_with_value = round(mean(is.finite(p$yv)), 3), share_min = round(min(byy$sh), 2),
      years = paste(range(x$y), collapse = "-"), units_with_value = uniqueN(x[[fe]]))
    if (nrow(x) < 200 || uniqueN(x$y) < 3) next
    sdy <- sd(x[y == 2016, yv], na.rm = TRUE); if (!is.finite(sdy) || sdy == 0) sdy <- sd(x$yv, na.rm = TRUE)
    m <- feols(as.formula(sprintf("yv ~ i(y, ex, ref = %d) | %s + st^y", REF, fe)), x, cluster = ~cty, weights = ~w)
    r <- as.data.table(coeftable(m), keep.rownames = "term")[grepl("^y::\\d+:ex$", term), .(y = as.integer(sub("y::(\\d+):.*", "\\1", term)), b = Estimate, se = `Std. Error`)]
    r <- rbind(r, data.table(y = REF, b = 0, se = 0))[, n := nobs(m)]
    ser[[length(ser) + 1]] <- copy(r)[, `:=`(source = src[i], outcome = lab, b_sd = b / sdy, se_sd = se / sdy)]
    res[[length(res) + 1]] <- pp(r)[, `:=`(source = src[i], outcome = lab, sdy = sdy, units = uniqueN(x[[fe]]))] } }
cov <- rbindlist(cov); res <- rbindlist(res)[, `:=`(b_sd = post_min_pre / sdy, se_sd = se / sdy)][, t := b_sd / se_sd]; ser <- rbindlist(ser)
fwrite(cov, file.path(OUT, "c32_exposure_only_coverage.csv")); fwrite(res, file.path(OUT, "c32_exposure_only.csv")); fwrite(ser, file.path(OUT, "c32_exposure_only_series.csv"))
sink(file.path(OUT, "c32_exposure_only.txt"), split = TRUE)
cat("Exposure x year on all districts / counties (no border instrument): unit FE + state x year FE, SE by county; exposure = lost subsidy per owner HH (SD units across units).\n",
    "post - pre in SD units of the outcome per SD of exposure (census-year panel: 2022 minus 2012). Cells: estimate (t); coverage as in 18.\n")
for (s in src) { cat("\n==", s, "\n"); w <- merge(res[source == s, .(outcome, cell = sprintf("%.3f (%.1f)", b_sd, t), units)], cov[source == s, .(outcome, share_with_value, years)], by = "outcome")
  print(as.data.frame(w[match(intersect(c(fv, sv, sdv, arv), outcome), outcome)]), row.names = FALSE) }
cat("\ncells with |t| > 1.96 by source:\n"); print(res[, .(k = .N, sig = sum(abs(t) > 1.96), share_pos = round(mean(b_sd > 0), 2), median_abs = round(median(abs(b_sd)), 3)), by = source])
sink()
# ---- figures
ord <- c(fv, sv, sdv, arv); res[, `:=`(outcome = factor(outcome, rev(unique(ord))), source = factor(source, src))]
cv <- merge(cov[, -"units"], res[, .(outcome = as.character(outcome), source = as.character(source), units)], by = c("source", "outcome"))[, .(outcome = factor(outcome, levels(res$outcome)), source = factor(source, src), lab = sprintf("%.0f%%, %d units", 100 * share_with_value, as.integer(units)))]
g <- ggplot(res, aes("exposure x year", outcome, fill = pmax(pmin(t, 3), -3))) + geom_tile(colour = "white") + geom_text(aes(label = sprintf("%.2f", b_sd)), size = 2.2) +
  geom_text(data = cv, aes(x = 1.62, y = outcome, label = lab), inherit.aes = FALSE, size = 2, hjust = 0, colour = "grey30") +
  facet_wrap(~ source, scales = "free_y", ncol = 2) + scale_fill_gradient2("t-stat", low = "#c0392b", mid = "white", high = "#2a64a8", limits = c(-3, 3)) + scale_x_discrete(expand = expansion(add = c(.6, 1.6))) +
  labs(x = NULL, y = NULL, title = "Exposure as the treatment, all districts and counties: post-2017 change in the exposure x year coefficient",
       subtitle = "cells: post - pre in SD units of the outcome per SD of lost subsidy per owner household; colour: t (SE by county). Unit FE + state x year FE; enrolment / 2016-return weights. Right: coverage and units") +
  theme_minimal(base_size = 7.5)
ggsave(file.path(OUT, "figures/c32_exposure_only_tiles.png"), g, width = 12, height = 12, dpi = 150)
ser[, `:=`(outcome = factor(outcome, unique(ord)), source = factor(source, src))]
key <- c("property tax", "local revenue", "state revenue", "current spending", "enrolment (log)", "instruction", "salaries & wages (Z32)", "employee benefits (Z33)", "capital outlay", "long-term debt outstanding (_41F)",
         "district test scores (SEDA, SD)", "school districts: property tax", "school districts: state revenue", "school districts: expenditure", "all local govts: property tax", "all local govts: total tax", "all local govts: expenditure", "spending: education (E12)", "spending: police (E62)", "all local govts: state revenue")
pl <- ser[outcome %in% key][, outcome := factor(outcome, key)]
g2 <- ggplot(pl, aes(y, b_sd)) + geom_hline(yintercept = 0, colour = "grey70") + geom_vline(xintercept = REF + .5, linetype = "22", colour = "grey50") +
  geom_ribbon(aes(ymin = b_sd - 1.96 * se_sd, ymax = b_sd + 1.96 * se_sd), fill = "#8c6d31", alpha = .15) + geom_line(colour = "#8c6d31") + geom_point(colour = "#8c6d31", size = .9) +
  facet_wrap(~ outcome, scales = "free_y", ncol = 5, labeller = label_wrap_gen(28)) + scale_x_continuous(breaks = seq(2012, 2024, 4)) +
  labs(x = NULL, y = "exposure x year, SD units of the outcome per SD of exposure, 2017 = 0; unit FE + state x year FE; 95% CI, SE by county",
       title = "Exposure event studies on all districts and counties (lost subsidy per owner household as the treatment)", subtitle = "F-33 districts enrolment-weighted; CoG counties 2016-return-weighted; census-year panel has 2012, 2017, 2022 only") +
  theme_minimal(base_size = 7.5) + theme(strip.text = element_text(size = 6))
ggsave(file.path(OUT, "figures/c32_exposure_only_es.png"), g2, width = 12, height = 8, dpi = 150)
