# More school-district and local-government outcomes for the near-border design (13 / 16), with coverage. Low-tax side
# within 50 mi, rural pairs (09's default rule), near = 1(distance < 10 mi); districts / counties get the population share of
# their tracts within 10 mi and enter when >= 50% of their population is in the sample (nb_panels). Outcomes:
#   F-33 districts FY2012-23 (build/census_finances/04, widened 2026-10-28): revenue by source, spending by function and
#     object, capital outlay, payments to charters / private schools / other governments, debt, cash; all logged per pupil
#     (0 -> NA, so coverage = share of district-years with a positive value);
#   SEDA 4.1 district test scores and SES, 2009-18 (data/built/acs/seda_district_year.rds), in levels;
#   CoG school districts by county 2012-22 (complete yearly): federal / state revenue, capital outlay, debt, cash;
#   CoG all local governments in the county area, census years 2012 / 2017 / 2022: sales and income tax, intergovernmental
#     revenue, capital outlay, debt, cash, and current spending by function (Census function codes E12 education, E24 fire,
#     E32 health, E36 hospitals, E44 highways, E50 housing, E52 libraries, E61 parks, E62 police, E74-79 welfare, E80 sewerage,
#     E81 solid waste, E89 general government); per capita (ACS 2013-17 county population), logged, 0 -> NA.
# For each outcome: coverage (units, unit-years, share of unit-years with a value, by year), the near x year DiD of 13
# (post - pre, SD units of the 2016 cross-section) and the near x own-exposure x year interaction of 16 (continuous exposure,
# lost subsidy per owner household). Tile figures of both; line figures of the near x year series.
suppressPackageStartupMessages({library(data.table); library(DBI); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); invisible(out_dir("designs/border_rd/figures")); MX <- 50; REF <- 2017
source("code/designs/border_rd/nb_panels.R")
d <- readRDS(file.path(DATA, "built/geo/low_side_balance_tracts.rds"))[dist_mi < MX]
metro <- fread(file.path(OUT, "c22_low_side_metro_pairs.csv"))$pair; d <- d[!pair %in% metro][, near := as.integer(dist_mi < 10)]
tz <- fread(file.path(DATA, "built/exposure/salt_exposure_tract_zip2016.csv"), colClasses = c(tract = "character", zip = "character"))[, .(tract, zip)]
cz <- fread(file.path(DATA, "built/exposure/exposure_components_zip.csv"), colClasses = c(geo = "character"))[clean == TRUE, .(zip = geo, d_salt_own)]
d <- merge(d, merge(tz, cz, by = "zip")[, .(tract, d_salt_own)], by = "tract", all.x = TRUE)
d[, ex := (d_salt_own - mean(d_salt_own, na.rm = TRUE)) / sd(d_salt_own, na.rm = TRUE)][, near_x := near * ex]
P <- nb_panels(d, c("near", "near_x", "ex")); dis <- P$dis; cnt <- P$cnt
cat("rural low-side tracts:", nrow(d), "| districts:", nrow(dis), "| counties:", nrow(cnt), "\n")
lg <- function(x) ifelse(is.finite(x) & x > 0, log(x), NA_real_)
# ---- F-33 districts, per pupil
f <- readRDS(file.path(DATA, "built/census_finances/district_finance.rds"))[enr > 0]
fv <- c(totrev = "total revenue", fedrev = "federal revenue", title1 = "Title I (C14)", idea = "IDEA special ed (C15)", nutrition = "child nutrition (C25)",
        st_formula = "state formula aid (C01)", st_sped = "state special ed (C05)", st_transp = "state transportation (C06)", st_comp = "state compensatory (C07)",
        loc_parent = "parent govt contributions (T02)", loc_othertax = "other local taxes (T99)", loc_cities = "from cities/counties (D23)", tuition = "tuition from pupils (A07)",
        lunch = "school lunch charges (A09)", activity = "district activity receipts (A13)", fees = "student fees (A15)", private_contrib = "private contributions (U50)",
        totexp = "total expenditure", inst = "instruction", ssvc = "support services", pupil_support = "pupil support (E17)", staff_support = "instr. staff support (E07)",
        gen_admin = "general administration (E08)", sch_admin = "school administration (E09)", om = "operations & maintenance (V40)", transp = "pupil transportation (V45)",
        food = "food services (E11)", capout = "capital outlay", construction = "construction (F12)", equip = "equipment (K09+K10)", pay_private = "payments to private schools (V91)",
        pay_charter = "payments to charter schools (V92)", pay_othersys = "payments to other school systems (Q11)", interest = "interest on debt (I86)",
        salaries = "salaries & wages (Z32)", benefits = "employee benefits (Z33)", inst_salaries = "instruction salaries (V11)", debt_end = "long-term debt outstanding (_41F)",
        debt_issued = "long-term debt issued (_21F)", cash = "cash & securities (W01+W31+W61)")
for (k in names(fv)) f[, (k) := lg(get(k) / enr)]
f <- merge(f[, c("leaid", "y", "conum", names(fv)), with = FALSE], dis, by = "leaid"); setnames(f, "conum", "cty")
# ---- SEDA
se <- merge(readRDS(file.path(DATA, "built/acs/seda_district_year.rds"))[y >= 2012, .(leaid, y, ach, ses)], dis, by = "leaid")
se <- merge(se, unique(f[, .(leaid, cty)]), by = "leaid", all.x = TRUE)[is.na(cty), cty := substr(leaid, 1, 2)]
sv <- c(ach = "district test scores (SEDA, SD)", ses = "district SES (SEDA)")
# ---- CoG
con <- db_open("census_finances_test.db")
sd <- as.data.table(dbGetQuery(con, "SELECT lpad(county_fips::VARCHAR, 5, '0') cty, year y, federal_ig_rev, state_ig_rev, total_capital_outlay, lt_debt_outstanding, lt_debt_issued, cash_securities, interest_on_debt FROM govt_finances_sd_by_county"))
ar <- as.data.table(dbGetQuery(con, "SELECT lpad(county_fips::VARCHAR, 5, '0') cty, year y, general_sales_tax_rev, ind_income_tax_rev, federal_ig_rev, state_ig_rev, other_own_source, enterprise_rev,
  total_capital_outlay, lt_debt_outstanding, lt_debt_issued, cash_securities, interest_on_debt, n_units, n_municipal, n_special_dist,
  amount_e12 e_educ, amount_e24 e_fire, amount_e32 e_health, amount_e36 e_hospitals, amount_e44 e_highways, amount_e50 e_housing, amount_e52 e_libraries, amount_e61 e_parks, amount_e62 e_police,
  amount_e74 + amount_e75 + amount_e77 + amount_e79 e_welfare, amount_e80 e_sewer, amount_e81 e_solidwaste, amount_e89 e_general
  FROM govt_finances_county_area WHERE year IN (2012, 2017, 2022)")); dbDisconnect(con, shutdown = TRUE)
cpop <- fread(file.path(DATA, "external/acs/acs5_tract_2017.csv"), colClasses = c(GEOID = "character"))[, .(cpop = sum(popE, na.rm = TRUE)), by = .(cty = substr(GEOID, 1, 5))]
sdv <- c(federal_ig_rev = "school districts: federal revenue", state_ig_rev = "school districts: state revenue", total_capital_outlay = "school districts: capital outlay",
         lt_debt_outstanding = "school districts: LT debt outstanding", lt_debt_issued = "school districts: LT debt issued", cash_securities = "school districts: cash & securities", interest_on_debt = "school districts: interest on debt")
arv <- c(general_sales_tax_rev = "all local govts: general sales tax", ind_income_tax_rev = "all local govts: individual income tax", federal_ig_rev = "all local govts: federal revenue",
         state_ig_rev = "all local govts: state revenue", other_own_source = "all local govts: other own-source revenue", enterprise_rev = "all local govts: enterprise revenue",
         total_capital_outlay = "all local govts: capital outlay", lt_debt_outstanding = "all local govts: LT debt outstanding", lt_debt_issued = "all local govts: LT debt issued",
         cash_securities = "all local govts: cash & securities", interest_on_debt = "all local govts: interest on debt",
         e_educ = "spending: education (E12)", e_fire = "spending: fire (E24)", e_health = "spending: health (E32)", e_hospitals = "spending: hospitals (E36)", e_highways = "spending: highways (E44)",
         e_housing = "spending: housing & comm. dev. (E50)", e_libraries = "spending: libraries (E52)", e_parks = "spending: parks (E61)", e_police = "spending: police (E62)",
         e_welfare = "spending: public welfare (E74-79)", e_sewer = "spending: sewerage (E80)", e_solidwaste = "spending: solid waste (E81)", e_general = "spending: general government (E89)",
         n_units = "number of local governments", n_municipal = "number of municipalities", n_special_dist = "number of special districts")
sd <- merge(merge(sd, cpop, by = "cty"), cnt, by = "cty"); for (k in names(sdv)) sd[, (k) := lg(get(k) / cpop)]
ar <- merge(merge(ar, cpop, by = "cty"), cnt, by = "cty"); for (k in setdiff(names(arv), c("n_units", "n_municipal", "n_special_dist"))) ar[, (k) := lg(get(k) / cpop)]
# ---- coverage + estimates
panels <- list(list(f, fv, "district", "leaid"), list(se, sv, "district", "leaid"), list(sd, sdv, "county", "cty"), list(ar, arv, "county", "cty"))
src <- c("F-33 district, log per pupil", "SEDA district", "CoG school districts by county, log per capita", "CoG all local govts, census years, log per capita")
pp <- function(r) if (!is.null(r)) { pre <- if (any(r$y >= 2013 & r$y < REF)) r$y >= 2013 & r$y < REF else r$y < REF   # census-year panel: pre = 2012
  r[, .(post_min_pre = mean(b[y > REF]) - mean(b[pre]), se = sqrt(mean(se[y > REF]^2) + mean(se[pre]^2) / sum(pre)), n = n[1])] }
cov <- list(); res <- list(); ser <- list()
for (i in seq_along(panels)) { p <- panels[[i]][[1]]; vv <- panels[[i]][[2]]; unit <- panels[[i]][[3]]; fe <- panels[[i]][[4]]
  for (k in names(vv)) { lab <- vv[k]; p[, yv := get(k)]
    byy <- p[, .(sh = mean(is.finite(yv))), by = y][order(y)]
    cov[[length(cov) + 1]] <- data.table(source = src[i], outcome = lab, units = uniqueN(p[[fe]]), unit_years = nrow(p), share_with_value = round(mean(is.finite(p$yv)), 3),
      share_min = round(min(byy$sh), 2), share_2016 = round(byy[y == 2016, sh][1], 2), share_2022 = round(byy[y == 2022, sh][1], 2), years = paste(range(p[is.finite(yv), y]), collapse = "-"),
      units_with_value = uniqueN(p[is.finite(yv), get(fe)]))
    if (sum(is.finite(p$yv)) < 200 || uniqueN(p[is.finite(yv), y]) < 3) next
    sdy <- sd(p[y == 2016, yv], na.rm = TRUE); if (!is.finite(sdy) || sdy == 0) sdy <- sd(p$yv, na.rm = TRUE)
    r1 <- es(p, "near", fe, "cty"); r2 <- es(p[is.finite(ex)], "near_x", fe, "cty", extra = sprintf(" + i(y, near, ref = %d) + i(y, ex, ref = %d)", REF, REF))
    if (!is.null(r1)) { ser[[length(ser) + 1]] <- copy(r1)[, `:=`(source = src[i], outcome = lab, b_sd = b / sdy, se_sd = se / sdy)]
      res[[length(res) + 1]] <- pp(r1)[, `:=`(source = src[i], outcome = lab, coef = "near x year", sdy = sdy)] }
    if (!is.null(r2)) res[[length(res) + 1]] <- pp(r2)[, `:=`(source = src[i], outcome = lab, coef = "near x exposure x year", sdy = sdy)] } }
cov <- rbindlist(cov); res <- rbindlist(res)[, `:=`(b_sd = post_min_pre / sdy, se_sd = se / sdy)][, t := b_sd / se_sd]; ser <- rbindlist(ser)
fwrite(cov, file.path(OUT, "c31_more_local_outcomes_coverage.csv")); fwrite(res, file.path(OUT, "c31_more_local_outcomes.csv")); fwrite(ser, file.path(OUT, "c31_more_local_outcomes_series.csv"))
sink(file.path(OUT, "c31_more_local_outcomes.txt"), split = TRUE)
cat("Coverage in the rural low-side sample (units with >= 50% of population in the sample):", nrow(dis), "districts,", nrow(cnt), "counties. share_with_value = share of unit-years with a\n",
    "positive (logged) value; share_min = lowest yearly share; units_with_value = units ever observed.\n")
for (s in src) { cat("\n==", s, "\n"); print(as.data.frame(cov[source == s, -"source"]), row.names = FALSE) }
cat("\nEstimates: post - pre (mean 2018+ minus mean 2013-16; census-year panel: 2022 minus 2012) in SD units; near x year (DiD, unit FE + pair x year FE) and\n",
    "near x own-exposure x year (continuous exposure, SD per SD). Cells: estimate (t).\n")
for (s in src) { cat("\n==", s, "\n"); w <- dcast(res[source == s, .(outcome, coef, cell = sprintf("%.3f (%.1f)", b_sd, t))], outcome ~ coef, value.var = "cell")
  print(as.data.frame(w[, c("outcome", "near x year", "near x exposure x year")]), row.names = FALSE) }
cat("\ncells with |t| > 1.96 by source and coefficient:\n"); print(res[, .(k = .N, sig = sum(abs(t) > 1.96), median_abs = round(median(abs(b_sd)), 3)), by = .(source, coef)])
sink()
# ---- figures
res[, `:=`(outcome = factor(outcome, rev(unique(cov$outcome))), coef = factor(coef, c("near x year", "near x exposure x year")), source = factor(source, src))]
cv <- cov[, .(outcome = factor(outcome, levels(res$outcome)), source = factor(source, src), lab = sprintf("%d%% of %d", round(100 * share_with_value), unit_years))]
g <- ggplot(res, aes(coef, outcome, fill = pmax(pmin(t, 3), -3))) + geom_tile(colour = "white") + geom_text(aes(label = sprintf("%.2f", b_sd)), size = 2.2) +
  geom_text(data = cv, aes(x = 2.75, y = outcome, label = lab), inherit.aes = FALSE, size = 2, hjust = 0, colour = "grey30") +
  facet_wrap(~ source, scales = "free_y", ncol = 2) + scale_fill_gradient2("t-stat", low = "#c0392b", mid = "white", high = "#2a64a8", limits = c(-3, 3)) +
  scale_x_discrete(expand = expansion(add = c(.6, 2.2))) +
  labs(x = NULL, y = NULL, title = "More district and local-government outcomes: post-2017 change in the near-border effect and in its exposure loading",
       subtitle = "rural pairs, low-tax side within 50 mi; cells: post - pre in SD units (near; per SD of own lost subsidy for the interaction), colour: t. Right: share of unit-years with a value, and unit-years") +
  theme_minimal(base_size = 7.5) + theme(legend.position = "right")
ggsave(file.path(OUT, "figures/c31_more_local_outcomes_tiles.png"), g, width = 13, height = 12, dpi = 150)
ser[, `:=`(outcome = factor(outcome, unique(cov$outcome)), source = factor(source, src))]
for (s in src[c(1, 3, 4)]) { pl <- ser[source == s]; if (!nrow(pl)) next
  g <- ggplot(pl, aes(y, b_sd)) + geom_hline(yintercept = 0, colour = "grey70") + geom_vline(xintercept = REF + .5, linetype = "22", colour = "grey50") +
    geom_ribbon(aes(ymin = b_sd - 1.96 * se_sd, ymax = b_sd + 1.96 * se_sd), fill = "#2e8b57", alpha = .15) + geom_line(colour = "#2e8b57") + geom_point(colour = "#2e8b57", size = .8) +
    facet_wrap(~ outcome, scales = "free_y", ncol = 5, labeller = label_wrap_gen(28)) + scale_x_continuous(breaks = seq(2012, 2024, 4)) +
    labs(x = NULL, y = "near x year, SD units of the outcome, 2017 = 0; unit FE + pair x year FE; 95% CI, SE by county", title = paste0("Near-border event studies: ", s), subtitle = "rural pairs, low-tax side within 50 mi") +
    theme_minimal(base_size = 7.5) + theme(strip.text = element_text(size = 6))
  ggsave(file.path(OUT, sprintf("figures/c31_more_local_outcomes_es_%s.png", c("f33", "seda", "cog_sd", "cog_area")[match(s, src)])), g, width = 12, height = 2.2 * ceiling(uniqueN(pl$outcome) / 5) + 1, dpi = 150) }
