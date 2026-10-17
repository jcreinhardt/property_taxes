# s9_assessment.R — Strategy 9a (CT/NJ revaluations) and 9b (California Prop 13 cohorts). Pre-specified 2026-10-16.
source("code/analysis/salt_cap/R/00_setup.R"); source("code/analysis/salt_cap/R/01b_load.R")
norm_muni <- function(x) { x <- toupper(trimws(x)); x <- gsub("\\b(BOROUGH|TOWNSHIP|CITY|TOWN|VILLAGE|BORO|TWP)\\b", "", x); gsub("[^A-Z]", "", x) }
# ---- published revaluation schedules -------------------------------------------------------------------------------------
nj <- fread(file.path(DATA, "external/nj_reval/nj_reval_2017_2022.csv")); nj[, st := "NJ"]
NJC <- data.table(county = c("Atlantic","Bergen","Burlington","Camden","Cape May","Cumberland","Essex","Gloucester","Hudson","Hunterdon","Mercer","Middlesex","Monmouth","Morris","Ocean","Passaic","Salem","Somerset","Sussex","Union","Warren"),
                  fips = 34000 + c(1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41))
nj <- merge(nj, NJC, by = "county"); nj[, key := paste(fips, norm_muni(municipality))]   # county fips + municipality (several NJ townships share a name)
ct <- fread(file.path(DATA, "external/ct_reval/ct_reval_2021_2025.csv")); setnames(ct, c("town_code", "town", "cycle1", "cycle2", "cycle3"))
ct[, key := norm_muni(town)]; ct[, st := "CT"]
# CT: the published 2023-2037 zone schedule post-dates PA 22-74 (re-zoning); earlier revaluation years are taken from the roll itself
# (town-year with >= 20% of parcels changing assessed value by > 3%) and compared with cycle1 - 5 / - 10 as the pre-2022 statutory cycle.
roll <- dq(sprintf("SELECT a.clip, a.y AS year, a.av, a.reval_year, a.bill, p.st, p.muni_name, p.muni_code, p.fips FROM read_parquet('%s') a JOIN read_parquet('%s') p USING (clip)
                    WHERE p.st IN ('CT','NJ') AND p.base_oo AND a.y BETWEEN 2011 AND 2024 AND a.av IS NOT NULL", pq("analysis_panel"), pq("analysis_parcels")))
roll[, town := fifelse(st == "CT", paste(st, muni_name), paste(st, fips, muni_name))]
setorder(roll, clip, year)
roll[, dav := av / shift(av) - 1, by = clip]; roll[, dy := year - shift(year), by = clip]
emp <- roll[dy == 1 & !is.na(dav), .(n = .N, share_chg = mean(abs(dav) > 0.03), med_chg = median(dav)), by = .(st, town, year)][n >= 30]
emp[, reval_emp := share_chg >= 0.20]
fwrite(emp, file.path(OUT_S, "table_s9a_empirical_reval_years.csv"))
towns <- unique(roll[, .(st, town, muni_name, key = fifelse(st == "NJ", paste(fips, norm_muni(muni_name)), norm_muni(muni_name)))])
towns <- merge(towns, nj[, .(st, key, nj_year = year)], by = c("st", "key"), all.x = TRUE)
towns <- merge(towns, ct[, .(st, key, cycle1)], by = c("st", "key"), all.x = TRUE)
# published reval year(s) in 2018-2020: NJ from the lists; CT from cycle1 - 5 (statutory 5-year cycle before the 2022 re-zoning)
pub <- rbind(towns[st == "NJ" & !is.na(nj_year), .(st, town, year = nj_year)], towns[st == "CT" & !is.na(cycle1), .(st, town, year = cycle1 - 5L)], towns[st == "CT" & !is.na(cycle1), .(st, town, year = cycle1 - 10L)])
pub <- unique(pub)
chk <- merge(emp[year %in% 2013:2022], pub[, .(st, town, year, published = TRUE)], by = c("st", "town", "year"), all.x = TRUE)[, published := !is.na(published)]
agree <- chk[, .(n_town_years = .N, emp_and_pub = sum(reval_emp & published), emp_only = sum(reval_emp & !published), pub_only = sum(!reval_emp & published)), by = st]
fwrite(agree, file.path(OUT_S, "table_s9a_schedule_check.csv")); print(agree)
add_ledger("S9a", "Revaluation timing exogenous to 2018 (schedules set before 2017)", "Published NJ lists 2017-22 / CT statutory cycle vs roll-based revaluation years; unscheduled revaluations",
           paste(sprintf("%s: %d matches, %d unscheduled (roll only), %d published-only", agree$st, agree$emp_and_pub, agree$emp_only, agree$pub_only), collapse = "; "),
           if (sum(agree$emp_only) <= 0.25 * sum(agree$emp_and_pub)) "pass" else "inconclusive")
# treatment: town revalued (roll-based, confirmed by schedule where available) in 2018-2020; parcel-level crossing of $10k at the revaluation
rv <- emp[reval_emp == TRUE & year %in% 2018:2020, .(st, town, reval_y = min(year)), by = .(st, town)][, .(st, town, reval_y)]
other <- emp[reval_emp == TRUE & year %in% c(2013:2016, 2021:2023), unique(town)]
d <- load_panel(2012:2022, "oo", extra_cols = "p.muni_name")[st %in% c("CT", "NJ") & rate_ok == TRUE & bill16 >= 5000 & bill16 <= 20000]
d[, town := fifelse(st == "CT", paste(st, muni_name), paste(st, county, muni_name))]
d <- merge(d, rv, by = c("st", "town"), all.x = TRUE)
d[, reval := as.integer(!is.na(reval_y))]
d <- d[reval == 1 | town %in% other]                     # comparison: towns revaluing at other times
bl <- d[, .(bill_pre = bill_t[year == reval_y - 1][1], bill_at = bill_t[year == reval_y][1]), by = .(clip, reval_y)]
d <- merge(d, bl, by = c("clip", "reval_y"), all.x = TRUE)
d[, cross := as.integer(reval == 1 & !is.na(bill_pre) & !is.na(bill_at) & bill_pre <= 10000 & bill_at > 10000)]
d[, post := as.integer(year >= 2018)]
cat("S9a: towns revalued 2018-20:", uniqueN(d[reval == 1, town]), " parcels", uniqueN(d[reval == 1, clip]), " crossing 10k:", uniqueN(d[cross == 1, clip]), " comparison towns:", uniqueN(d[reval == 0, town]), "\n")
for (oc in c("sale", "log_price", "exit_prim", "refi")) for (tr in c("reval", "cross")) {
  r <- tryCatch(run_es(d, oc, tr, if (oc == "log_price") "vb^year + county^year" else "clip + vb^year + county^year", ~town, "S9a", paste0("S9a_", tr), label = sprintf("S9a CT/NJ revaluation 2018-20: %s on %s", oc, tr), fig = tr == "cross"), error = function(e) NULL)
  if (!is.null(r)) { cat(sprintf("  %-10s %-6s post=%.4g (%.4g) p_pre=%.3f N=%d\n", oc, tr, r$summary$post_coef, r$summary$post_se, r$summary$pre_p, r$N))
    if (tr == "cross") add_ledger("S9a", sprintf("Parallel trends reval vs other-time towns [%s]", oc), "Pre-trends 2012-2015", sprintf("F=%.2f p=%.3f", r$summary$pre_F, r$summary$pre_p), verdict_pre(r$summary$pre_p, r$summary$pre_mean_abs, r$summary$post_coef)) }
}
# ---- 9b: California Prop 13 purchase-year cohorts within block group, DDD vs WA/OR/NV/AZ/CO -------------------------------
CTRL <- c("WA", "OR", "NV", "AZ", "CO")
db <- load_panel(2012:2019, "oo", extra_cols = "p.tract, p.bg")[st %in% c("CA", CTRL) & rate_ok == TRUE & !is.na(bg) & !is.na(buy_year)]
db[, cohort := cut(buy_year, c(-Inf, 2000, 2007, 2012, 2016), labels = c("pre2001", "2001_07", "2008_12", "2013_16"))]
db <- db[!is.na(cohort)]
db[, ca := as.integer(st == "CA")]; db[, bgk := paste(st, county, tract, bg)]
db[, recent := as.integer(cohort %in% c("2008_12", "2013_16"))]; db[, recent_ca := recent * ca]
# descriptive: bill gap by cohort within block group, CA vs control
gap <- db[year == 2016, .(med_rate = median(rate16), n = .N), by = .(ca, cohort)][order(ca, cohort)]; fwrite(gap, file.path(OUT_S, "table_s9b_rate_by_cohort.csv")); print(gap)
for (oc in c("sale", "log_price", "refi")) {
  dd <- db[!is.na(get(oc))]; if (oc == "log_price") dd <- dd[sale == 1]
  fe <- if (oc == "log_price") "bgk^year + cohort^ca" else "clip + bgk^year"   # cohort^ca^year would absorb recent x CA x year (fixed 2026-10-16, cluster 10% run)
  ctrl <- if (oc == "log_price") paste0(" + ", HEDONIC) else ""
  m <- feols(as.formula(sprintf("%s ~ i(year, recent, ref = 2016) + i(year, recent_ca, ref = 2016)%s | %s", oc, ctrl, fe)), dd, cluster = ~juris, notes = FALSE, mem.clean = TRUE)
  dropped <- grep("recent_ca", m$collin.var, value = TRUE)   # thin cells (1% sample): DDD terms dropped as collinear -> skip outcome, report (2026-10-16, see agent_notes_S7_S9.md)
  if (length(dropped)) { cat(sprintf("S9b %-10s SKIPPED: %d of 7 recent_ca x year terms collinear after singleton removal (%s); N=%d\n", oc, length(dropped), paste(dropped, collapse = ", "), nobs(m)))
    add_ledger("S9b", sprintf("Cohort trends same in CA and control states [%s]", oc), "Pre-period DDD joint test", sprintf("not identified on this sample: %d DDD terms collinear within block group x year (N=%d)", length(dropped), nobs(m)), "untestable"); next }
  nm <- names(coef(m)); yr <- suppressWarnings(as.integer(sub(".*year::(\\d{4}).*", "\\1", nm))); sel <- grepl("recent_ca", nm)
  w <- as.numeric(sel & yr %in% 2018:2019) / 2; post <- sum(w * coef(m)); se_ <- sqrt(as.numeric(t(w) %*% vcov(m) %*% w))
  ipre <- which(sel & yr %in% 2012:2015); b <- coef(m)[ipre]; V <- vcov(m)[ipre, ipre]; preF <- as.numeric(t(b) %*% solve(V) %*% b) / length(ipre); prep <- pf(preF, length(ipre), 1e6, lower.tail = FALSE)
  add_estimate("S9b", oc, post, se_, nobs(m), uniqueN(dd$juris), "S9b_base")
  cf <- as.data.table(coeftable(m), keep.rownames = "term"); setnames(cf, c("term", "coef", "se", "t", "p")); cf <- cf[grepl("recent_ca", term)]
  cf[, year := as.integer(sub(".*year::(\\d{4}).*", "\\1", term))]; cf <- rbind(cf[, .(year, coef, se)], data.table(year = 2016L, coef = 0, se = 0))[order(year)]
  fwrite(cf, file.path(FIG, sprintf("es_S9b_%s_S9b_base.csv", oc))); es_plot(cf, sprintf("S9b Prop 13: %s, recent cohort x CA", oc), "DDD coefficient", file.path(FIG, sprintf("es_S9b_%s_S9b_base.png", oc)))
  add_ledger("S9b", sprintf("Cohort trends same in CA and control states [%s]", oc), "Pre-period DDD joint test", sprintf("F=%.2f p=%.3f; post %.4g (%.4g)", preF, prep, post, se_), verdict_pre(prep, mean(abs(b)), post))
  cat(sprintf("S9b %-10s post=%.4g (%.4g) pre p=%.3f N=%d\n", oc, post, se_, prep, nobs(m)))
}
cat("S9 done\n")
