# Simulated-instrument event studies: house prices, jurisdiction revenue and migration on the ACS/TAXSIM cap burden
# (build/exposure/06). Written 2026-10-27.
# Exposure per owner household ($/yr, winsorised p1/p99 within the county sample, per $100):
#   Z1  = 2016 ACS households at their own location through TAXSIM (cap burden, 2018 law with vs without the cap);
#   Z2  = one national 2016 sample at the county's effective rate and state law (rate instrument; no local prices/incomes);
#   SOI = observed lost SALT value per owner, TY2016 -> TY2018 (build/exposure/05, d_salt_own), which embeds the 2017+ responses.
# Specs, all | county + state x year, weighted by 2016 returns, SE by county, ref 2017:
#   OLS  y ~ i(year, X)            for X in {Z1, Z2, SOI}
#   IV   y ~ i(year, SOI)  instrumented by i(year, Z1)   (the Currie-Gruber first stage: observed local change on simulated)
# County variation is used (outcomes exist at county level; states give 51 units; PUMAs have no outcomes). The county value
# is the ACS county estimate where the ACS identifies the county (430 counties, ~70% of returns), else the tract-weighted mean
# of its PUMAs' values (2010 tract-PUMA relationship file). Sample A = identified counties; sample B = all counties.
# Outcomes: log median arm's-length price (CoreLogic county panel 2013-24, n_sales >= 50, balanced); log property-tax revenue
# (CoG school districts by county FY2012-22, county governments balanced); SOI out-migration and out-of-state rates (2012-22).
suppressPackageStartupMessages({library(data.table); library(fixest); library(duckdb); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/exposure_did"); invisible(out_dir("designs/exposure_did/figures")); L <- file.path(DATA, "built")

# ---- exposure at county level ---------------------------------------------------------------------------------------------
zc <- fread(file.path(L, "exposure/simulated_instrument_county.csv"), colClasses = c(fips = "character"))[, .(fips, z1_direct = z1_cap, z2_direct = z2_cap, n_acs)]
zpu <- fread(file.path(L, "exposure/simulated_instrument_puma.csv"), colClasses = c(puma5 = "character"))[, .(puma5, z1_cap, z2_cap)]
xw <- fread(file.path(DATA, "external/puma_rel/2010_Census_Tract_to_2010_PUMA.txt"), colClasses = "character")
xw[, `:=`(fips = paste0(STATEFP, COUNTYFP), puma5 = paste0(STATEFP, PUMA5CE))]
zf <- merge(xw[, .N, by = .(fips, puma5)], zpu, by = "puma5")[, .(z1_puma = weighted.mean(z1_cap, N), z2_puma = weighted.mean(z2_cap, N, na.rm = TRUE)), by = fips]
Z <- merge(zf, zc, by = "fips", all = TRUE)[, `:=`(direct = !is.na(z1_direct))]
Z[, `:=`(z1 = fifelse(direct, z1_direct, z1_puma), z2 = fifelse(!is.na(z2_direct), z2_direct, z2_puma))]
ec <- fread(file.path(L, "exposure/exposure_components_county.csv"), colClasses = c(geo = "character"))[, .(fips = geo, returns16, soi = d_salt_own)]
Z <- merge(Z, ec, by = "fips")[!is.na(z1) & !is.na(z2) & !is.na(soi)]
wvar <- \(x, w) weighted.mean((x - weighted.mean(x, w))^2, w)
wins <- \(x) { q <- quantile(x, c(.01, .99), na.rm = TRUE); pmin(pmax(x, q[1]), q[2]) / 100 }
Z[, `:=`(z1 = wins(z1), z2 = wins(z2), soi = wins(soi))][, st := substr(fips, 1, 2)]
cat("counties with exposure:", nrow(Z), "| ACS-identified:", sum(Z$direct), "| share of 2016 returns in identified counties:", round(Z[, sum(returns16[direct]) / sum(returns16)], 3), "\n")

# ---- outcomes -------------------------------------------------------------------------------------------------------------
YRS <- 2013:2024
cy <- readRDS(file.path(L, "corelogic/county_year.rds"))[y %in% YRS & n_sales >= 50 & p50 > 0][, nY := .N, by = fips][nY == length(YRS), .(fips, y, out = log(p50))]
con <- db_open("census_finances_test.db")
qfin <- function(tb) as.data.table(dbGetQuery(con, sprintf("SELECT county_fips::INT county, year::INT y, property_tax_rev ptax FROM %s", tb)))[
  ptax > 0 & y <= 2022, .(fips = sprintf("%05d", county), y, out = log(ptax))]
rv_s <- qfin("govt_finances_sd_by_county"); rv_c <- qfin("govt_finances_county")[, nY := .N, by = fips][nY == 11][, nY := NULL]
dbDisconnect(con, shutdown = TRUE)
M <- file.path(DATA, "SOI/migration_county"); yrs <- 12:22
fl <- rbindlist(lapply(yrs, \(yy) { x <- fread(file.path(M, sprintf("countyoutflow%02d%02d.csv", yy - 1, yy)), colClasses = "character", encoding = "Latin-1")
  x[, .(o = sprintf("%02d%03d", as.integer(y1_statefips), as.integer(y1_countyfips)), ds = as.integer(y2_statefips), dc = as.integer(y2_countyfips),
        name = y2_countyname, n1 = as.numeric(n1), y = 2000L + yy)] }))
fl[n1 < 0, n1 := NA][, o_st := substr(o, 1, 2)]
tot <- fl[, .(nonmig = sum(n1[ds == as.integer(o_st) & sprintf("%02d%03d", ds, dc) == o & grepl("Non-migrants", name)], na.rm = TRUE),
              out_all = sum(n1[ds == 97 & dc == 0], na.rm = TRUE), out_xst = sum(n1[ds == 97 & dc == 3], na.rm = TRUE)), by = .(o, y)]
tot <- tot[nonmig > 0][, base := nonmig + out_all][, nY := .N, by = o][nY == length(yrs)]
mg_o <- tot[, .(fips = o, y, out = 100 * out_all / base)]; mg_x <- tot[, .(fips = o, y, out = 100 * out_xst / base)]
panels <- list("log median sale price" = cy, "log property-tax revenue, school districts" = rv_s, "log property-tax revenue, county governments" = rv_c,
               "out-migration rate (% of returns)" = mg_o, "out-of-state migration rate (%)" = mg_x)

# ---- estimation -----------------------------------------------------------------------------------------------------------
est <- function(d, x, lab, sample) {
  d <- merge(d, Z[, .(fips, st, returns16, z1, z2, soi, direct)], by = "fips"); if (sample == "A") d <- d[direct == TRUE]
  f <- if (x == "iv") out ~ 1 | fips + st^y | i(y, soi, ref = 2017) ~ i(y, z1, ref = 2017) else as.formula(sprintf("out ~ i(y, %s, ref = 2017) | fips + st^y", x))
  m <- feols(f, d, cluster = ~fips, weights = ~returns16)
  ct <- as.data.table(coeftable(m), keep.rownames = "term")[grepl("^(fit_)?y::", term)]
  r <- ct[, .(y = as.integer(sub("^(fit_)?y::(\\d+):.*", "\\2", term)), b = Estimate, se = `Std. Error`)]
  rbind(r, data.table(y = 2017L, b = 0, se = 0))[, `:=`(outcome = lab, spec = x, sample = sample, n_cty = uniqueN(d$fips), n = nobs(m))] }
specs <- c(z1 = "Z1 (ACS, own location)", z2 = "Z2 (national sample at county rate)", soi = "SOI observed (OLS)", iv = "SOI instrumented by Z1")
res <- rbindlist(lapply(names(panels), \(p) rbindlist(lapply(c("A", "B"), \(s) rbindlist(lapply(names(specs), \(x) est(panels[[p]], x, p, s)))))))
res[, spec := factor(specs[spec], specs)][, sample := factor(sample, c("A", "B"), c("A: ACS-identified counties", "B: all counties (PUMA fill-in)"))]
res[, scale := fifelse(grepl("^log", outcome), 100, 1)]   # % for log outcomes, pp for rates
fwrite(res, file.path(OUT, "c14_simulated_instrument.csv"))

# first stages (cross-section, 2016 returns weights): observed SOI loss per owner on Z1 / Z2
fs <- rbindlist(lapply(c("A", "B"), \(s) { d <- if (s == "A") Z[direct == TRUE] else Z
  rbindlist(lapply(c("z1", "z2"), \(x) rbindlist(lapply(c(FALSE, TRUE), \(fe) {
    m <- feols(as.formula(sprintf("soi ~ %s%s", x, if (fe) " | st" else "")), d, weights = ~returns16, cluster = ~st)
    data.table(sample = s, instrument = x, state_fe = fe, b = coef(m)[x], se = se(m)[x], t = coef(m)[x] / se(m)[x], r2 = r2(m, if (fe) "wr2" else "r2"), n = nobs(m)) })))) }))

sink(file.path(OUT, "c14_simulated_instrument.txt"), split = TRUE)
cat("Simulated-instrument event studies. Exposure per $100/yr of cap burden per owner household (winsorised p1/p99); | county + state x year;\n")
cat("weighted by 2016 returns; SE by county; ref 2017. Log outcomes in %, migration rates in percentage points.\n")
cat("Counties:", nrow(Z), "| ACS-identified (sample A):", sum(Z$direct), "| returns share in A:", round(Z[, sum(returns16[direct]) / sum(returns16)], 3), "\n")
cat("Exposure SDs across counties ($/owner hh, return-weighted): Z1", round(100 * sqrt(wvar(Z$z1, Z$returns16))), "| Z2", round(100 * sqrt(wvar(Z$z2, Z$returns16))),
    "| SOI", round(100 * sqrt(wvar(Z$soi, Z$returns16))), "\n")
cat("\n== First stage: SOI observed lost SALT value per owner ~ instrument (cross-section, weights = 2016 returns, SE by state) ==\n")
print(fs[, .(sample, instrument, state_fe, b = round(b, 3), se = round(se, 3), t = round(t, 1), r2 = round(r2, 3), n)])
for (p in names(panels)) for (s in levels(res$sample)) {
  r <- res[outcome == p & sample == s]
  cat("\n==", p, "|", s, "| counties", r$n_cty[1], "\n")
  print(dcast(r[, .(spec, y, cell = sprintf("%.2f (%.2f)", scale * b, scale * se))], spec ~ y, value.var = "cell")) }
cat("\n== Summary: mean post coefficient 2019-2022 and mean pre coefficient 2013-2016 (same units) ==\n")
print(res[, .(pre = round(mean(scale * b[y %in% 2013:2016]), 2), post = round(mean(scale * b[y %in% 2019:2022]), 2),
              post_se = round(mean(scale * se[y %in% 2019:2022]), 2)), by = .(outcome, sample, spec)])
sink()

g <- ggplot(res, aes(y, scale * b, colour = spec)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey40") +
  geom_pointrange(aes(ymin = scale * (b - 1.96 * se), ymax = scale * (b + 1.96 * se)), position = position_dodge(.6), size = .12, linewidth = .35) +
  geom_line(position = position_dodge(.6), linewidth = .3) +
  facet_grid(outcome ~ sample, scales = "free_y", labeller = label_wrap_gen(28)) + scale_x_continuous(breaks = seq(2012, 2024, 2)) +
  scale_colour_manual(NULL, values = c("#c0392b", "#2a64a8", "grey45", "#e67e22")) +
  labs(x = NULL, y = "effect per $100/yr cap burden per owner household (vs 2017): % for logs, pp for rates",
       title = "Simulated SALT-cap instrument: prices, revenue, migration", subtitle = "county + state x year FE, weighted by 2016 returns, 95% CI by county") +
  theme_ptax(8)
ggsave(file.path(OUT, "figures/c14_simulated_instrument.png"), g, width = 10, height = 12, dpi = 150)
