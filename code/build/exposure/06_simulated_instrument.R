# Simulated SALT-cap instrument from the 2016 ACS (Currie-Gruber style). Written 2026-10-27.
# A fixed pre-reform sample of owner-occupier households is pushed through the federal + state tax code (TAXSIM 35 via
# usincometaxes, WebAssembly) under 2017 law, 2018 law, and 2018 law with the SALT cap removed. The cap burden per household is
#   dcap = T(2018 law) - T(2018 law, excess SALT allowed as an uncapped deduction),
# i.e. the cap alone, holding the rest of TCJA (brackets, standard deduction, exemptions, child credit) at 2018 law; it is zero
# for households that take the standard deduction either way. "Cap removed" is emulated by adding max(0, proptax + state income
# tax - 10k) to TAXSIM's uncapped non-preference deduction field (`mortgage`); AMT treatment of that excess is therefore the
# non-preference one, which matters little under the 2018 AMT exemption. dtcja = T(2018 law) - T(2017 law) is kept for context.
# Two versions:
#   Z1 (own location): each 2016 household at its own state / county / PUMA, own (imputed) property tax; purges post-2017
#      behaviour but keeps 2016 local composition. Top-coded property tax ($10,000+, code 69) is replaced by
#      max(10k, PUMA effective rate x house value).
#   Z2 (national sample by jurisdiction): one national subsample of 2016 owner households is assigned to every state and
#      to a grid of effective property-tax rates, with proptax = rate x own house value; g_s(r) = mean dcap is interpolated at
#      each county's / PUMA's 2016 effective rate (CoreLogic). Variation comes only from state tax law and the local rate.
# Approximations (no better ACS fields): non-wage income (inctot - incwage, head + spouse) enters as taxable property income;
# no charitable giving, so 2017-law itemisation uses SALT + mortgage interest only; children assumed under 17.
# Inputs: data/database.db (acs_microdata, build/acs/01-03), data/built/corelogic/county_year.rds and tract/tract_eff.parquet,
#   data/external/puma_rel/2010_Census_Tract_to_2010_PUMA.txt (Census relationship file), built/exposure/salt_exposure_county2016.csv.
# Outputs: data/built/exposure/simulated_instrument_{county,puma,state}.csv, simulated_z2_grid.csv (and TAXSIM caches
#   taxsim_z1.parquet, taxsim_z2.parquet); output/build/exposure/06_simulated_instrument.{txt,png}.
suppressPackageStartupMessages({library(data.table); library(duckdb); library(usincometaxes); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("build/exposure"); dir.create(file.path(OUT, "figures"), showWarnings = FALSE)
BUILT <- file.path(DATA, "built/exposure"); dir.create(BUILT, showWarnings = FALSE, recursive = TRUE)
M_Z2 <- 8000L; R_GRID <- seq(0, 0.04, by = 0.0025); set.seed(20261027)
fips_ab <- unique(as.data.table(tigris::fips_codes)[, .(statefip = as.integer(state_code), ab = state)])[statefip <= 56]

# ---- 1. 2016 owner-occupier heads --------------------------------------------------------------------------------------------
con <- db_open("database.db")
hh <- dq("SELECT h.serial, h.statefip, h.countyfip, h.puma, h.hhwt, h.hhincome, h.valueh, h.proptx99, h.age, h.marst, h.nchild,
    CASE WHEN h.incwage = 9999999 THEN 0 ELSE h.incwage END wage_h, CASE WHEN h.inctot = 9999999 THEN 0 ELSE h.inctot END inc_h,
    h.imp_proptx, h.imp_mort_interest, s.sage, s.wage_s, s.inc_s
  FROM acs_microdata h LEFT JOIN (SELECT year, serial, min(age) sage, sum(CASE WHEN incwage = 9999999 THEN 0 ELSE incwage END) wage_s,
    sum(CASE WHEN inctot = 9999999 THEN 0 ELSE inctot END) inc_s FROM acs_microdata WHERE relate = 2 AND year = 2016 GROUP BY 1, 2) s
    USING (year, serial)
  WHERE h.year = 2016 AND h.pernum = 1 AND h.ownershp = 1 AND h.gq IN (1, 2) AND h.statefip <= 56", con)
dbDisconnect(con, shutdown = TRUE)
hh <- merge(hh, fips_ab, by = "statefip")
hh[, `:=`(fips = fifelse(countyfip > 0, sprintf("%02d%03d", statefip, countyfip), NA_character_), puma5 = sprintf("%02d%05d", statefip, puma),
          married = marst == 1L)]
hh[, mstat := fifelse(married, "married, jointly", fifelse(nchild > 0, "head of household", "single"))]
hh[, `:=`(page = as.integer(age), sage = fifelse(married, fcoalesce(as.integer(sage), as.integer(age)), 0L), depx = pmin(nchild, 9L),
          pwages = pmax(0, wage_h), swages = fifelse(married, pmax(0, fcoalesce(wage_s, 0)), 0),
          mortgage = pmax(0, fcoalesce(imp_mort_interest, 0)))]
hh[, otherprop := pmax(0, inc_h + fifelse(married, fcoalesce(inc_s, 0), 0) - pwages - swages)]
hh[, `:=`(age1 = fifelse(depx >= 1, 10L, 0L), age2 = fifelse(depx >= 2, 10L, 0L), age3 = fifelse(depx >= 3, 10L, 0L))]
cat("2016 owner-occupier heads:", nrow(hh), "| with county:", sum(!is.na(hh$fips)), "| PUMAs:", uniqueN(hh$puma5),
    "| top-coded property tax:", sum(hh$proptx99 == 69), "\n")

# ---- 2. Effective property-tax rates: county (2016 sales-based median) and PUMA (tract medians via the 2010 crosswalk) -------
cy <- readRDS(file.path(DATA, "built/corelogic/county_year.rds"))[y %in% 2015:2017 & !is.na(eff50), .(rate = median(eff50)), by = fips]
te <- dq(sprintf("SELECT tract, n, med FROM read_parquet('%s')", file.path(DATA, "built/corelogic/tract/tract_eff.parquet")))
xw <- fread(file.path(DATA, "external/puma_rel/2010_Census_Tract_to_2010_PUMA.txt"), colClasses = "character")
xw[, `:=`(tract = paste0(STATEFP, COUNTYFP, TRACTCE), puma5 = paste0(STATEFP, PUMA5CE))]
pr <- merge(te, xw[, .(tract, puma5)], by = "tract")[, .(rate = { o <- order(med); w <- cumsum(n[o]) / sum(n); med[o][which(w >= .5)[1]] }, n_tr = .N), by = puma5]
cat("county rates:", nrow(cy), "| PUMA rates:", nrow(pr), "of", uniqueN(hh$puma5), "PUMAs in the ACS sample\n")
hh <- merge(hh, pr[, .(puma5, rate_puma = rate)], by = "puma5", all.x = TRUE)
hh[, rate_puma := fcoalesce(rate_puma, median(rate_puma, na.rm = TRUE)), by = statefip]
hh[, proptax := fifelse(proptx99 == 69L, pmax(10000, rate_puma * valueh), fcoalesce(imp_proptx, 0))]

# ---- 3. TAXSIM runner: 2017 law, 2018 law, 2018 law without the cap -----------------------------------------------------------
tx_cols <- c("taxsimid", "year", "state", "mstat", "page", "sage", "depx", "age1", "age2", "age3", "pwages", "swages", "otherprop", "proptax", "mortgage")
run_tx <- function(df, chunk = 100000L) {
  out <- vector("list", ceiling(nrow(df) / chunk))
  for (i in seq_along(out)) { idx <- ((i - 1) * chunk + 1):min(i * chunk, nrow(df))
    r <- as.data.table(taxsim_calculate_taxes(as.data.frame(df[idx, ..tx_cols]), return_all_information = TRUE))
    out[[i]] <- r[, .(taxsimid, fiitax, siitax, item = v17_itemized_deductions)] }
  rbindlist(out) }
# d: data.table with taxsimid, state, mstat, page, sage, depx, age1-3, pwages, swages, otherprop, proptax, mortgage
sim_laws <- function(d, label) {
  d <- copy(d); d[, year := 2017L]; cat(label, ": 2017 law on", nrow(d), "rows\n"); r17 <- run_tx(d)
  d[, year := 2018L]; cat(label, ": 2018 law\n"); r18 <- run_tx(d)
  d <- merge(d, r18[, .(taxsimid, si18 = siitax)], by = "taxsimid")
  d[, excess := pmax(0, proptax + pmax(0, si18) - 10000)]
  nc <- d[excess > 0]; nc[, mortgage := mortgage + excess]
  cat(label, ": 2018 law, cap removed, on", nrow(nc), "households with SALT > 10k\n")
  rnc <- if (nrow(nc)) run_tx(nc) else r18[0]
  res <- merge(r17[, .(taxsimid, fed17 = fiitax, st17 = siitax, item17 = item)], r18[, .(taxsimid, fed18 = fiitax, st18 = siitax, item18 = item)], by = "taxsimid")
  res <- merge(res, rnc[, .(taxsimid, fed18nc = fiitax, st18nc = siitax, item18nc = item)], by = "taxsimid", all.x = TRUE)
  res[is.na(fed18nc), `:=`(fed18nc = fed18, st18nc = st18, item18nc = item18)]
  res[, `:=`(dcap = fed18 - fed18nc, dcap_tot = fed18 + st18 - fed18nc - st18nc, dtcja = fed18 + st18 - fed17 - st17,
             bind = as.integer(fed18 - fed18nc > 0.5), it17 = as.integer(item17 > 0), it18 = as.integer(item18 > 0), it18nc = as.integer(item18nc > 0))]
  res }
pq_write <- function(x, f) { con <- dbConnect(duckdb::duckdb()); duckdb_register(con, "x", x); dbExecute(con, sprintf("COPY x TO '%s' (FORMAT parquet)", f)); dbDisconnect(con, shutdown = TRUE) }

# ---- 4. Z1: each household at its own location -------------------------------------------------------------------------------
f1 <- file.path(BUILT, "taxsim_z1.parquet")
if (!file.exists(f1)) { hh[, taxsimid := .I]; hh[, state := ab]; z1 <- sim_laws(hh, "Z1"); pq_write(merge(hh[, .(taxsimid, serial)], z1, by = "taxsimid"), f1) }
z1 <- merge(hh, dq(sprintf("SELECT * FROM read_parquet('%s')", f1))[, -"taxsimid"], by = "serial")
agg1 <- function(by) z1[, .(n_acs = .N, hh = sum(hhwt), z1_cap = weighted.mean(dcap, hhwt), z1_cap_tot = weighted.mean(dcap_tot, hhwt),
                            z1_bind = weighted.mean(bind, hhwt), z1_cap_inc = sum(dcap * hhwt) / sum(pmax(hhincome, 1) * hhwt),
                            z1_tcja = weighted.mean(dtcja, hhwt), z1_item17 = weighted.mean(it17, hhwt), z1_item18 = weighted.mean(it18, hhwt),
                            z1_item18nc = weighted.mean(it18nc, hhwt), proptax = weighted.mean(proptax, hhwt), valueh = weighted.mean(valueh, hhwt),
                            hhincome = weighted.mean(hhincome, hhwt)), by = by]
z1_cty <- agg1("fips")[!is.na(fips)]; z1_puma <- agg1("puma5"); z1_st <- agg1("ab")

# ---- 5. Z2: one national subsample assigned to every state x rate-grid point -------------------------------------------------
f2 <- file.path(BUILT, "taxsim_z2.parquet")
if (!file.exists(f2)) {
  smp <- hh[sample(.N, M_Z2, replace = TRUE, prob = hhwt), .(hid = .I, mstat, page, sage, depx, age1, age2, age3, pwages, swages, otherprop, mortgage, valueh, hhincome)]
  g <- CJ(hid = smp$hid, state = fips_ab$ab, r = R_GRID)
  g <- merge(g, smp, by = "hid")[, proptax := r * valueh][, taxsimid := .I]
  z2 <- sim_laws(g, "Z2"); pq_write(merge(g[, .(taxsimid, hid, state, r, hhincome)], z2, by = "taxsimid"), f2) }
z2 <- dq(sprintf("SELECT * FROM read_parquet('%s')", f2))
grid <- z2[, .(z2_cap = mean(dcap), z2_cap_tot = mean(dcap_tot), z2_bind = mean(bind), z2_cap_inc = sum(dcap) / sum(pmax(hhincome, 1)),
               z2_tcja = mean(dtcja), z2_item17 = mean(it17), z2_item18 = mean(it18)), by = .(state, r)][order(state, r)]
fwrite(grid, file.path(BUILT, "simulated_z2_grid.csv"))
interp <- function(st, rate) { g <- grid[state == st]; if (!nrow(g) || is.na(rate)) return(rep(NA_real_, 4))
  rate <- pmin(pmax(rate, min(R_GRID)), max(R_GRID)); sapply(c("z2_cap", "z2_cap_tot", "z2_bind", "z2_cap_inc"), \(v) approx(g$r, g[[v]], xout = rate)$y) }
add_z2 <- function(d, st_col) { d[, c("z2_cap", "z2_cap_tot", "z2_bind", "z2_cap_inc") := as.list(interp(get(st_col), rate)), by = seq_len(nrow(d))]; d }
z1_cty <- merge(z1_cty, cy, by = "fips", all.x = TRUE)[, ab := fips_ab[match(as.integer(substr(fips, 1, 2)), statefip), ab]]
z1_cty <- add_z2(z1_cty, "ab")
z1_puma <- merge(z1_puma, pr[, .(puma5, rate)], by = "puma5", all.x = TRUE)[, ab := fips_ab[match(as.integer(substr(puma5, 1, 2)), statefip), ab]]
z1_puma <- add_z2(z1_puma, "ab")
st_rate <- z1[, .(rate = weighted.mean(rate_puma, hhwt)), by = ab]
z1_st <- add_z2(merge(z1_st, st_rate, by = "ab"), "ab")
fwrite(z1_cty, file.path(BUILT, "simulated_instrument_county.csv")); fwrite(z1_puma, file.path(BUILT, "simulated_instrument_puma.csv"))
fwrite(z1_st, file.path(BUILT, "simulated_instrument_state.csv"))

# ---- 6. Compare with the SOI county exposure ---------------------------------------------------------------------------------
soi <- fread(file.path(DATA, "built/exposure/salt_exposure_county2016.csv"))[, fips := sprintf("%05d", county)]
cmp <- merge(z1_cty, soi[, .(fips, returns, excess_pp, sh_capped, ex_z)], by = "fips")
wcor <- function(x, y, w) { ok <- complete.cases(x, y, w); cov.wt(cbind(x[ok], y[ok]), w[ok], cor = TRUE)$cor[1, 2] }
sink(file.path(OUT, "06_simulated_instrument.txt"))
cat("Simulated SALT-cap instrument, 2016 ACS owner-occupier heads (n =", nrow(hh), "), TAXSIM 35.\n")
cat("dcap = federal tax under 2018 law minus 2018 law with the cap removed; dtcja = 2018-law minus 2017-law total (fed + state) tax.\n\n")
cat("== National means (weighted, Z1) ==\n"); print(z1[, .(dcap = weighted.mean(dcap, hhwt), dcap_tot = weighted.mean(dcap_tot, hhwt), bind = weighted.mean(bind, hhwt),
  dtcja = weighted.mean(dtcja, hhwt), item17 = weighted.mean(it17, hhwt), item18 = weighted.mean(it18, hhwt), item18_nocap = weighted.mean(it18nc, hhwt))])
cat("\n== By state (Z1 own location; Z2 national sample at the state's mean PUMA rate) ==\n")
print(z1_st[order(-z1_cap), .(ab, n_acs, z1_cap = round(z1_cap), z1_bind = round(z1_bind, 3), z1_tcja = round(z1_tcja), rate = round(rate, 4), z2_cap = round(z2_cap), z2_bind = round(z2_bind, 3))], nrows = 60)
cat("\n== Z2 grid: national sample, cap burden ($/owner household) by state and effective rate ==\n")
print(dcast(grid[round(r, 4) %in% c(0.005, 0.01, 0.015, 0.02, 0.03)], state ~ r, value.var = "z2_cap")[, lapply(.SD, \(x) if (is.numeric(x)) round(x) else x)], nrows = 60)
cat("\n== County level: coverage and correlations (weights = 2016 SOI returns) ==\n")
cat("counties with Z1:", nrow(z1_cty), "| with county rate (Z2):", sum(!is.na(z1_cty$z2_cap)), "| matched to SOI:", nrow(cmp), "\n")
cat(sprintf("corr(Z1 cap, SOI excess/return) = %.3f | corr(Z2 cap, SOI) = %.3f | corr(Z1, Z2) = %.3f | corr(Z1 bind share, SOI capped share) = %.3f\n",
            wcor(cmp$z1_cap, cmp$excess_pp, cmp$returns), wcor(cmp$z2_cap, cmp$excess_pp, cmp$returns), wcor(cmp$z1_cap, cmp$z2_cap, cmp$returns),
            wcor(cmp$z1_bind, cmp$sh_capped, cmp$returns)))
cmp[, `:=`(z1_d = z1_cap - weighted.mean(z1_cap, returns), soi_d = excess_pp - weighted.mean(excess_pp, returns)), by = ab]
cmp[!is.na(z2_cap), `:=`(z2_d = z2_cap - weighted.mean(z2_cap, returns), soi_d2 = excess_pp - weighted.mean(excess_pp, returns),
                         z1_d2 = z1_cap - weighted.mean(z1_cap, returns)), by = ab]
cat("within-state (state means removed):", sprintf("corr(Z1, SOI) = %.3f | corr(Z2, SOI) = %.3f | corr(Z1, Z2) = %.3f\n",
    wcor(cmp$z1_d, cmp$soi_d, cmp$returns), wcor(cmp$z2_d, cmp$soi_d2, cmp$returns), wcor(cmp$z1_d2, cmp$z2_d, cmp$returns)))
cat("\n== Top 15 counties by Z1 (>= 200 ACS households) ==\n")
print(cmp[n_acs >= 200][order(-z1_cap)][1:15, .(fips, ab, n_acs, z1_cap = round(z1_cap), z1_bind = round(z1_bind, 3), rate = round(rate, 4), z2_cap = round(z2_cap), soi_excess_pp = round(excess_pp, 3))])
cat("\n== PUMA level ==\n"); cat("PUMAs:", nrow(z1_puma), "| with rate:", sum(!is.na(z1_puma$rate)), "| SD of Z1 cap across PUMAs (hh-weighted):",
    round(sqrt(weighted.mean((z1_puma$z1_cap - weighted.mean(z1_puma$z1_cap, z1_puma$hh))^2, z1_puma$hh))), "| of Z2:",
    round(sqrt(weighted.mean((z1_puma$z2_cap - weighted.mean(z1_puma$z2_cap, z1_puma$hh, na.rm = TRUE))^2, z1_puma$hh, na.rm = TRUE))), "\n")
sink(); cat(readLines(file.path(OUT, "06_simulated_instrument.txt")), sep = "\n")
pl <- rbind(cmp[, .(x = excess_pp * 1000, y = z1_cap, w = returns, panel = "Z1 (own location) vs SOI excess SALT per return")],
            cmp[, .(x = excess_pp * 1000, y = z2_cap, w = returns, panel = "Z2 (national sample at county rate) vs SOI")],
            cmp[, .(x = z2_cap, y = z1_cap, w = returns, panel = "Z1 vs Z2")])
g <- ggplot(pl[!is.na(y)], aes(x, y)) + geom_point(aes(size = w), alpha = .25, colour = COL_SIDE["high"]) + geom_smooth(aes(weight = w), method = "lm", se = FALSE, colour = "black", linewidth = .5) +
  facet_wrap(~ panel, scales = "free") + scale_size_area(max_size = 5, guide = "none") +
  labs(x = "$ per return (SOI) / $ per owner household (Z2)", y = "simulated cap burden, $ per owner household", title = "Simulated SALT-cap instrument vs observed SOI exposure, counties (TY2016)",
       subtitle = "Z1: 2016 ACS households at their own location; Z2: one national sample at each county's effective rate and state code; point size = returns") + theme_ptax()
ggsave(file.path(OUT, "figures/06_simulated_instrument.png"), g, width = 11, height = 4.2, dpi = 150)
