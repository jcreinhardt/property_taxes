# Migration counts and net migration on the simulated SALT-cap instrument (companion to 14_simulated_instrument.R). 2026-10-27.
# SOI county inflow + outflow files 2012-22 (file 1718 = address change between TY2017 and TY2018 returns = 2018). Outcomes:
#   county: log out-migrant returns (n1) and people (n2, exemptions), log in-migrant returns, out-migration rate of people,
#           net migration rate (in - out, % of base returns / people), net interstate rate (in from other states - out to other states).
#   state:  counties summed; out-of-state, in-from-other-state and net interstate rates (% of returns), log out-of-state returns;
#           state + year FE, weighted by 2016 returns (population weights), SE by state; exposure = state Z1 / Z2 / SOI loss per owner.
# County specs as in 14: | county + state x year, weighted by 2016 returns, SE by county; Z1, Z2, SOI (OLS), SOI instrumented by Z1;
# sample A = ACS-identified counties, B = all counties with PUMA fill-in. All exposures per $100/yr per owner household.
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/exposure_did"); invisible(out_dir("designs/exposure_did/figures")); L <- file.path(DATA, "built")

# ---- exposure at county level (as in 14) ---------------------------------------------------------------------------------
zc <- fread(file.path(L, "exposure/simulated_instrument_county.csv"), colClasses = c(fips = "character"))[, .(fips, z1_direct = z1_cap, z2_direct = z2_cap)]
zpu <- fread(file.path(L, "exposure/simulated_instrument_puma.csv"), colClasses = c(puma5 = "character"))[, .(puma5, z1_cap, z2_cap)]
xw <- fread(file.path(DATA, "external/puma_rel/2010_Census_Tract_to_2010_PUMA.txt"), colClasses = "character")
xw[, `:=`(fips = paste0(STATEFP, COUNTYFP), puma5 = paste0(STATEFP, PUMA5CE))]
zf <- merge(xw[, .N, by = .(fips, puma5)], zpu, by = "puma5")[, .(z1_puma = weighted.mean(z1_cap, N), z2_puma = weighted.mean(z2_cap, N, na.rm = TRUE)), by = fips]
Z <- merge(zf, zc, by = "fips", all = TRUE)[, direct := !is.na(z1_direct)]
Z[, `:=`(z1 = fifelse(direct, z1_direct, z1_puma), z2 = fifelse(!is.na(z2_direct), z2_direct, z2_puma))]
ec <- fread(file.path(L, "exposure/exposure_components_county.csv"), colClasses = c(geo = "character"))[, .(fips = geo, returns16, soi = d_salt_own)]
Z <- merge(Z, ec, by = "fips")[!is.na(z1) & !is.na(z2) & !is.na(soi)]
wins <- \(x) { q <- quantile(x, c(.01, .99), na.rm = TRUE); pmin(pmax(x, q[1]), q[2]) / 100 }
Z[, `:=`(z1 = wins(z1), z2 = wins(z2), soi = wins(soi))][, st := substr(fips, 1, 2)]

# ---- SOI flows: out and in, returns and people ---------------------------------------------------------------------------
M <- file.path(DATA, "SOI/migration_county"); yrs <- 12:22
rd <- \(f) fread(f, colClasses = "character", encoding = "Latin-1")
out <- rbindlist(lapply(yrs, \(yy) { x <- rd(file.path(M, sprintf("countyoutflow%02d%02d.csv", yy - 1, yy)))
  x[, .(fips = sprintf("%02d%03d", as.integer(y1_statefips), as.integer(y1_countyfips)), ds = as.integer(y2_statefips), dc = as.integer(y2_countyfips),
        name = y2_countyname, n1 = as.numeric(n1), n2 = as.numeric(n2), y = 2000L + yy)] }))
inn <- rbindlist(lapply(yrs, \(yy) { x <- rd(file.path(M, sprintf("countyinflow%02d%02d.csv", yy - 1, yy)))
  x[, .(fips = sprintf("%02d%03d", as.integer(y2_statefips), as.integer(y2_countyfips)), os = as.integer(y1_statefips), oc = as.integer(y1_countyfips),
        n1 = as.numeric(n1), n2 = as.numeric(n2), y = 2000L + yy)] }))
out[n1 < 0, n1 := NA][n2 < 0, n2 := NA]; inn[n1 < 0, n1 := NA][n2 < 0, n2 := NA]
o <- out[, .(nonmig1 = sum(n1[ds <= 56 & sprintf("%02d%03d", ds, dc) == fips & grepl("Non-migrants", name)], na.rm = TRUE),
             nonmig2 = sum(n2[ds <= 56 & sprintf("%02d%03d", ds, dc) == fips & grepl("Non-migrants", name)], na.rm = TRUE),
             out1 = sum(n1[ds == 97 & dc == 0], na.rm = TRUE), out2 = sum(n2[ds == 97 & dc == 0], na.rm = TRUE), outx1 = sum(n1[ds == 97 & dc == 3], na.rm = TRUE)), by = .(fips, y)]
i <- inn[, .(in1 = sum(n1[os == 97 & oc == 0], na.rm = TRUE), in2 = sum(n2[os == 97 & oc == 0], na.rm = TRUE), inx1 = sum(n1[os == 97 & oc == 3], na.rm = TRUE)), by = .(fips, y)]
fl <- merge(o, i, by = c("fips", "y"))[nonmig1 > 0 & out1 > 0 & in1 > 0][, nY := .N, by = fips][nY == length(yrs)]
fl[, `:=`(base1 = nonmig1 + out1, base2 = nonmig2 + out2)]
panels <- list("log out-migrant returns" = fl[, .(fips, y, out = log(out1))], "log out-migrant people (exemptions)" = fl[, .(fips, y, out = log(out2))],
               "log in-migrant returns" = fl[, .(fips, y, out = log(in1))], "out-migration rate, people (%)" = fl[, .(fips, y, out = 100 * out2 / base2)],
               "net migration rate, returns (pp)" = fl[, .(fips, y, out = 100 * (in1 - out1) / base1)], "net migration rate, people (pp)" = fl[, .(fips, y, out = 100 * (in2 - out2) / base2)],
               "net interstate migration rate, returns (pp)" = fl[, .(fips, y, out = 100 * (inx1 - outx1) / base1)])
est <- function(d, x, lab, sample) {
  d <- merge(d, Z[, .(fips, st, returns16, z1, z2, soi, direct)], by = "fips"); if (sample == "A") d <- d[direct == TRUE]
  f <- if (x == "iv") out ~ 1 | fips + st^y | i(y, soi, ref = 2017) ~ i(y, z1, ref = 2017) else as.formula(sprintf("out ~ i(y, %s, ref = 2017) | fips + st^y", x))
  m <- feols(f, d, cluster = ~fips, weights = ~returns16)
  ct <- as.data.table(coeftable(m), keep.rownames = "term")[grepl("^(fit_)?y::", term)]
  rbind(ct[, .(y = as.integer(sub("^(fit_)?y::(\\d+):.*", "\\2", term)), b = Estimate, se = `Std. Error`)], data.table(y = 2017L, b = 0, se = 0))[,
    `:=`(outcome = lab, spec = x, sample = sample, n_cty = uniqueN(d$fips))] }
specs <- c(z1 = "Z1 (ACS, own location)", z2 = "Z2 (national sample at county rate)", soi = "SOI observed (OLS)", iv = "SOI instrumented by Z1")
res <- rbindlist(lapply(names(panels), \(p) rbindlist(lapply(c("A", "B"), \(s) rbindlist(lapply(names(specs), \(x) est(panels[[p]], x, p, s)))))))
res[, spec := factor(specs[spec], specs)][, sample := factor(sample, c("A", "B"), c("A: ACS-identified counties", "B: all counties (PUMA fill-in)"))]
res[, scale := fifelse(grepl("^log", outcome), 100, 1)]

# ---- state level: counties summed, population (returns) weights -----------------------------------------------------------
fips_ab <- unique(as.data.table(tigris::fips_codes)[, .(st = state_code, ab = state)])
sfl <- fl[, .(out1 = sum(out1), in1 = sum(in1), outx1 = sum(outx1), inx1 = sum(inx1), base1 = sum(base1)), by = .(st = substr(fips, 1, 2), y)]
zs <- fread(file.path(L, "exposure/simulated_instrument_state.csv"))[, .(ab, z1 = z1_cap / 100, z2 = z2_cap / 100)]
es <- fread(file.path(L, "exposure/exposure_components_state.csv"), colClasses = c(geo = "character"))[, .(st = geo, returns16, soi = d_salt_own / 100)]
sfl <- merge(merge(merge(sfl, fips_ab, by = "st"), zs, by = "ab"), es, by = "st")
spanels <- list("state: out-of-state rate (% of returns)" = sfl[, .(st, y, out = 100 * outx1 / base1)], "state: in-from-other-state rate (%)" = sfl[, .(st, y, out = 100 * inx1 / base1)],
                "state: net interstate rate (pp)" = sfl[, .(st, y, out = 100 * (inx1 - outx1) / base1)], "state: log out-of-state returns" = sfl[, .(st, y, out = log(outx1))])
sest <- function(d, x, lab) {
  d <- merge(d, unique(sfl[, .(st, returns16, z1, z2, soi)]), by = "st")
  f <- if (x == "iv") out ~ 1 | st + y | i(y, soi, ref = 2017) ~ i(y, z1, ref = 2017) else as.formula(sprintf("out ~ i(y, %s, ref = 2017) | st + y", x))
  m <- feols(f, d, cluster = ~st, weights = ~returns16)
  ct <- as.data.table(coeftable(m), keep.rownames = "term")[grepl("^(fit_)?y::", term)]
  rbind(ct[, .(y = as.integer(sub("^(fit_)?y::(\\d+):.*", "\\2", term)), b = Estimate, se = `Std. Error`)], data.table(y = 2017L, b = 0, se = 0))[,
    `:=`(outcome = lab, spec = x, sample = "S", n_cty = uniqueN(d$st))] }
sres <- rbindlist(lapply(names(spanels), \(p) rbindlist(lapply(names(specs), \(x) sest(spanels[[p]], x, p)))))
sres[, spec := factor(specs[spec], specs)][, sample := factor("S", "S", "S: states, state + year FE")][, scale := fifelse(grepl("log", outcome), 100, 1)]
all <- rbind(res, sres); fwrite(all, file.path(OUT, "c15_simulated_instrument_migration.csv"))

sink(file.path(OUT, "c15_simulated_instrument_migration.txt"), split = TRUE)
cat("Migration counts and net migration on the simulated instrument. Per $100/yr cap burden per owner household; weighted by 2016 returns; ref 2017.\n")
cat("County: | county + state x year, SE by county. State: | state + year, SE by state (51 states). Logs in %, rates in pp.\n")
cat("Counties balanced 2012-22 with positive in- and out-flows:", uniqueN(fl$fips), "| mean out rate (returns)", round(fl[, 100 * mean(out1 / base1)], 2),
    "% | people", round(fl[, 100 * mean(out2 / base2)], 2), "% | mean net rate", round(fl[, 100 * mean((in1 - out1) / base1)], 2), "pp\n")
for (p in c(names(panels), names(spanels))) for (s in unique(all[outcome == p, sample])) {
  r <- all[outcome == p & sample == s]; cat("\n==", p, "|", s, "| units", r$n_cty[1], "\n")
  print(dcast(r[, .(spec, y, cell = sprintf("%.3f (%.3f)", scale * b, scale * se))], spec ~ y, value.var = "cell")) }
cat("\n== Summary: mean pre (2012-16) and post (2019-22) coefficients ==\n")
print(all[, .(pre = round(mean(scale * b[y %in% 2012:2016]), 3), post = round(mean(scale * b[y %in% 2019:2022]), 3), post_se = round(mean(scale * se[y %in% 2019:2022]), 3)),
          by = .(outcome, sample, spec)], nrows = 200)
sink()
g <- ggplot(all, aes(y, scale * b, colour = spec)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey40") +
  geom_pointrange(aes(ymin = scale * (b - 1.96 * se), ymax = scale * (b + 1.96 * se)), position = position_dodge(.6), size = .12, linewidth = .35) +
  geom_line(position = position_dodge(.6), linewidth = .3) +
  facet_wrap(~ outcome + sample, scales = "free_y", ncol = 3, labeller = label_wrap_gen(40)) + scale_x_continuous(breaks = seq(2012, 2022, 2)) +
  scale_colour_manual(NULL, values = c("#c0392b", "#2a64a8", "grey45", "#e67e22")) +
  labs(x = "migration year (address change between tax years y-1 and y)", y = "effect per $100/yr cap burden per owner household (vs 2017): % for logs, pp for rates",
       title = "Simulated SALT-cap instrument: migration counts, in-migration and net migration (SOI)",
       subtitle = "county: county + state x year FE, SE by county; state: state + year FE, SE by state; all weighted by 2016 returns; 95% CI") + theme_ptax(7)
ggsave(file.path(OUT, "figures/c15_simulated_instrument_migration.png"), g, width = 12, height = 16, dpi = 150)
