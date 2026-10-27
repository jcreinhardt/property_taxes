# Formerly code/analysis/salt_local/c24_exposure_delta.R (moved in the 2026-10-27 restructuring).
# Alternative county SALT-cap exposure: the observed change in the federal tax value of deductions per return, TY2016 -> TY2018
# (TY2017 is inflated by prepayment of 2018 property taxes in Dec 2017, so it is only a robustness base). SOI county x AGI-stub
# files (data/SOI/county_returns_w_agi/YYincyallagi.csv; amounts in $1,000). Per county c and stub b:
#   S  = SALT claimed on Schedule A = A18425 (income) + A18450 (sales) + A18500 (real estate) + A18800 (personal property);
#        from TY2018 this is the amount before the $10k limit; L18 = A18460 = SALT deducted after the limit.
#   MID = A19300 (home mortgage interest deducted).
#   amt = N09600 / N04470 (AMT returns per itemizing return, capped at 1): SALT is not deductible under the AMT, so its value
#        is scaled by (1 - amt). Mortgage interest is deductible under the AMT.
#   tau = assumed federal marginal rate of an itemizer in the stub (married filing jointly, taxable income ~ AGI minus
#        deductions; TY2016 / TY2018 schedules): <$10k 0/0, $10-25k 10/10, $25-50k 15/12, $50-75k 15/12, $75-100k 15/12,
#        $100-200k 25/22, $200k+ 33/32.
# Measures ($ per TY2016 return):
#   d_salt   = sum_b [tau16 (1-amt16) S16 - tau18 (1-amt18) L18] / N1_16   lost federal value of SALT deductions (cap +
#              itemization exit + rate cuts + AMT relief) -- the measure proposed;
#   d_salt17 = same with TY2017 as base;
#   cap_bite = sum_b tau18 (1-amt18) (S18 - L18) / N1_16                  value of SALT disallowed by the cap among 2018 itemizers;
#   d_mid    = sum_b [tau16 MID16 - tau18 MID18] / N1_16                  lost value of mortgage-interest deductions;
#   d_both   = d_salt + d_mid.
# Compared with the original measure (a1: stub-average SALT above $10k per return, TY2016).
suppressPackageStartupMessages({library(data.table)})
source("code/R/paths.R"); OUT <- out_dir("build/exposure"); S <- file.path(DATA, "SOI/county_returns_w_agi")
tau <- data.table(agi_stub = 1:8, t16 = c(0, 0, .10, .15, .15, .15, .25, .33), t18 = c(0, 0, .10, .12, .12, .12, .22, .32))
rd <- function(yy) {
  x <- fread(file.path(S, sprintf("%02dincyallagi.csv", yy)), colClasses = "character")
  setnames(x, toupper(names(x)))
  num <- intersect(c("N1", "N04470", "A18425", "A18450", "A18500", "A18800", "A18460", "N09600", "A19300"), names(x))
  for (v in num) set(x, j = v, value = as.numeric(x[[v]]))
  x <- x[as.integer(COUNTYFIPS) != 0, c("STATEFIPS", "COUNTYFIPS", "AGI_STUB", num), with = FALSE]
  x[, `:=`(fips = sprintf("%02d%03d", as.integer(STATEFIPS), as.integer(COUNTYFIPS)), agi_stub = as.integer(AGI_STUB), y = 2000L + yy)]
  x[, salt := rowSums(.SD, na.rm = TRUE), .SDcols = c("A18425", "A18450", "A18500", "A18800")]
  x[, amt := pmin(1, fifelse(N04470 > 0, N09600 / N04470, 0))]   # (state-total rows: COUNTYFIPS "0" in 2016, "000" later)
  x[] }
p16 <- rd(16); p17 <- rd(17); p18 <- rd(18)
cat("rows:", nrow(p16), nrow(p17), nrow(p18), "| stubs:", paste(sort(unique(p16$agi_stub)), collapse = " "), "\n")
nat <- rbind(p16, p17, p18, fill = TRUE)[, .(returns_m = sum(N1) / 1e6, itemizers_m = sum(N04470) / 1e6, salt_bn = sum(salt) / 1e6,
  limited_bn = sum(A18460, na.rm = TRUE) / 1e6, mid_bn = sum(A19300) / 1e6, amt_share_itemizers = sum(N09600) / pmax(sum(N04470), 1)), by = .(y, agi_stub)][order(agi_stub, y)]
m <- merge(p16[, .(fips, agi_stub, N1_16 = N1, S16 = salt, amt16 = amt, MID16 = A19300)],
           p17[, .(fips, agi_stub, S17 = salt, amt17 = amt, MID17 = A19300)], by = c("fips", "agi_stub"))
m <- merge(m, p18[, .(fips, agi_stub, S18 = salt, L18 = A18460, amt18 = amt, MID18 = A19300)], by = c("fips", "agi_stub"))
m <- merge(m, tau, by = "agi_stub")
cty <- m[, .(returns16 = sum(N1_16),
  v_salt16 = sum(t16 * (1 - amt16) * S16), v_salt17 = sum(t16 * (1 - amt17) * S17), v_salt18 = sum(t18 * (1 - amt18) * L18),
  v_cap = sum(t18 * (1 - amt18) * pmax(S18 - L18, 0)), v_mid16 = sum(t16 * MID16), v_mid18 = sum(t18 * MID18),
  salt16 = sum(S16), lim18 = sum(L18), salt18 = sum(S18)), by = fips]
cty[, `:=`(d_salt = 1000 * (v_salt16 - v_salt18) / returns16, d_salt17 = 1000 * (v_salt17 - v_salt18) / returns16,
           cap_bite = 1000 * v_cap / returns16, d_mid = 1000 * (v_mid16 - v_mid18) / returns16)]
cty[, d_both := d_salt + d_mid]
old <- fread(file.path(DATA, "built/exposure/salt_exposure_county2016.csv"))[, fips := sprintf("%05d", county)]
cty <- merge(cty, old[, .(fips, excess_pp, ex_z, returns)], by = "fips", all.x = TRUE)
z <- \(x) (x - mean(x, na.rm = TRUE)) / sd(x, na.rm = TRUE)
for (k in c("d_salt", "d_salt17", "cap_bite", "d_mid", "d_both")) cty[, (paste0(k, "_z")) := z(get(k))]
fwrite(cty[, .(fips, returns16, d_salt, d_salt17, cap_bite, d_mid, d_both, d_salt_z, d_salt17_z, cap_bite_z, d_mid_z, d_both_z)],
       file.path(DATA, "built/exposure/salt_exposure_county_delta.csv"))
sink(file.path(OUT, "c24_exposure_delta.txt"), split = TRUE)
cat("== National totals by AGI stub (1 <$1, 2 $1-10k, 3 10-25k, 4 25-50k, 5 50-75k, 6 75-100k, 7 100-200k, 8 200k+)\n")
print(nat[, lapply(.SD, \(x) if (is.numeric(x)) round(x, 2) else x)])
cat("\n== County measures ($ per 2016 return)\n")
ms <- c("excess_pp", "d_salt", "d_salt17", "cap_bite", "d_mid", "d_both")
cty[, excess_pp := 1000 * excess_pp]
print(rbindlist(lapply(ms, \(k) cty[, .(measure = k, mean = round(mean(get(k), na.rm = TRUE)), sd = round(sd(get(k), na.rm = TRUE)),
  p10 = round(quantile(get(k), .1, na.rm = TRUE)), p50 = round(median(get(k), na.rm = TRUE)), p90 = round(quantile(get(k), .9, na.rm = TRUE)),
  p99 = round(quantile(get(k), .99, na.rm = TRUE)), wtd_mean = round(weighted.mean(get(k), returns16, na.rm = TRUE)))])))
cat("\nCorrelations across counties (unweighted):\n"); print(round(cor(cty[, ..ms], use = "pairwise"), 2))
cat("\nShare of the national d_salt that is the cap bite on 2018 itemizers:", round(sum(cty$v_cap) / sum(cty$v_salt16 - cty$v_salt18), 3), "\n")
cat("National: SALT claimed 2016", round(sum(cty$salt16) / 1e6, 1), "bn | 2018 claimed (pre-limit)", round(sum(cty$salt18) / 1e6, 1), "bn | 2018 deducted", round(sum(cty$lim18) / 1e6, 1), "bn\n")
cat("\nTop 15 counties by d_salt:\n")
cn <- fread(file.path(S, "16incyallagi.csv"), select = c("STATEFIPS", "COUNTYFIPS", "COUNTYNAME", "STATE"), colClasses = "character")[, .(fips = sprintf("%02d%03d", as.integer(STATEFIPS), as.integer(COUNTYFIPS)), name = paste0(COUNTYNAME, ", ", STATE))]
cn <- unique(cn, by = "fips")
print(merge(cty, cn, by = "fips")[order(-d_salt)][1:15, .(name, d_salt = round(d_salt), cap_bite = round(cap_bite), d_mid = round(d_mid), old_excess = round(excess_pp))])
cat("\nRank changes: counties in the top decile of d_salt that are not in the top decile of the old measure:",
    cty[d_salt >= quantile(d_salt, .9, na.rm = TRUE) & excess_pp < quantile(excess_pp, .9, na.rm = TRUE), .N], "of", cty[d_salt >= quantile(d_salt, .9, na.rm = TRUE), .N], "\n")
sink()
