# Formerly code/analysis/salt_local/c26_exposure_components.R (moved in the 2026-10-27 restructuring).
# Exposure components at county, state and ZIP level (TY2016 -> TY2018 change in the federal value of SALT deductions, as c24),
# split by tax type and re-based to homeowners.
# Per geography g and AGI stub b (SOI; amounts $1,000):
#   S = A18425 + A18450 + A18500 + A18800 (SALT claimed; pre-limit in TY2018), P = A18500 (real estate taxes), L18 = A18460.
#   amt = N09600 / N04470 (capped at 1); tau = assumed MFJ marginal rates (c24; ZIP stubs: $1-25k 10/10, 25-50k 15/12,
#   50-75k 15/12, 75-100k 15/12, 100-200k 25/22, 200k+ 33/32).
#   lost value  = tau16 (1-amt16) S16 - tau18 (1-amt18) L18
#   property part: tau16 (1-amt16) P16 - tau18 (1-amt18) L18 x P18/S18 (the deducted amount allocated pro rata to real estate
#   taxes); non-property part (state/local income or sales tax) = lost value - property part.
#   owner attribution: the stub's lost value x (N18500 / N04470, TY2016), i.e. the share of itemizers claiming real estate tax.
# Measures: per 2016 return (d_salt, d_prop, d_nonprop) and per owner-occupied household (ACS 2013-17: d_salt_own, d_prop_own,
# d_nonprop_own = owner-attributed lost value / owner households).
# Mortgage interest (A19300; deductible under the AMT): mid loss = tau16 MID16 - tau18 MID18, attributed fully to owners
# (d_mid per return, d_mid_own per owner household).
# ZIP censoring: SOI sets counts < 20 to 0. A ZIP is "clean" if every stub has N1 > 0 in both years and the $100-200k and
# $200k+ stubs have itemizers > 0 in both years.
suppressPackageStartupMessages({library(data.table); library(tidycensus)})
source("code/R/paths.R"); OUT <- out_dir("build/exposure"); SO <- file.path(DATA, "SOI")
taus <- list(county = data.table(agi_stub = 1:8, t16 = c(0, 0, .10, .15, .15, .15, .25, .33), t18 = c(0, 0, .10, .12, .12, .12, .22, .32)),
             zip = data.table(agi_stub = 1:6, t16 = c(.10, .15, .15, .15, .25, .33), t18 = c(.10, .12, .12, .12, .22, .32)))
rd <- function(level, yy) {
  f <- if (level == "county") file.path(SO, sprintf("county_returns_w_agi/%02dincyallagi.csv", yy)) else file.path(SO, sprintf("zipcode_returns_w_agi/%02dzpallagi.csv", yy))
  x <- fread(f, colClasses = "character", encoding = "Latin-1"); setnames(x, toupper(names(x)))
  num <- intersect(c("N1", "N04470", "N18500", "A18425", "A18450", "A18500", "A18800", "A18460", "N09600", "A19300"), names(x))
  for (v in num) set(x, j = v, value = as.numeric(x[[v]]))
  if (level == "county") { x <- x[as.integer(COUNTYFIPS) != 0][, geo := sprintf("%02d%03d", as.integer(STATEFIPS), as.integer(COUNTYFIPS))] }
  else { x <- x[!ZIPCODE %in% c("0", "00000", "99999")][, geo := sprintf("%05d", as.integer(ZIPCODE))] }
  x[, `:=`(st = sprintf("%02d", as.integer(STATEFIPS)), agi_stub = as.integer(AGI_STUB))]
  x[, S := rowSums(.SD, na.rm = TRUE), .SDcols = c("A18425", "A18450", "A18500", "A18800")]
  x[, amt := pmin(1, fifelse(N04470 > 0, N09600 / N04470, 0))]
  x[, own := pmin(1, fifelse(N04470 > 0, N18500 / N04470, 0))]
  x[, c("geo", "st", "agi_stub", "N1", "N04470", "S", "A18500", intersect("A18460", names(x)), "amt", "own", "A19300"), with = FALSE] }
build <- function(level) {
  a <- rd(level, 16); b <- rd(level, 18)
  m <- merge(a[, .(geo, st, agi_stub, N1_16 = N1, it16 = N04470, S16 = S, P16 = A18500, amt16 = amt, own16 = own, M16 = A19300)],
             b[, .(geo, agi_stub, N1_18 = N1, it18 = N04470, S18 = S, P18 = A18500, L18 = A18460, amt18 = amt, M18 = A19300)], by = c("geo", "agi_stub"))
  m <- merge(m, taus[[level]], by = "agi_stub")
  m[, `:=`(v16 = t16 * (1 - amt16) * S16, v18 = t18 * (1 - amt18) * L18,
           p16 = t16 * (1 - amt16) * P16, p18 = t18 * (1 - amt18) * L18 * fifelse(S18 > 0, P18 / S18, 0))]
  m[, `:=`(loss = v16 - v18, ploss = p16 - p18, mloss = t16 * M16 - t18 * M18)]
  nst <- max(m$agi_stub)
  g <- m[, .(st = st[1], returns16 = sum(N1_16), loss = sum(loss), ploss = sum(ploss), loss_own = sum(loss * own16), ploss_own = sum(ploss * own16), mloss = sum(mloss),
             clean = all(N1_16 > 0 & N1_18 > 0) & all(it16[agi_stub >= nst - 1] > 0 & it18[agi_stub >= nst - 1] > 0),
             share_ret_top = sum(N1_16[agi_stub == nst]) / sum(N1_16)), by = geo]
  g[, `:=`(d_salt = 1000 * loss / returns16, d_prop = 1000 * ploss / returns16, d_nonprop = 1000 * (loss - ploss) / returns16, d_mid = 1000 * mloss / returns16)]
  g[] }
cty <- build("county"); zip <- build("zip")
# owner-occupied households, ACS 2013-17
acs <- fread(file.path(DATA, "external/acs/acs5_tract_2017.csv"), colClasses = c(GEOID = "character"))
own_c <- acs[, .(owners = sum(ownE, na.rm = TRUE), hh = sum(ten_totE, na.rm = TRUE)), by = .(geo = substr(GEOID, 1, 5))]
zf <- file.path(DATA, "external/acs/acs5_zcta_2017.csv")
if (!file.exists(zf)) fwrite(as.data.table(get_acs("zcta", variables = c(own = "B25003_002", ten = "B25003_001"), year = 2017, output = "wide")), zf)
own_z <- fread(zf, colClasses = c(GEOID = "character"))[, .(geo = substr(GEOID, nchar(GEOID) - 4, nchar(GEOID)), owners = ownE, hh = tenE)]
cty <- merge(cty, own_c, by = "geo", all.x = TRUE); zip <- merge(zip, own_z, by = "geo", all.x = TRUE)
per_own <- \(g) g[owners > 0, `:=`(d_salt_own = 1000 * loss_own / owners, d_prop_own = 1000 * ploss_own / owners,
                                   d_nonprop_own = 1000 * (loss_own - ploss_own) / owners, d_mid_own = 1000 * mloss / owners)][]
cty <- per_own(cty); zip <- per_own(zip)
stt <- cty[, .(returns16 = sum(returns16), loss = sum(loss), ploss = sum(ploss), loss_own = sum(loss_own), ploss_own = sum(ploss_own), mloss = sum(mloss),
               owners = sum(owners, na.rm = TRUE)), by = .(geo = st)]
stt[, `:=`(d_salt = 1000 * loss / returns16, d_prop = 1000 * ploss / returns16, d_nonprop = 1000 * (loss - ploss) / returns16,
           d_salt_own = 1000 * loss_own / owners, d_prop_own = 1000 * ploss_own / owners, d_nonprop_own = 1000 * (loss_own - ploss_own) / owners,
           d_mid = 1000 * mloss / returns16, d_mid_own = 1000 * mloss / owners)]
keep <- c("geo", "returns16", "owners", "d_salt", "d_prop", "d_nonprop", "d_mid", "d_salt_own", "d_prop_own", "d_nonprop_own", "d_mid_own")
fwrite(cty[, ..keep], file.path(DATA, "built/exposure/exposure_components_county.csv"))
fwrite(stt[, ..keep], file.path(DATA, "built/exposure/exposure_components_state.csv"))
fwrite(zip[, c(keep, "clean", "st"), with = FALSE], file.path(DATA, "built/exposure/exposure_components_zip.csv"))
ms <- c("d_salt", "d_prop", "d_nonprop", "d_mid", "d_salt_own", "d_prop_own", "d_nonprop_own", "d_mid_own")
sm <- \(g, lab) rbindlist(lapply(ms, \(k) g[is.finite(get(k)), .(level = lab, measure = k, n = .N, mean = round(mean(get(k))), sd = round(sd(get(k))),
  p50 = round(median(get(k))), p90 = round(quantile(get(k), .9)), wtd = round(weighted.mean(get(k), returns16)))]))
sink(file.path(OUT, "c26_exposure_components.txt"), split = TRUE)
cat("Lost federal value of SALT deductions, TY2016 -> TY2018, $ per 2016 return or per owner household (ACS 2013-17)\n")
print(rbind(sm(cty, "county"), sm(stt, "state"), sm(zip, "zip (all)"), sm(zip[clean == TRUE], "zip (clean)")))
cat("\nNational: property-tax share of the lost value", round(sum(cty$ploss) / sum(cty$loss), 3), "| owner-attributed share", round(sum(cty$loss_own) / sum(cty$loss), 3), "\n")
cat("ZIPs:", nrow(zip), "| clean:", zip[clean == TRUE, .N], sprintf("(%.1f%% of 2016 returns)", 100 * zip[clean == TRUE, sum(returns16)] / sum(zip$returns16)),
    "| with ACS owners:", zip[owners > 0, .N], "\n")
cat("County correlations:\n"); print(round(cor(cty[, ..ms], use = "pairwise"), 2))
cat("\nStates, sorted by d_salt_own:\n"); print(stt[order(-d_salt_own), .(st = geo, d_salt = round(d_salt), d_prop = round(d_prop), d_nonprop = round(d_nonprop), d_salt_own = round(d_salt_own), d_prop_own = round(d_prop_own))])
sink()
