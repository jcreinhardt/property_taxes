# 01_prepare.R — turn the Bouchet exports (parcels, panel, mortgages, da_addr) into the analysis data set.
# Adds: mortgage stock and 2016 interest (Section 3.4), refi / cash-out outcomes (Section 2), imputed AGI (Section 3.6),
# CBSA fallback, value bins, cluster ids. Writes data/salt_cap/panel/analysis_parcels.parquet and analysis_panel.parquet.
source("code/analysis/salt_cap/R/00_setup.R")
library(readxl)

parcels <- dq(sprintf("SELECT * FROM read_parquet('%s')", pq("parcels")))
panel   <- dq(sprintf("SELECT * FROM read_parquet('%s')", pq("panel")))
mort    <- dq(sprintf("SELECT * FROM read_parquet('%s')", pq("mortgages")))
cat("parcels", nrow(parcels), " panel", nrow(panel), " mortgages", nrow(mort), "\n")

# ---- 1. State match-rate screen (Section 3.1) ---------------------------------------------------------------------------
mr <- fread(pcsv("diag_match_rates.csv"))
# 2016 match rate among the base sample, where "owner-occupied" is taken from the two non-Data-Axle signals (homestead flag or
# mailing = situs) so that the rate is not mechanically tied to the surname signal. The all-parcel rate is kept for the appendix.
mr16 <- parcels[!corp & base_y >= 2015 & (hs16 | mh16), .(match_rate = mean(da16), n = .N), by = st][order(match_rate)]
mr16_all <- mr[y == 2016, .(match_rate_all = weighted.mean(match_rate, n_parcels), n_all = sum(n_parcels)), by = st]
mr16 <- merge(mr16, mr16_all, by = "st", all.x = TRUE)[order(match_rate)]
drop_states <- mr16[match_rate < 0.70, st]
fwrite(mr16, file.path(OUT, "table_match_rate_by_state_2016.csv"))
fwrite(dcast(mr[, .(match_rate = weighted.mean(match_rate, n_parcels)), by = .(st, y)], st ~ y, value.var = "match_rate"),
       file.path(OUT, "table_match_rate_by_state_year.csv"))
cat("states dropped (2016 match < 70%):", paste(drop_states, collapse = ", "), "\n")
parcels[, state_ok := !(st %in% drop_states) & !(st %in% c("GU", "PR", "VI"))]

# ---- 2. Mortgage stock, mortint16, refi and cash-out (Sections 3.4 and 2) -------------------------------------------------
pmms <- fread(file.path(DATA, "external/MORTGAGE30US.csv"))
setnames(pmms, c("date", "rate")); pmms[, date := as.IDate(date)]
pmms <- pmms[!is.na(rate), .(pmms = mean(rate) / 100), by = .(ym = format(date, "%Y-%m"))]
mort[, mdate := as.IDate(mdate)]
mort <- mort[mtype %in% c("P", "R") | refi_ind | is.na(mtype)]
mort[, ym := format(mdate, "%Y-%m")]
mort <- merge(mort, pmms, by = "ym", all.x = TRUE)
mort[, r := fifelse(!is.na(irate) & irate >= 1 & irate <= 15, irate / 100, pmms)]
mort[is.na(r), r := median(pmms$pmms)]
mort[, is_refi := (mtype == "R" & !is.na(mtype)) | refi_ind]
mort[, is_purch := mtype == "P" & !is.na(mtype) & !refi_ind]
# remaining balance of a 30-year fixed loan after k months
bal30 <- function(P, r, k) { i <- r / 12; n <- 360; k <- pmax(pmin(k, n), 0); L <- max(length(P), length(i), length(k))
  P <- rep_len(P, L); i <- rep_len(i, L); k <- rep_len(k, L)          # ifelse() truncates to the length of the test; recycle explicitly
  ifelse(i > 0, P * ((1 + i)^n - (1 + i)^k) / ((1 + i)^n - 1), P * (1 - k / n)) }
# interest paid during calendar year Y on a loan originated at mdate (sum of monthly interest)
int_year <- function(P, r, mdate, Y) {
  m0 <- 12 * (Y - as.integer(format(mdate, "%Y"))) + (1 - as.integer(format(mdate, "%m")))  # months elapsed at Jan Y
  sapply(seq_along(P), function(j) { ks <- m0[j] + 0:11; ks <- ks[ks >= 0 & ks < 360]; if (!length(ks)) 0 else sum(bal30(P[j], r[j], ks) * r[j] / 12) })
}
setorder(mort, clip, mdate)
# latest first-lien loan by end-2016 (from 2000)
m16 <- mort[mdate >= as.IDate("2000-01-01") & mdate <= as.IDate("2016-12-31")][, .SD[.N], by = clip]
m16[, mortint16 := int_year(amt, r, mdate, 2016L)]
m16[, mortbal16 := bal30(amt, r, 12 * (2016 - as.integer(format(mdate, "%Y"))) + (1 - as.integer(format(mdate, "%m"))))]
parcels <- merge(parcels, m16[, .(clip, mort_amt = amt, mort_date = mdate, mort_rate = r, mort_refi = is_refi, mortint16, mortbal16)], by = "clip", all.x = TRUE)
parcels[is.na(mortint16), `:=`(mortint16 = 0, mortbal16 = 0)]
# refi events with cash-out test against the previous loan's imputed balance
mort[, `:=`(prev_amt = shift(amt), prev_r = shift(r), prev_date = shift(mdate)), by = clip]
mort[, prev_bal := bal30(prev_amt, prev_r, as.integer((mdate - prev_date) / 30.44))]
mort[, cashout := is_refi & !is.na(prev_bal) & amt >= 1.05 * prev_bal]
mort[, cashout_cl := is_refi & purpose == "A" & !is.na(purpose)]
refi_y <- mort[is_refi == TRUE, .(refi = 1L, cashout = as.integer(any(cashout)), cashout_cl = as.integer(any(cashout_cl)), refi_amt = max(amt)), by = .(clip, y = as.integer(format(mdate, "%Y")))]
panel <- merge(panel, refi_y, by = c("clip", "y"), all.x = TRUE)
panel[is.na(refi), `:=`(refi = 0L, cashout = 0L, cashout_cl = 0L)]
cat("refi events", sum(panel$refi), " cash-out (balance rule)", sum(panel$cashout), " cash-out (CoreLogic code)", sum(panel$cashout_cl), "\n")

# ---- 3. CBSA fallback, geography, clusters -----------------------------------------------------------------------------
cw <- as.data.table(read_excel(file.path(DATA, "external/list1_Sep_2018.xls"), skip = 2))
cw <- cw[, .(fips = as.integer(paste0(`FIPS State Code`, `FIPS County Code`)), cbsa_cw = as.integer(`CBSA Code`))][!is.na(fips)]
parcels <- merge(parcels, cw, by = "fips", all.x = TRUE)
parcels[, cbsa_use := fifelse(!is.na(cbsa) & cbsa != "", suppressWarnings(as.integer(cbsa)), cbsa_cw)]
parcels[is.na(cbsa_use), cbsa_use := -as.integer(factor(st))]     # non-metro parcels: state as the "CBSA"
parcels[, juris := paste(fips, fifelse(is.na(tac) | tac == "", fifelse(is.na(muni_code) | muni_code == "", "", muni_code), tac), sep = "_")]
parcels[, county := fips]

# ---- 4. Imputed 2016 AGI (Section 3.6) ---------------------------------------------------------------------------------
soi <- dq(sprintf("SELECT zipcode, agi_stub, returns n1, returns_w_items n_item, property_taxes_returns n_ptax, taxes_paid_returns n_salt, taxes_paid_amount a_salt, individuals n2
                   FROM read_parquet('%s') WHERE year = 2016", SOI_PQ))
# AGI bin bounds (SOI ZIP stubs 2016): 1: <25k, 2: 25-50k, 3: 50-75k, 4: 75-100k, 5: 100-200k, 6: 200k+
bounds <- data.table(agi_stub = 1:6, lo = c(1, 25, 50, 75, 100, 200) * 1e3, hi = c(25, 50, 75, 100, 200, 600) * 1e3)
cpi <- data.table(year = 1995:2016, cpi = c(152.4,156.9,160.5,163.0,166.6,172.2,177.1,179.9,184.0,188.9,195.3,201.6,207.3,215.3,214.5,218.1,224.9,229.6,233.0,236.7,237.0,240.0))
parcels[, buy_year := as.integer(format(as.IDate(buy_date), "%Y"))]
parcels <- merge(parcels, cpi, by.x = "buy_year", by.y = "year", all.x = TRUE)
parcels[, lv := log(pmax(value16, 1e3))]
parcels[, lp := fifelse(!is.na(buy_price) & buy_price > 1e4 & !is.na(cpi), log(buy_price * 240.0 / cpi), NA_real_)]
parcels[, lm := fifelse(!is.na(mort_amt) & mort_amt > 1e4, log(mort_amt), NA_real_)]
# income index: standardised average of the available components (value always present)
parcels[, idx := rowMeans(cbind(scale(lv), scale(lp), scale(lm)), na.rm = TRUE)]
parcels[, zip5 := sprintf("%05d", suppressWarnings(as.integer(zip5)))]
setorder(parcels, zip5, idx)
parcels[, u := (frank(idx, ties.method = "random") - 0.5) / .N, by = zip5]   # within-ZIP rank
soi[, zip5 := sprintf("%05d", zipcode)]
soi_w <- dcast(soi, zip5 ~ agi_stub, value.var = "n1", fill = 0); setnames(soi_w, as.character(1:6), paste0("n", 1:6))
soi_w[, tot := n1 + n2 + n3 + n4 + n5 + n6]
soi_w <- soi_w[tot > 0]
for (k in 1:6) soi_w[, paste0("c", k) := Reduce(`+`, mget(paste0("n", 1:k))) / tot]
parcels <- merge(parcels, soi_w[, c("zip5", paste0("c", 1:6)), with = FALSE], by = "zip5", all.x = TRUE)
parcels[, agi_bin := fifelse(is.na(c6), NA_integer_, 1L + (u > c1) + (u > c2) + (u > c3) + (u > c4) + (u > c5))]
parcels <- merge(parcels, bounds, by.x = "agi_bin", by.y = "agi_stub", all.x = TRUE)
parcels[, cum_lo := fcase(agi_bin == 1, 0, agi_bin == 2, c1, agi_bin == 3, c2, agi_bin == 4, c3, agi_bin == 5, c4, agi_bin == 6, c5)]
parcels[, cum_hi := fcase(agi_bin == 1, c1, agi_bin == 2, c2, agi_bin == 3, c3, agi_bin == 4, c4, agi_bin == 5, c5, agi_bin == 6, 1)]
parcels[, agi_imp := lo + (hi - lo) * pmin(pmax((u - cum_lo) / pmax(cum_hi - cum_lo, 1e-6), 0), 1)]
parcels[, c("lo", "hi", "cum_lo", "cum_hi", "cpi", "lv", "lp", "lm") := NULL]
cat("imputed AGI: ", sum(!is.na(parcels$agi_imp)), "parcels; bin shares:", round(prop.table(table(parcels$agi_bin)), 3), "\n")

# ---- 5. Value bins (50 within CBSA), deciles, analysis flags -------------------------------------------------------------
parcels[, base_oo := !corp & oo16 & state_ok & pic %in% c(10, 11) & !is.na(bill16) & bill16 > 0 & !is.na(value16) & value16 > 0]
parcels[, base_inv := !corp & inv16 & state_ok & pic %in% c(10, 11) & !is.na(bill16) & bill16 > 0 & !is.na(value16) & value16 > 0]
parcels[base_oo | base_inv, valuebin := qbin(value16, cbsa_use, 50)]
parcels[base_oo | base_inv, valuebin_price := fifelse(!is.na(price1416), qbin(price1416, cbsa_use, 50), NA_real_)]
parcels[, mortint_dec := fifelse(mortint16 <= 0, 0L, as.integer(pmin(ceiling(10 * frank(mortint16, ties.method = "average") / .N), 10L)))]
parcels[, rate16_pp := 100 * rate16]
parcels[, lbill16 := log(bill16)]
parcels[, married := coowner %in% c("married", "likely_married")]

# ---- 6. Write ------------------------------------------------------------------------------------------------------------
con <- dbConnect(duckdb::duckdb()); on.exit(dbDisconnect(con, shutdown = TRUE))
duckdb_register(con, "parcels", parcels); duckdb_register(con, "panel", panel)
dbExecute(con, sprintf("COPY parcels TO '%s' (FORMAT parquet)", pq("analysis_parcels")))
dbExecute(con, sprintf("COPY panel TO '%s' (FORMAT parquet)", pq("analysis_panel")))
cat("written", pq("analysis_parcels"), pq("analysis_panel"), "\n")
