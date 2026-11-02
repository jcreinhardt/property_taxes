# Three house-price indices per geography x year from the sales extract of l_hedonic_sales.sql (runs on SOM inside
# l_price_indices.sbatch; R 4.3 with fixest). Usage: Rscript l_price_indices.R sales_attr.parquet tract_leaid.csv OUT_DIR
# Sample: arm's-length residential sales 2012-2024, price $10k-$20m, no new construction, no REO / foreclosure / short sale,
# tract known. Geographies g: tract, county (fips), school district (leaid via the tract crosswalk). Estimated state by state.
#   median   : median and mean log price of the sales in g x y (what tract_year.rds p50 is, in logs), n_sales.
#   repeat   : parcels with >= 2 sales in the sample; log p_ist = a_i + d_gt + e, i.e. feols(lp ~ 1 | clip + g^y); the g x y
#              effects are the index (comparable within g across years; reported minus the g mean). n_rep = sales used.
#   hedonic  : all sales; log p = X b_state + d_gt + e with X = log living area, age and age^2, log lot, bedrooms, bathrooms,
#              missing-value dummies, property-type dummies (condo, duplex, apartment vs SFR); coefficients by state, fixed
#              over time; the g x y effects are the quality-adjusted index (minus the g mean).
# Output: OUT_DIR/price_indices_{tract,county,district}_year.parquet (g, y, st, n_sales, med_lp, mean_lp, hed, n_rep, rep).
suppressPackageStartupMessages({library(data.table); library(duckdb); library(fixest)})
args <- commandArgs(TRUE); IN <- args[1]; XW <- args[2]; OUT <- args[3]; dir.create(OUT, showWarnings = FALSE, recursive = TRUE)
NT <- as.integer(Sys.getenv("SLURM_CPUS_PER_TASK", "4")); setFixest_nthreads(NT); setDTthreads(NT)
con <- dbConnect(duckdb())
d <- as.data.table(dbGetQuery(con, sprintf("SELECT clip, fips, st, tract, pic, living, lot_sqft, acres, year_built, beds, baths, y, price
  FROM '%s' WHERE NOT newcon AND NOT distress AND tract IS NOT NULL AND st IS NOT NULL AND price BETWEEN 10000 AND 20000000", IN)))
xw <- fread(XW, colClasses = "character")
d <- merge(d, xw, by = "tract", all.x = TRUE)
cat("sales:", nrow(d), "| parcels:", uniqueN(d$clip), "| states:", uniqueN(d$st), "| with district:", sum(!is.na(d$leaid)), "\n")
d[, `:=`(lp = log(price), lliv = fifelse(living >= 200 & living <= 20000, log(living), NA_real_),
         age = fifelse(year_built >= 1700 & year_built <= y, as.numeric(y - year_built), NA_real_),
         lot = fcoalesce(as.numeric(lot_sqft), as.numeric(acres) * 43560), condo = as.integer(pic == 11), dup = as.integer(pic == 21), apt = as.integer(pic == 22))]
d[, llot := fifelse(lot >= 100 & lot <= 1e8, log(lot), NA_real_)]
d[, `:=`(beds = fifelse(beds >= 0 & beds <= 12, as.numeric(beds), NA_real_), baths = fifelse(baths >= 0 & baths <= 12, as.numeric(baths), NA_real_))]
for (k in c("lliv", "age", "llot", "beds", "baths")) { d[, paste0(k, "_m") := as.integer(is.na(get(k)))]; set(d, which(is.na(d[[k]])), k, 0) }
d[, age2 := age^2 / 100]
cat("attribute coverage: living", round(1 - mean(d$lliv_m), 3), "| year built", round(1 - mean(d$age_m), 3), "| lot", round(1 - mean(d$llot_m), 3), "| beds", round(1 - mean(d$beds_m), 3), "| baths", round(1 - mean(d$baths_m), 3), "\n")
hf <- lp ~ lliv + lliv_m + age + age2 + age_m + llot + llot_m + beds + beds_m + baths + baths_m + condo + dup + apt
gcols <- c(tract = "tract", county = "fips", district = "leaid")
# Repeat-sales index: within g, years are comparable only inside a connected component of the parcel-year graph (parcels link
# the years they sold in). Label propagation over the edges between consecutive sales of a parcel; keep the component with the
# most sales per g, set the index to NA in the others (fixest's g x y effects in secondary components have their own reference).
main_component <- function(xr) { setorder(xr, clip, y); e <- xr[, .(a = y[-.N], b = y[-1]), by = .(g, clip)][a != b, .(g, a, b)]; e <- unique(e)
  nd <- unique(xr[, .(g, y)])[, lab := y]
  repeat { m <- merge(e, nd[, .(g, a = y, la = lab)], by = c("g", "a"))[nd[, .(g, b = y, lb = lab)], on = c("g", "b"), nomatch = NULL][, l := pmin(la, lb)]
    new <- rbind(m[, .(g, y = a, l)], m[, .(g, y = b, l)])[, .(l = min(l)), by = .(g, y)]
    nd <- merge(nd, new, by = c("g", "y"), all.x = TRUE)[, lab2 := pmin(lab, l, na.rm = TRUE)][, l := NULL]; ch <- nd[, sum(lab2 != lab)]; nd[, lab := lab2][, lab2 := NULL]; if (ch == 0) break }
  cs <- merge(xr[, .N, by = .(g, y)], nd, by = c("g", "y"))[, .(n = sum(N)), by = .(g, lab)][order(g, -n)][!duplicated(g), .(g, lab)]
  merge(nd, cs, by = c("g", "lab"))[, .(g, y, main = TRUE)] }
one <- function(x, g) { x <- x[!is.na(get(g))][, `:=`(g = get(g))][, gy := paste(g, y, sep = "|")]
  med <- x[, .(n_sales = .N, med_lp = median(lp), mean_lp = mean(lp)), by = .(g, y)]
  h <- tryCatch({ m <- feols(hf, x, fixef = "gy", notes = FALSE); fe <- fixef(m, notes = FALSE)$gy; data.table(gy = names(fe), hed = as.numeric(fe)) }, error = function(e) { cat("  hedonic failed:", conditionMessage(e), "\n"); NULL })
  xr <- x[, if (.N >= 2) .SD, by = clip]
  r <- if (nrow(xr) >= 20 && uniqueN(xr$gy) >= 2) tryCatch({ m <- feols(lp ~ 1 | clip + gy, xr, notes = FALSE); fe <- fixef(m, notes = FALSE)$gy
      rr <- merge(data.table(gy = names(fe), rep = as.numeric(fe)), xr[, .(n_rep = .N), by = gy], by = "gy")
      mc <- main_component(xr[, .(g, clip, y)])[, gy := paste(g, y, sep = "|")]; rr <- merge(rr, mc[, .(gy, main)], by = "gy", all.x = TRUE)
      rr[is.na(main), rep := NA_real_][, main := NULL] }, error = function(e) { cat("  repeat failed:", conditionMessage(e), "\n"); NULL }) else NULL
  med[, gy := paste(g, y, sep = "|")]
  if (!is.null(h)) med <- merge(med, h, by = "gy", all.x = TRUE) else med[, hed := NA_real_]
  if (!is.null(r)) med <- merge(med, r, by = "gy", all.x = TRUE) else med[, `:=`(rep = NA_real_, n_rep = NA_integer_)]
  med[, `:=`(hed = hed - mean(hed, na.rm = TRUE), rep = rep - mean(rep, na.rm = TRUE)), by = g]   # within-g level is arbitrary
  med[, gy := NULL]; med }
res <- list(tract = list(), county = list(), district = list())
for (s in sort(unique(d$st))) { x <- d[st == s]; t0 <- Sys.time()
  for (lv in names(gcols)) { r <- one(x, gcols[lv]); if (!is.null(r)) res[[lv]][[s]] <- r[, st := s] }
  cat(sprintf("%s: %s sales, %s parcels, %.1f min\n", s, format(nrow(x), big.mark = ","), format(uniqueN(x$clip), big.mark = ","), as.numeric(difftime(Sys.time(), t0, units = "mins")))) }
for (lv in names(gcols)) { out <- rbindlist(res[[lv]]); setnames(out, "g", gcols[lv]); setcolorder(out, c(gcols[lv], "y", "st", "n_sales", "med_lp", "mean_lp", "hed", "n_rep", "rep"))
  duckdb_register(con, "out", out); dbExecute(con, sprintf("COPY out TO '%s/price_indices_%s_year.parquet' (FORMAT parquet)", OUT, lv)); duckdb_unregister(con, "out")
  cat(lv, ": ", nrow(out), " unit-years | hedonic ", sum(is.finite(out$hed)), " | repeat ", sum(is.finite(out$rep)), "\n", sep = "") }
dbDisconnect(con, shutdown = TRUE)
