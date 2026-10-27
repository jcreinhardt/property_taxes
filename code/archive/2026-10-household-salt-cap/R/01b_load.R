# 01b_load.R — loader used by the strategy scripts: parcel x year panel joined to parcel attributes, restricted by sample.
PANEL_COLS <- "a.clip, a.y AS year, a.sale, a.log_price, a.exit, a.exit_prim, a.refi, a.cashout, a.to_rental, a.appeal_proxy, a.bill AS bill_t, a.n_persons"
PARCEL_COLS <- "p.st, p.fips AS county, p.juris, p.cbsa_use, p.zip5, p.pic, p.valuebin, p.valuebin_price, p.price1416, p.value16, p.bill16, p.lbill16, p.rate16,
           p.rate16_pp, p.above16, p.rate_ok, p.agi_imp, p.agi_bin, p.mortint16, p.mortint_dec, p.coowner, p.married, p.base_oo, p.base_inv,
           p.hs16, p.mh16, p.nm16, p.year_built, p.living_area, p.lot_sqft, p.lor16, p.age16, p.buy_year"
load_panel <- function(years = 2012:2019, which = c("oo", "inv", "both"), extra = "", extra_cols = "") {
  which <- match.arg(which)
  cond <- switch(which, oo = "p.base_oo", inv = "p.base_inv", both = "(p.base_oo OR p.base_inv)")
  d <- dq(sprintf("SELECT %s, %s %s FROM read_parquet('%s') a JOIN read_parquet('%s') p USING (clip) WHERE %s AND a.y BETWEEN %d AND %d %s",
                  PANEL_COLS, PARCEL_COLS, if (nzchar(extra_cols)) paste0(", ", extra_cols) else "", pq("analysis_panel"), pq("analysis_parcels"), cond, min(years), max(years), extra))
  d[, owner := as.integer(base_oo)]
  d[, post := as.integer(year >= 2018)]
  d[, above16 := as.integer(above16)]
  d[, lv16 := log(value16)]
  d[, yb_cat := as.integer(cut(year_built, c(-Inf, 1900, 1940, 1960, 1980, 2000, 2010, Inf)))]; d[is.na(yb_cat), yb_cat := 0L]
  d[, lla := log(pmax(living_area, 100))]; d[is.na(lla), lla := 0]
  d[, llot := log(pmax(lot_sqft, 100))]; d[is.na(llot), llot := 0]
  d[, condo := as.integer(pic == 11)]
  d[, vb := as.integer(factor(paste(cbsa_use, valuebin)))]
  d[, juris := as.integer(factor(juris))]
  for (v in c("sale", "exit", "exit_prim", "refi", "cashout", "to_rental", "appeal_proxy", "year")) if (v %in% names(d)) set(d, j = v, value = as.integer(d[[v]]))
  stopifnot(!anyNA(d$clip))   # clip is a 64-bit id: keep it as double (exact up to 2^53), never as.integer
  gc(); d
}
OUTCOMES_PRIMARY <- c("sale", "exit", "log_price")
OUTCOMES_ALL     <- c("sale", "exit", "exit_prim", "log_price", "refi", "cashout", "to_rental", "appeal_proxy")
# Generic event-study runner: Y ~ T x year | fe, returns summary and writes estimates + figure data
HEDONIC <- "lv16 + yb_cat + lla + llot + condo"
run_es <- function(d, outcome, treat, fe, cluster, strategy, spec_id, label = NULL, years_post = 2018:2019, sample = SAMPLE, fig = TRUE, weights = NULL,
                   controls = NULL, price_fe = NULL) {
  if (outcome == "log_price") {                      # hedonic controls replace the parcel FE (decision log 2026-10-16)
    if (is.null(price_fe)) fe <- sub("clip \\+ ", "", fe)
    controls <- paste(c(controls, HEDONIC), collapse = " + ")
  }
  rhs <- sprintf("i(year, %s, ref = 2016)%s", treat, if (is.null(controls)) "" else paste0(" + ", controls))
  f <- as.formula(sprintf("%s ~ %s | %s", outcome, rhs, fe))
  need <- unique(c(all.vars(f), all.vars(as.formula(cluster)), if (!is.null(weights)) all.vars(weights), "sale"))
  need <- intersect(need, names(d))
  keep <- !is.na(d[[outcome]]) & !is.na(d[[treat]]); if (outcome == "log_price") keep <- keep & d$sale == 1
  dd <- d[keep, need, with = FALSE]
  m <- feols(f, dd, cluster = cluster, weights = weights, notes = FALSE, mem.clean = TRUE)
  s <- es_summary(m, post = years_post)
  ncl <- if (is.character(cluster) || inherits(cluster, "formula")) length(unique(dd[[all.vars(as.formula(cluster))[1]]])) else NA
  add_estimate(strategy, outcome, s$post_coef, s$post_se, nobs(m), ncl, spec_id, sample)
  cf <- es_coefs(m); cf[, `:=`(strategy = strategy, outcome = outcome, spec_id = spec_id, treat = treat)]
  fwrite(cf, file.path(FIG, sprintf("es_%s_%s_%s.csv", strategy, outcome, spec_id)))
  if (fig) es_plot(cf, label %||% sprintf("%s: %s on %s", strategy, outcome, treat), sprintf("effect per unit of %s", treat),
                   file.path(FIG, sprintf("es_%s_%s_%s.png", strategy, outcome, spec_id)))
  out <- list(summary = s, coefs = cf, N = nobs(m), clusters = ncl); rm(m, dd); gc(); out
}
