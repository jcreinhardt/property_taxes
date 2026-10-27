# 00_setup.R — paths, packages and helpers shared by all SALT-cap scripts. Source from any script in code/analysis/salt_cap/R.
suppressPackageStartupMessages({
  library(data.table); library(fixest); library(ggplot2); library(duckdb); library(DBI)
})
ROOT <- normalizePath(file.path(dirname(normalizePath(sys.frame(1)$ofile %||% "code/archive/2026-10-household-salt-cap/R/00_setup.R")), "../../../.."), mustWork = FALSE)
if (!dir.exists(file.path(ROOT, "output"))) ROOT <- getwd()
if (!dir.exists(file.path(ROOT, "output"))) ROOT <- "/Users/jr2728/PhD/projects/property_taxes"
if (nzchar(Sys.getenv("SALT_ROOT"))) ROOT <- Sys.getenv("SALT_ROOT")
DATA <- file.path(ROOT, "data/salt_cap")            # git-ignored: panel.parquet, parcels.parquet, mortgages.parquet, external/
OUT  <- file.path(ROOT, "output/archive/2026-10-household-salt-cap/salt_cap") # decision_log.md, estimates.csv, assumption_ledger.csv, figures/, report.md
`%||%` <- function(a, b) if (is.null(a)) b else a
SAMPLE <- Sys.getenv("SALT_SAMPLE", "bld1pct")      # label written to estimates.csv; non-default samples get their own sub-folders
PANEL_DIR <- if (SAMPLE == "bld1pct") file.path(DATA, "panel") else file.path(DATA, "panel", SAMPLE)
if (nzchar(Sys.getenv("SALT_PANEL_DIR"))) PANEL_DIR <- Sys.getenv("SALT_PANEL_DIR")
OUT_S <- if (SAMPLE == "bld1pct") OUT else file.path(OUT, SAMPLE)   # tables, figures and ledger of a non-default sample
FIG  <- file.path(OUT_S, "figures"); dir.create(FIG, showWarnings = FALSE, recursive = TRUE)

# ---- DuckDB helper: read parquet with a query -----------------------------------------------------------------------
dq <- function(sql) { con <- dbConnect(duckdb::duckdb()); on.exit(dbDisconnect(con, shutdown = TRUE)); setDT(dbGetQuery(con, sql)) }
pq <- function(name) file.path(PANEL_DIR, paste0(name, ".parquet"))
pcsv <- function(name) file.path(PANEL_DIR, name)

# Parallel runs: with SALT_PART=<label> each process writes its own part files (merged by 99_report_tables.R), avoiding write races.
part_file <- function(kind) { d <- file.path(OUT_S, "parts"); dir.create(d, showWarnings = FALSE, recursive = TRUE)
  file.path(d, sprintf("%s_%s_%s.csv", kind, Sys.getenv("SALT_PART"), SAMPLE)) }
# ---- estimate and ledger writers ---------------------------------------------------------------------------------------
add_estimate <- function(strategy, outcome, coef, se, N, clusters, spec_id, sample = SAMPLE) {
  row <- data.table(strategy = strategy, outcome = outcome, sample = sample, coef = signif(coef, 6), se = signif(se, 6),
                    N = N, clusters = clusters, spec_id = spec_id)
  f <- if (nzchar(Sys.getenv("SALT_PART"))) part_file("estimates") else file.path(OUT, "estimates.csv")
  if (!file.exists(f)) fwrite(row[0], f); old <- fread(f)
  old <- old[!(strategy == row$strategy & outcome == row$outcome & sample == row$sample & spec_id == row$spec_id)]
  fwrite(rbind(old, row, fill = TRUE), f); invisible(row)
}
add_ledger <- function(strategy, assumption, test, result, verdict = c("pass", "fail", "inconclusive", "untestable")) {
  verdict <- match.arg(verdict)
  row <- data.table(strategy = strategy, assumption = assumption, test = test, result = result, verdict = verdict)
  f <- if (nzchar(Sys.getenv("SALT_PART"))) part_file("ledger") else file.path(OUT_S, "assumption_ledger.csv")
  if (!file.exists(f)) fwrite(row[0], f); old <- fread(f)
  old <- old[!(strategy == row$strategy & assumption == row$assumption & test == row$test)]
  fwrite(rbind(old, row, fill = TRUE), f); invisible(row)
}

# ---- event-study helpers ----------------------------------------------------------------------------------------------
REF_YEAR <- 2016L
# Pull event-study coefficients (treatment x year) from a fixest object into a data.table
es_coefs <- function(m, pattern = "year::(\\d{4}):") {
  ct <- as.data.table(coeftable(m), keep.rownames = "term")
  setnames(ct, c("term", "coef", "se", "t", "p"))
  ct <- ct[grepl("year::", term)]
  ct[, year := as.integer(sub(".*year::(\\d{4}).*", "\\1", term))]
  rbind(ct[, .(year, coef, se)], data.table(year = REF_YEAR, coef = 0, se = 0))[order(year)]
}
# Post-period average effect and pre-period joint test (2012-2015 vs 2016; 2017 excluded)
es_summary <- function(m, pre = 2012:2015, post = 2018:2019) {
  cf <- coef(m); V <- vcov(m); nm <- names(cf)
  yr <- as.integer(sub(".*year::(\\d{4}).*", "\\1", nm)); yr[!grepl("year::", nm)] <- NA
  w_post <- as.numeric(yr %in% post) / sum(yr %in% post, na.rm = TRUE); w_post[is.na(yr)] <- 0
  post_coef <- sum(w_post * cf); post_se <- sqrt(as.numeric(t(w_post) %*% V %*% w_post))
  ipre <- which(yr %in% pre)
  pre_F <- if (length(ipre)) { b <- cf[ipre]; Vb <- V[ipre, ipre, drop = FALSE]; as.numeric(t(b) %*% solve(Vb) %*% b) / length(ipre) } else NA_real_
  pre_p <- if (!is.na(pre_F)) pf(pre_F, length(ipre), 1e6, lower.tail = FALSE) else NA_real_
  pre_mean_abs <- if (length(ipre)) mean(abs(cf[ipre])) else NA_real_
  list(post_coef = post_coef, post_se = post_se, pre_F = pre_F, pre_p = pre_p, pre_df = length(ipre), pre_mean_abs = pre_mean_abs)
}
# Pre-trend verdict as pre-specified (decision log 2026-10-16): pass if p >= 0.05; fail if p < 0.05 and the mean absolute pre-period
# coefficient exceeds 25% of the absolute post effect; inconclusive otherwise (statistically detectable but economically small pre-trend).
verdict_pre <- function(pre_p, pre_mean_abs, post_coef) {
  if (is.na(pre_p)) "inconclusive" else if (pre_p >= 0.05) "pass" else if (!is.na(pre_mean_abs) && pre_mean_abs > 0.25 * abs(post_coef)) "fail" else "inconclusive" }
pre_text <- function(s) sprintf("F=%.2f p=%.3f, mean|pre|=%.3g vs post %.4g (SE %.4g)", s$pre_F, s$pre_p, s$pre_mean_abs, s$post_coef, s$post_se)
es_plot <- function(dt, title, ylab, file, colour_var = NULL) {
  dt <- copy(dt); dt[, period := fifelse(year == 2017, "transition", fifelse(year < 2017, "pre", "post"))]
  p <- ggplot(dt, aes(year, coef)) +
    annotate("rect", xmin = 2016.5, xmax = 2017.5, ymin = -Inf, ymax = Inf, alpha = 0.08) +
    geom_hline(yintercept = 0, colour = "grey50") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey40") +
    geom_errorbar(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), width = 0.2, colour = "#4C6EF5") +
    geom_point(colour = "#4C6EF5", size = 2) +
    { if (!is.null(colour_var)) list() else list() } +
    labs(title = title, x = NULL, y = ylab, caption = "Reference year 2016; 2017 (shaded) is the transition year. 95% CIs.") +
    scale_x_continuous(breaks = seq(2010, 2026, 2)) + theme_minimal(base_size = 11)
  if (!is.null(colour_var)) p <- p + aes(colour = .data[[colour_var]], group = .data[[colour_var]]) + facet_wrap(colour_var)
  ggsave(file, p, width = 7, height = 4, dpi = 150, bg = "white"); invisible(p)
}
# Within-CBSA quantile bins
qbin <- function(x, g, k = 50) { r <- ave(x, g, FUN = function(v) if (all(is.na(v))) NA_real_ else (frank(v, ties.method = "average", na.last = "keep") - 0.5) / sum(!is.na(v)))
  pmin(ceiling(r * k), k) }
cat("setup ok: ROOT =", ROOT, " sample =", SAMPLE, "\n")

# SOI ZIP x AGI-bin table (exported from data/database.db) and ACS SALT imputation rows for 2016
SOI_PQ <- file.path(DATA, "external/soi_zip_agi.parquet"); ACS_PQ <- file.path(DATA, "external/acs_salt_2016.parquet")
