# 99_report_tables.R — builds the comparison table (strategies x outcomes) and the ledger table as markdown for report.md.
source("code/analysis/salt_cap/R/00_setup.R")
# merge part files written by parallel runs (parts override main rows with the same key)
merge_parts <- function(main, kind, key) { old <- fread(main); parts <- list.files(file.path(OUT_S, "parts"), pattern = paste0("^", kind, "_"), full.names = TRUE)
  if (length(parts)) { new <- rbindlist(lapply(parts, fread), fill = TRUE); new <- new[!duplicated(new, by = key, fromLast = TRUE)]  # later part files win
    old <- rbind(old[!new, on = key], new, fill = TRUE); fwrite(old, main) }; old }
est <- merge_parts(file.path(OUT, "estimates.csv"), "estimates", c("strategy", "outcome", "sample", "spec_id"))
led <- merge_parts(file.path(OUT_S, "assumption_ledger.csv"), "ledger", c("strategy", "assumption", "test"))
# Re-derive pre-trend verdicts for S1/S2/S3 rows with the pre-specified magnitude rule (their scripts ran with the p-value-only rule)
spec_of <- function(strategy, treat) switch(strategy, S1 = "S1_base", S2 = if (treat == "above16") "S2_above" else "S2_logbill", S3 = if (treat == "rate16_pp") "S3_rate" else "S3_above", NA)
for (i in which(led$strategy %in% c("S1", "S2", "S3") & grepl("Parallel trends", led$assumption))) {
  m <- regmatches(led$assumption[i], regexec("\\[([a-z_]+)(?:, ([a-z0-9_]+))?\\]", led$assumption[i], perl = TRUE))[[1]]
  oc <- m[2]; tr <- if (length(m) > 2 && nzchar(m[3])) m[3] else "E_z"; sp <- spec_of(led$strategy[i], tr)
  f <- file.path(FIG, sprintf("es_%s_%s_%s.csv", led$strategy[i], oc, sp)); if (!file.exists(f)) next
  cf <- fread(f); post <- mean(cf[year %in% 2018:2019, coef]); pre <- mean(abs(cf[year %in% 2012:2015, coef]))
  pp <- as.numeric(sub(".*p=([0-9.]+).*", "\\1", led$result[i]))
  led$verdict[i] <- verdict_pre(pp, pre, post); led$result[i] <- sprintf("%s; mean|pre|=%.3g", led$result[i], pre)
}
fwrite(led, file.path(OUT_S, "assumption_ledger.csv"))
md_table <- function(dt) { h <- paste0("| ", paste(names(dt), collapse = " | "), " |"); s <- paste0("|", paste(rep("---", ncol(dt)), collapse = "|"), "|")
  b <- apply(dt, 1, function(r) paste0("| ", paste(r, collapse = " | "), " |")); paste(c(h, s, b), collapse = "\n") }
fmt <- function(c, s) ifelse(is.na(c), "", sprintf("%.4g (%.2g)", c, s))
# main rows: one pre-specified headline spec per strategy
main_specs <- c("S1_base", "S2_above", "S3_rate", "S3_above", "S4_rate", "S5_rate_050", "S6_base", "S7_notax", "S8_pooled", "S9a_cross", "S9b_base", "S10_rate16_pp", "S13_incumbent")
cmp <- est[spec_id %in% main_specs & sample == SAMPLE]
cmp[, cell := fmt(coef, se)]
wide <- dcast(cmp, strategy + spec_id ~ outcome, value.var = "cell", fill = "")
smax <- function(x) { x <- x[!is.na(x)]; if (length(x)) max(x) else NA_real_ }
nn <- cmp[, .(N = format(max(N), big.mark = ","), clusters = as.numeric(smax(clusters))), by = .(strategy, spec_id)]
wide <- merge(wide, nn, by = c("strategy", "spec_id"))
ver <- led[, .(verdicts = paste0(sum(verdict == "pass"), " pass / ", sum(verdict == "fail"), " fail / ", sum(verdict == "inconclusive"), " inconcl. / ", sum(verdict == "untestable"), " untestable")), by = strategy]
wide <- merge(wide, ver, by = "strategy", all.x = TRUE)
setcolorder(wide, c("strategy", "spec_id", intersect(c("sale", "log_price", "exit", "exit_prim", "refi", "cashout", "to_rental", "appeal_proxy"), names(wide))))
writeLines(md_table(wide), file.path(OUT, "table_comparison.md"))
writeLines(md_table(led[, .(strategy, assumption, test, result = substr(result, 1, 110), verdict)]), file.path(OUT, "table_ledger.md"))
# full estimates table for the appendix
e2 <- copy(est)[, `:=`(coef = signif(coef, 4), se = signif(se, 3))]
writeLines(md_table(e2), file.path(OUT, "table_all_estimates.md"))
cat("tables written:", nrow(wide), "comparison rows,", nrow(led), "ledger rows,", nrow(est), "estimates\n")
