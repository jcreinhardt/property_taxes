# Is the change in border effective rates driven by tax paid (bill) or by sale prices? Same sales as R/13 (per-side sample,
# effective rate bill / price kept if 0.05-10%). In logs the decomposition is exact: mean log(eff) = mean log(bill) - mean log(price).
# (1) Gap regressions, analogous to R/13: |side A - side B| per pair-year for mean log bill, mean log price, mean log eff;
#     pair FE + year dummies, 2017 = 0, SE clustered by pair; full and balanced panel.
# (2) Level regressions: side-year mean log bill / log price / log eff on side FE + year dummies (2017 = 0), SE by pair;
#     bill and price coefficients add up to the effective-rate coefficient.
suppressPackageStartupMessages({library(data.table); library(duckdb); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- file.path(ROOT, "output/analysis/local_juris"); MIN <- 30
IN <- file.path(ROOT, "data/local_juris", Sys.getenv("BORDER_IN", "bsample1024"))
Y0 <- as.integer(Sys.getenv("BORDER_Y0", "2013")); Y1 <- as.integer(Sys.getenv("BORDER_Y1", "2024")); SFX <- Sys.getenv("BORDER_SFX", "_2013_2024")
con <- dbConnect(duckdb()); s <- as.data.table(dbGetQuery(con, sprintf("SELECT * FROM '%s/border_sample_sales.parquet'", IN))); dbDisconnect(con, shutdown = TRUE)
s <- s[y >= Y0 & y <= Y1 & price > 10000 & bill > 0 & !newcon][bill / price >= .0005 & bill / price <= .10]
side <- s[, .(n = .N, lb = mean(log(bill)), lp = mean(log(price)), le = mean(log(bill / price))), by = .(pair, st_a, y)][n >= MIN]
side[, k := seq_len(.N), by = .(pair, y)]
w <- dcast(side, pair + y ~ k, value.var = c("lb", "lp", "le"))[!is.na(lb_2)]
gaps <- w[, .(pair, y, bill = abs(lb_1 - lb_2), price = abs(lp_1 - lp_2), eff = abs(le_1 - le_2))]
gaps[, balanced := uniqueN(y) == (Y1 - Y0 + 1), by = pair]
pairs_ok <- unique(gaps$pair); side2 <- side[pair %in% pairs_ok]; side2[, sid := paste(pair, st_a)]
side2 <- merge(side2, unique(gaps[, .(pair, balanced)]), by = "pair")
est <- function(d, v, fe, lab, panel) {
  f <- feols(as.formula(sprintf("%s ~ i(y, ref = 2017) | %s", v, fe)), d, cluster = ~pair)
  ct <- as.data.table(coeftable(f), keep.rownames = "term")
  rbind(data.table(y = 2017L, coef = 0, se = 0), ct[, .(y = as.integer(sub("y::", "", term)), coef = Estimate, se = `Std. Error`)])[,
    `:=`(spec = lab, outcome = v, panel = panel, n_pairs = uniqueN(d$pair))]
}
res <- rbindlist(c(
  lapply(c("full", "balanced"), \(p) { d <- if (p == "full") gaps else gaps[balanced == TRUE]
    rbindlist(lapply(c("eff", "bill", "price"), \(v) est(d, v, "pair", "cross-border gap (abs. log difference), pair FE", p))) }),
  lapply(c("full", "balanced"), \(p) { d <- if (p == "full") side2 else side2[balanced == TRUE]
    rbindlist(lapply(c("le", "lb", "lp"), \(v) est(d, v, "sid", "side level (mean log), side FE", p))) })))
res[, outcome := factor(outcome, c("eff", "bill", "price", "le", "lb", "lp"),
                        c("effective rate", "tax bill", "sale price", "effective rate", "tax bill", "sale price"))]
fwrite(res, file.path(OUT, paste0("border_bill_price_twfe", SFX, ".csv")))
g <- ggplot(res[panel == "full"], aes(y, coef, colour = outcome, shape = outcome)) + geom_hline(yintercept = 0, colour = "grey60") +
  geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey60") +
  geom_pointrange(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), position = position_dodge(width = .5), size = .25) +
  facet_wrap(~ spec, ncol = 1, scales = "free_y") + scale_x_continuous(breaks = Y0:Y1) +
  labs(x = NULL, y = "change vs 2017 (log points)", colour = NULL, shape = NULL,
       title = "Border effective rates: tax bill vs sale price (2017 = 0)",
       subtitle = "Sales within 20 mi, per-side sample; full panel; log eff = log bill - log price; 95% CIs clustered by pair") +
  theme_minimal(base_size = 10) + theme(legend.position = "bottom")
ggsave(file.path(OUT, paste0("figures/border_bill_price_twfe", SFX, ".png")), g, width = 7.5, height = 6.8, dpi = 150)
sink(file.path(OUT, paste0("border_bill_price_twfe", SFX, ".txt")))
for (k in split(res, by = c("spec", "panel"))) {
  cat("\n==", k$spec[1], "|", k$panel[1], "| pairs:", k$n_pairs[1], "\n")
  print(as.data.frame(dcast(k, y ~ outcome, value.var = c("coef", "se"))[, lapply(.SD, round, 3)]), row.names = FALSE)
}
sink(); cat(readLines(file.path(OUT, paste0("border_bill_price_twfe", SFX, ".txt"))), sep = "\n")
