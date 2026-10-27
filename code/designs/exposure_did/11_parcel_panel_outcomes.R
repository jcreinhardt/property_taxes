# Formerly code/analysis/salt_local/c2_parcel_panel.R (moved in the 2026-10-27 restructuring).
# (a) and (c) on the CoreLogic 1% building-sample parcel panel (data/built/corelogic/parcel_1pct: 949k residential parcels, 2010-2025;
# SOM extract unavailable tonight). Parcels fixed over time, so changes in bills are changes in the levy on a constant base.
# Outcomes (parcel-year, 2013-2025): log bill, log assessed value (AV), log nominal rate (bill/AV); for arm's-length sales
# (price > 10k, no new construction): log effective rate (bill/price), log assessment ratio (AV/price) and its slope on log
# price (regressivity), log price.
# Designs: (1) county SALT exposure x year, parcel FE + state x year FE, SE by county (sales outcomes: county FE + state x year,
# controlling log price for ratio and rate); (2) border DDD (tract near x high from R/26): parcel FE + pair x side x year +
# pair x distance-bin x year FE, SE by county; sales outcomes with tract FE.
suppressPackageStartupMessages({library(data.table); library(duckdb); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/exposure_did"); invisible(out_dir("designs/exposure_did/figures"))
con <- dbConnect(duckdb())
p <- as.data.table(dbGetQuery(con, sprintf("SELECT p.clip, p.y::INT y, p.bill, p.av, p.price, coalesce(p.newcon, false) newcon, lpad(q.fips::VARCHAR, 5, '0') fips, lpad(q.fips::VARCHAR, 5, '0') || lpad(q.tract::VARCHAR, 6, '0') tract
  FROM '%s/data/built/corelogic/parcel_1pct/panel.parquet' p JOIN '%s/data/built/corelogic/parcel_1pct/parcels.parquet' q USING (clip)
  WHERE p.y BETWEEN 2013 AND 2025 AND p.bill > 0 AND p.av > 0", ROOT, ROOT)))
dbDisconnect(con, shutdown = TRUE)
ex <- fread(file.path(DATA, "built/exposure/salt_exposure_county2016.csv"))[, fips := sprintf("%05d", county)]
iv <- unique(readRDS(file.path(DATA, "built/geo/iv_tract_side.rds"))[, .(tract, pair, high, dbin, near)])
p <- merge(p, ex[, .(fips, ex_z)], by = "fips")[, st := substr(fips, 1, 2)]
p[, `:=`(lb = log(bill), lav = log(av), lnom = log(bill / av))]
p <- p[lnom > log(1e-4) & lnom < log(.5)]
s <- p[!is.na(price) & price > 10000 & !newcon][, `:=`(lp = log(price), leff = log(bill / price), lratio = log(av / price))][leff > log(5e-4) & leff < log(.1)]
cat("parcel-years:", nrow(p), "| parcels:", uniqueN(p$clip), "| sales:", nrow(s), "\n")
tidy <- function(m, pat, lab) as.data.table(coeftable(m), keep.rownames = "term")[grepl(pat, term)][, .(y = as.integer(sub("y::(\\d+):.*", "\\1", term)),
  coef = Estimate, se = `Std. Error`, outcome = lab)]
# (1) exposure designs
r1 <- rbind(
  tidy(feols(lb ~ i(y, ex_z, ref = 2017) | clip + st^y, p, cluster = ~fips), "ex_z", "log bill"),
  tidy(feols(lav ~ i(y, ex_z, ref = 2017) | clip + st^y, p, cluster = ~fips), "ex_z", "log AV"),
  tidy(feols(lnom ~ i(y, ex_z, ref = 2017) | clip + st^y, p, cluster = ~fips), "ex_z", "log nominal rate"),
  tidy(feols(lp ~ i(y, ex_z, ref = 2017) | fips + st^y, s, cluster = ~fips), "ex_z", "log sale price"),
  tidy(feols(leff ~ i(y, ex_z, ref = 2017) + lp | fips + st^y, s, cluster = ~fips), "ex_z", "log effective rate (| log price)"),
  tidy(feols(lratio ~ i(y, ex_z, ref = 2017) + lp | fips + st^y, s, cluster = ~fips), "ex_z", "log assessment ratio (| log price)"),
  tidy(feols(lratio ~ i(y, I(ex_z * lp), ref = 2017) + i(y, lp, ref = 2017) | fips^lp_bin + st^y, s[, lp_bin := cut(lp, 10)], cluster = ~fips), "I\\(ex_z", "regressivity: d slope of log ratio on log price"))
r1[, design := "county exposure"]
# (2) border DDD
pb <- merge(p, iv, by = "tract")[, nh := near * high]; sb <- merge(s, iv, by = "tract")[, nh := near * high]
cat("border sample parcels:", uniqueN(pb$clip), "| near-high parcels:", uniqueN(pb[nh == 1, clip]), "| sales:", nrow(sb), "\n")
r2 <- rbind(
  tidy(feols(lb ~ i(y, nh, ref = 2017) | clip + pair^high^y + pair^dbin^y, pb, cluster = ~fips), ":nh", "log bill"),
  tidy(feols(lav ~ i(y, nh, ref = 2017) | clip + pair^high^y + pair^dbin^y, pb, cluster = ~fips), ":nh", "log AV"),
  tidy(feols(lnom ~ i(y, nh, ref = 2017) | clip + pair^high^y + pair^dbin^y, pb, cluster = ~fips), ":nh", "log nominal rate"),
  tidy(feols(lp ~ i(y, nh, ref = 2017) | tract + pair^high^y + pair^dbin^y, sb, cluster = ~fips), ":nh", "log sale price"),
  tidy(feols(leff ~ i(y, nh, ref = 2017) + lp | tract + pair^high^y + pair^dbin^y, sb, cluster = ~fips), ":nh", "log effective rate (| log price)"),
  tidy(feols(lratio ~ i(y, nh, ref = 2017) + lp | tract + pair^high^y + pair^dbin^y, sb, cluster = ~fips), ":nh", "log assessment ratio (| log price)"))
r2[, design := "border DDD (near x high)"]
res <- rbind(r1, r2); res <- rbind(res, unique(res[, .(outcome, design)])[, `:=`(y = 2017L, coef = 0, se = 0)])
fwrite(res, file.path(OUT, "c2_parcel_panel.csv"))
sink(file.path(OUT, "c2_parcel_panel.txt"))
cat("CoreLogic 1% parcel panel, 2013-2025; coefficients relative to 2017\n")
for (k in split(res, by = c("design", "outcome"))) { cat("\n==", k$design[1], "|", k$outcome[1], "\n")
  print(as.data.frame(dcast(k, . ~ y, value.var = "coef")[, -1][, lapply(.SD, round, 4)]), row.names = FALSE)
  print(as.data.frame(dcast(k, . ~ y, value.var = "se")[, -1][, lapply(.SD, round, 4)]), row.names = FALSE) }
sink(); cat(readLines(file.path(OUT, "c2_parcel_panel.txt")), sep = "\n")
g <- ggplot(res, aes(y, coef)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey60") +
  geom_pointrange(aes(ymin = coef - 1.96 * se, ymax = coef + 1.96 * se), size = .12) + facet_grid(outcome ~ design, scales = "free_y") +
  scale_x_continuous(breaks = seq(2013, 2025, 3)) + labs(x = NULL, y = "coefficient vs 2017 (exposure: per SD; border: near x high)",
  title = "CoreLogic 1% parcel panel: SALT exposure and border designs", subtitle = "95% CI clustered by county") +
  theme_minimal(base_size = 8) + theme(strip.text.y = element_text(angle = 0, hjust = 0))
ggsave(file.path(OUT, "figures/c2_parcel_panel.png"), g, width = 10, height = 11, dpi = 150)
