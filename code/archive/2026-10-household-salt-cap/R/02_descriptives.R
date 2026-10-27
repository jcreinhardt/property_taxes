# 02_descriptives.R — Section 3.7 descriptive tables + the validation tables that go into the report (Section 14.2).
source("code/archive/2026-10-household-salt-cap/R/00_setup.R")
P <- dq(sprintf("SELECT * FROM read_parquet('%s')", pq("analysis_parcels")))
base <- P[base_oo == TRUE]
# counts by state and year (parcels in the base sample observed on the roll)
roll <- dq(sprintf("SELECT p.st, a.y, count(*) n FROM read_parquet('%s') a JOIN read_parquet('%s') p USING (clip)
                    WHERE p.base_oo AND a.in_roll AND a.y BETWEEN 2012 AND 2025 GROUP BY 1, 2", pq("analysis_panel"), pq("analysis_parcels")))
fwrite(dcast(roll, st ~ y, value.var = "n", fill = 0), file.path(OUT_S, "table_counts_state_year.csv"))
# bill16 / rate16 distributions and above16 by state
q <- function(x, p) as.numeric(quantile(x, p, na.rm = TRUE))
dist <- base[, .(n = .N, bill_p10 = q(bill16, .1), bill_p50 = q(bill16, .5), bill_p90 = q(bill16, .9), bill_p99 = q(bill16, .99),
                 rate_p10 = q(rate16, .1), rate_p50 = q(rate16, .5), rate_p90 = q(rate16, .9),
                 above16 = mean(above16), value_p50 = q(value16, .5), agi_p50 = q(agi_imp, .5), mortint_p50 = q(mortint16, .5),
                 share_rate_ok = mean(rate_ok), share_ratio_adj = mean(!is.na(ratio_c) & !(ratio_c >= 0.85 & ratio_c <= 1.15)))]
dist_st <- base[, .(n = .N, bill_p50 = q(bill16, .5), rate_p50 = q(rate16, .5), above16 = mean(above16), value_p50 = q(value16, .5)), by = st][order(-above16)]
fwrite(dist, file.path(OUT_S, "table_exposure_distribution.csv")); fwrite(dist_st, file.path(OUT_S, "table_exposure_by_state.csv"))
# occupancy agreement, co-owner shares, exit validation, investor counts
occ <- P[!corp & state_ok, .N, by = .(hs16, mh16, nm16, da16)][order(-N)]; occ[, share := N / sum(N)]
fwrite(occ, file.path(OUT_S, "table_occupancy_signals.csv"))
co <- base[, .N, by = coowner][, share := N / sum(N)][order(-N)]; fwrite(co, file.path(OUT_S, "table_coowner_classes.csv"))
ev <- fread(pcsv("diag_exit_validation.csv")); fwrite(ev, file.path(OUT_S, "table_exit_validation.csv"))
samp <- data.table(group = c("owner-occupied base", "investor-owned", "owner-occupied, all three signals agree", "two-owner married/likely", "two-owner unmarried"),
                   n = c(nrow(base), sum(P$base_inv), base[hs16 & mh16 & nm16, .N], base[coowner %in% c("married", "likely_married"), .N], base[coowner == "unmarried", .N]))
fwrite(samp, file.path(OUT_S, "table_sample_sizes.csv"))
# figures: bill16 histogram around 10k, rate16 distribution
g1 <- ggplot(base[bill16 < 30000], aes(bill16)) + geom_histogram(binwidth = 250, fill = "#4C6EF5") + geom_vline(xintercept = 10000, linetype = 2) +
  labs(x = "2016 property tax bill ($)", y = "parcels", title = "Distribution of bill16 (owner-occupied base sample)") + theme_minimal()
ggsave(file.path(FIG, "desc_bill16_hist.png"), g1, width = 7, height = 4, dpi = 150, bg = "white")
g2 <- ggplot(base[rate_ok == TRUE], aes(100 * rate16)) + geom_histogram(binwidth = 0.05, fill = "#4C6EF5") +
  labs(x = "effective tax rate 2016 (% of value)", y = "parcels", title = "Distribution of rate16") + xlim(0, 5) + theme_minimal()
ggsave(file.path(FIG, "desc_rate16_hist.png"), g2, width = 7, height = 4, dpi = 150, bg = "white")
print(dist); print(head(dist_st, 12)); print(co); print(samp)
