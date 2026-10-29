# Data Axle vs IRS/SOI state-to-state migration, moves between CA, TX, NY and FL (12 directed pairs), by year.
# SOI: stateoutflowYYYY.csv (returns n1, exemptions n2 that changed state between the returns filed in y1 and y2; labelled y2 as in
#      the rest of the project; file 1617 = y2 2017 is known to code ~30% too many movers). In-state base = non-migrants + same-state
#      movers (the same-state row is missing in the 1112 and 1213 files: imputed from the 2014-16 same-state share, flagged).
# Data Axle: output/validation/dataaxle/soi_vs_dataaxle/da_mig4_full.csv from da_mig4_full.sql (Bouchet, full dataaxle_annual, 2022
#      from the quarterly replacement): families with the same family_id in two consecutive annual files, both in the four states;
#      loose = state changed, strict = state changed and length_of_residence reset in t+1; persons = non-null individual ids on the
#      t+1 record; stayers = same state. The DA pair (t, t+1) is matched to the SOI file labelled t+1 (addresses ~Apr t vs ~Apr t+1
#      against DA cut-offs cut(t) to cut(t+1)); the DA interval is cut-off months apart (12 only in 2012-14), so rates are also annualised.
# DA_FILE=1pct uses the 1% family sample (x100) instead of the full extract.
# Outputs (output/validation/dataaxle/soi_vs_dataaxle/): soi_vs_dataaxle_pairs.csv (pair x year), soi_vs_dataaxle_pooled.csv, .txt,
#      figures soi_vs_dataaxle_capture.png (pooled and by origin), soi_vs_dataaxle_pairs.png (12 pairs).
suppressPackageStartupMessages({library(data.table); library(ggplot2); library(patchwork)})
source("code/R/paths.R"); OUT <- out_dir("validation/dataaxle/soi_vs_dataaxle"); DF <- Sys.getenv("DA_FILE", "full"); SFX <- ifelse(DF == "1pct", "_1pct", "")
ST <- c(`6` = "CA", `12` = "FL", `36` = "NY", `48` = "TX")
# ---- SOI
rd <- function(y1) { x <- fread(file.path(DATA, sprintf("SOI/migration_state/stateoutflow%02d%02d.csv", y1 - 2000, y1 - 1999)), colClasses = "character", encoding = "Latin-1"); setnames(x, tolower(names(x)))
  x <- x[, .(y = y1 + 1L, o = as.integer(y1_statefips), d = as.integer(y2_statefips), name = y2_state_name, n1 = as.numeric(n1), n2 = as.numeric(n2))][o %in% names(ST)]
  x[, kind := fcase(d == o, "nonmig", grepl("Same State", name), "same", d %in% as.integer(names(ST)), "flow", default = "other")][kind != "other"] }
soi <- rbindlist(lapply(2011:2021, rd))[, `:=`(o = ST[as.character(o)], d = fifelse(kind == "flow", ST[as.character(d)], NA_character_))]
base <- dcast(soi[kind != "flow"], y + o ~ kind, value.var = c("n1", "n2"))
imp <- base[y %in% 2014:2016, .(r1 = mean(n1_same / n1_nonmig), r2 = mean(n2_same / n2_nonmig)), by = o]
base <- merge(base, imp, by = "o")[, same_imputed := is.na(n1_same)][, `:=`(soi_base1 = n1_nonmig + fifelse(is.na(n1_same), n1_nonmig * r1, n1_same), soi_base2 = n2_nonmig + fifelse(is.na(n2_same), n2_nonmig * r2, n2_same))][, .(y, o, soi_base1, soi_base2, same_imputed)]
flow <- soi[kind == "flow", .(y, o, d, soi_n1 = n1, soi_n2 = n2)]
# ---- Data Axle
da <- fread(file.path(OUT, sprintf("da_mig4_%s.csv", DF))); if (DF == "1pct") da[, (names(da)[-(1:4)]) := lapply(.SD, `*`, 100), .SDcols = -(1:4)]
cut <- c(`2012` = 3, `2013` = 3, `2014` = 3, `2015` = 4, `2016` = 6, `2017` = 5, `2018` = 7, `2019` = 6, `2020` = 6, `2021` = 6, `2022` = 7, `2023` = 6)
da[, months := 12 * (y1 - y0) + cut[as.character(y1)] - cut[as.character(y0)]]        # NA for 2011->12 (2011 cut-off unknown)
da[, y := y1]; da <- da[!(y0 == 2021 & y1 == 2023)]                                   # keep the annual chain, incl. 2022 from the quarterly files
stay <- da[o == d, .(y, o, da_stay = fam, da_stay_prim = fam_prim, da_stay_persons = persons)]
mv <- da[o != d]
# ---- merge, pair level
p <- merge(mv, flow, by = c("y", "o", "d"), all = TRUE); p <- merge(p, stay, by = c("y", "o"), all.x = TRUE); p <- merge(p, base, by = c("y", "o"), all.x = TRUE)
p[, `:=`(pair = paste0(o, ">", d), ann = 12 / months,
         cap_loose = fam / soi_n1, cap_strict = fam_strict / soi_n1, cap_prim_strict = fam_prim_strict / soi_n1, cap_persons = persons_strict / soi_n2,
         rate_da = fam_strict / da_stay, rate_soi = soi_n1 / soi_base1)][, `:=`(cap_strict_ann = cap_strict * ann, cap_loose_ann = cap_loose * ann, rel_rate = rate_da / rate_soi, rel_rate_ann = rate_da * ann / rate_soi)]
fwrite(p[order(y, o, d)], file.path(OUT, paste0("soi_vs_dataaxle_pairs", SFX, ".csv")))
# ---- pooled over the 12 pairs, and by origin state
pool <- function(x, by) x[, .(soi_n1 = sum(soi_n1), soi_n2 = sum(soi_n2), fam = sum(fam), fam_strict = sum(fam_strict), fam_prim_strict = sum(fam_prim_strict), persons_strict = sum(persons_strict),
                              da_stay = sum(da_stay), soi_base1 = sum(soi_base1), months = months[1], same_imputed = any(same_imputed)), by = by][
  , `:=`(cap_loose = fam / soi_n1, cap_strict = fam_strict / soi_n1, cap_prim_strict = fam_prim_strict / soi_n1, cap_persons = persons_strict / soi_n2, rate_da = fam_strict / da_stay, rate_soi = soi_n1 / soi_base1)][
  , `:=`(cap_strict_ann = cap_strict * 12 / months, cap_loose_ann = cap_loose * 12 / months, rel_rate = rate_da / rate_soi, rel_rate_ann = rate_da * 12 / months / rate_soi)]
# the stayer base by origin is the same for every destination: take it once per origin-year before pooling
pb <- p[, .SD[1], by = .(y, o), .SDcols = c("da_stay", "soi_base1", "same_imputed")]; pm <- p[, .(soi_n1 = sum(soi_n1), soi_n2 = sum(soi_n2), fam = sum(fam), fam_strict = sum(fam_strict), fam_prim_strict = sum(fam_prim_strict), persons_strict = sum(persons_strict), months = months[1]), by = .(y, o)]
po <- merge(pm, pb, by = c("y", "o")); po[, grp := o]
pa <- copy(po)[, grp := "all 12 pairs"]
pooled <- pool(rbind(pa, po), c("grp", "y"))[order(grp, y)]
fwrite(pooled, file.path(OUT, paste0("soi_vs_dataaxle_pooled", SFX, ".csv")))
sink(file.path(OUT, paste0("soi_vs_dataaxle", SFX, ".txt")), split = TRUE)
cat("Data Axle vs SOI moves between CA, TX, NY and FL (12 directed pairs), by year (y = SOI file label = Data Axle destination file).\n",
    "cap_* = Data Axle moves / SOI returns that moved (loose: state changed; strict: + LOR reset; prim: primary families; persons: DA persons / SOI exemptions).\n",
    "*_ann = scaled to 12 months by the Data Axle cut-off interval (months). rel_rate = (DA strict moves / DA same-state families) / (SOI movers / SOI in-state returns).\n",
    "SOI y = 2017 (file 1617) codes about 30% too many movers; 2012-13 in-state bases impute the same-state row. Data Axle", DF, "\n\n")
print(as.data.frame(pooled[grp == "all 12 pairs", .(y, months, soi_returns = soi_n1, da_loose = fam, da_strict = fam_strict, da_prim_strict = fam_prim_strict, cap_loose = round(cap_loose, 3), cap_strict = round(cap_strict, 3), cap_strict_ann = round(cap_strict_ann, 3), cap_prim_strict = round(cap_prim_strict, 3), cap_persons = round(cap_persons, 3), rate_da_pct = round(100 * rate_da, 2), rate_soi_pct = round(100 * rate_soi, 2), rel_rate_ann = round(rel_rate_ann, 3))]), row.names = FALSE)
cat("\nBy origin state (strict, annualised capture):\n"); print(dcast(pooled[grp != "all 12 pairs"], y ~ grp, value.var = "cap_strict_ann", fun.aggregate = function(v) round(v, 3)))
cat("\nBy directed pair (strict, annualised capture):\n"); print(dcast(p, y ~ pair, value.var = "cap_strict_ann", fun.aggregate = function(v) round(v, 3)))
cat("\nBy directed pair (strict, raw capture):\n"); print(dcast(p, y ~ pair, value.var = "cap_strict", fun.aggregate = function(v) round(v, 3)))
cat("\nMean over 2013-2022 by pair: SOI returns, DA strict, capture raw / annualised, persons per DA strict family vs exemptions per SOI return\n")
print(p[y >= 2013 & is.finite(fam_strict), .(soi = round(mean(soi_n1)), da_strict = round(mean(fam_strict)), cap = round(sum(fam_strict) / sum(soi_n1), 3), cap_ann = round(sum(fam_strict * ann) / sum(soi_n1), 3), da_pp = round(sum(persons_strict) / sum(fam_strict), 2), soi_pp = round(sum(soi_n2) / sum(soi_n1), 2)), by = pair][order(pair)])
sink()
# ---- figures
pl <- melt(pooled[grp == "all 12 pairs"], id.vars = "y", measure.vars = c("cap_loose", "cap_strict", "cap_prim_strict", "cap_persons", "cap_strict_ann", "rel_rate_ann"))
pl[, variable := factor(variable, c("cap_loose", "cap_strict", "cap_prim_strict", "cap_persons", "cap_strict_ann", "rel_rate_ann"),
                        c("families, state changed (loose)", "families, strict (LOR reset)", "primary families, strict", "persons strict / SOI exemptions", "strict, annualised to 12 months", "relative move rate, annualised"))]
g1 <- ggplot(pl, aes(y, value, colour = variable)) + geom_hline(yintercept = 1, colour = "grey70", linetype = 2) + geom_vline(xintercept = 2017.5, colour = "grey80") + geom_line() + geom_point(size = 1.4) +
  scale_x_continuous(breaks = 2012:2022) + scale_y_continuous(labels = scales::percent) + scale_colour_brewer(NULL, palette = "Dark2") +
  labs(x = "SOI file year (second filing year) = Data Axle destination file", y = "Data Axle / SOI", title = "Share of SOI interstate moves among CA, TX, NY, FL found in Data Axle (12 pairs pooled)",
       subtitle = "SOI: returns (or exemptions) that changed state between consecutive filings; Data Axle: same family_id in consecutive annual files, state changed (+ LOR reset = strict)") +
  theme_minimal(base_size = 9) + theme(legend.position = "bottom") + guides(colour = guide_legend(nrow = 2))
g2 <- ggplot(pooled[grp != "all 12 pairs"], aes(y, cap_strict_ann, colour = grp)) + geom_vline(xintercept = 2017.5, colour = "grey80") + geom_line() + geom_point(size = 1.4) +
  scale_x_continuous(breaks = 2012:2022) + scale_y_continuous(labels = scales::percent) + scale_colour_brewer("origin state", palette = "Set1") +
  labs(x = NULL, y = "strict, annualised", title = "By origin state (moves to the other three)") + theme_minimal(base_size = 9) + theme(legend.position = "bottom")
ggsave(file.path(OUT, paste0("soi_vs_dataaxle_capture", SFX, ".png")), g1 / g2 + plot_layout(heights = c(3, 2)), width = 9, height = 8, dpi = 150, bg = "white")
g3 <- ggplot(melt(p, id.vars = c("y", "pair"), measure.vars = c("cap_strict", "cap_strict_ann"))[, variable := factor(variable, c("cap_strict", "cap_strict_ann"), c("raw", "annualised"))], aes(y, value, colour = variable)) +
  geom_vline(xintercept = 2017.5, colour = "grey80") + geom_line() + geom_point(size = 1) + facet_wrap(~ pair, ncol = 4) + scale_x_continuous(breaks = seq(2012, 2022, 2)) + scale_y_continuous(labels = scales::percent) +
  scale_colour_manual(NULL, values = c("grey50", "#2a64a8")) + labs(x = NULL, y = "Data Axle strict moves / SOI returns", title = "Capture by directed pair (strict definition)") + theme_minimal(base_size = 8) + theme(legend.position = "bottom")
ggsave(file.path(OUT, paste0("soi_vs_dataaxle_pairs", SFX, ".png")), g3, width = 10, height = 6.5, dpi = 150, bg = "white")
