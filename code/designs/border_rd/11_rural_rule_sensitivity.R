# How the balance of the low-tax side within 50 mi depends on the rule that drops "metro pairs" (09). Three rules: a pair is
# dropped when a metropolitan CBSA >= 0.5m has at least a share c of its population within 50 mi of the line on the LOW side,
# on the HIGH side, or on EITHER side; c runs 0.1 ... 0.9 and "none" (nothing dropped). For each rule x c: pairs and tracts
# kept, and the balance of the 21 outcomes/controls of 09 on the low side within 50 mi (5-mi bins, pair FE, SE by county):
# covariates with joint p < .05, covariates with |mean(30-50 mi) - mean(0-10 mi)| > 0.1 SD, median and max of that gap,
# median |slope per 10 mi|, and the rate's own slope. Also the metros that straddle a border: shares on both sides. And a
# residualising variant: distance from the tract to the nearest metro centre (population-weighted centroid of each metro
# >= 0.5m) enters as bins (0-10, 10-20, 20-30, 30-50, 50-75, 75-100, 100-150, 150+ mi) next to the distance-to-border bins,
# on all pairs and on the 30% subsets.
# Reads data/built/geo/low_side_balance_tracts.rds (08), output/designs/border_rd/c22_metro_shares_by_pair_side.csv and
# data/built/geo/tract_cbsa_2019.csv (09).
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2); library(sf)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_rd"); MX <- 50; BW <- 5
d <- readRDS(file.path(DATA, "built/geo/low_side_balance_tracts.rds"))[dist_mi < MX]
d[, bin := cut(dist_mi, seq(0, MX, BW), paste0(seq(0, MX - BW, BW), "-", seq(BW, MX, BW)), right = FALSE)]
sh <- fread(file.path(OUT, "c22_metro_shares_by_pair_side.csv"))
# distance to the nearest metro centre (>= 0.5m): population-weighted centroid of the metro's tracts
tc <- fread(file.path(DATA, "built/geo/tract_cbsa_2019.csv"), colClasses = c(tract = "character", cbsa = "character"))
ce <- tc[cbsa_pop >= 5e5 & is.finite(pop) & is.finite(lat), .(lat = weighted.mean(lat, pop), lon = weighted.mean(lon, pop)), by = .(cbsa, cbsa_name)]
cp <- st_transform(st_as_sf(ce, coords = c("lon", "lat"), crs = 4326), 5070); tp <- st_transform(st_as_sf(d[, .(tract, lat, lon)], coords = c("lon", "lat"), crs = 4326), 5070)
nn <- st_nearest_feature(tp, cp); d[, dmetro := as.numeric(st_distance(tp, cp[nn, ], by_element = TRUE)) / 1609.344][, metro_nn := ce$cbsa_name[nn]]
d[, dm_bin := cut(dmetro, c(0, 10, 20, 30, 50, 75, 100, 150, Inf), right = FALSE)]
cat("distance to nearest metro centre (mi), low side < 50 mi:\n"); print(summary(d$dmetro)); print(d[, .N, by = dm_bin][order(dm_bin)])
v <- c("lp50", "lhval", "moveout", "sh_moved", "turnover", "linc", "sh_agi100", "sh_item", "exz", "age", "hhsize", "sh_u18", "sh_hh_kids",
       "sh_own", "sh_ba", "sh_white", "yrbuilt", "lpop", "ldens", "ach", "ses")
sds <- sapply(c(v, "med"), \(k) sd(d[[k]], na.rm = TRUE))
# ---- straddling metros: >= 10% of the metro on each side of the same pair
str <- dcast(sh, pair + cbsa_name + cbsa_pop ~ side, value.var = "share", fill = 0)[low >= .1 & high >= .1][order(-cbsa_pop)]
str[, `:=`(cbsa_name = sub(",.*", "", cbsa_name), low = round(low, 2), high = round(high, 2), cbsa_pop = round(cbsa_pop / 1e6, 2))]
fwrite(str, file.path(OUT, "c24_straddling_metros.csv"))
# ---- balance of a pair set
bal <- function(keep, ctrl = "") { x0 <- d[pair %in% keep]
  r <- rbindlist(lapply(c(v, "med"), \(k) { x <- x0[is.finite(get(k))]
    m <- feols(as.formula(sprintf("%s ~ i(bin, ref = '0-5')%s | pair", k, ctrl)), x, cluster = ~cty)
    ct <- as.data.table(coeftable(m), keep.rownames = "term")[grepl("^bin::", term), .(x = as.numeric(sub("bin::(\\d+)-.*", "\\1", term)) + BW / 2, b = Estimate / sds[k])]
    ct <- rbind(data.table(x = BW / 2, b = 0), ct)
    sl <- coeftable(feols(as.formula(sprintf("%s ~ dist_mi%s | pair", k, ctrl)), x, cluster = ~cty))
    data.table(var = k, p_joint = wald(m, "^bin::", print = FALSE)$p, d = mean(ct[x > 30, b]) - mean(ct[x < 10, b]), slope10 = 10 * sl[1, 1] / sds[k], slope_p = sl[1, 4]) }))
  c(list(pairs = uniqueN(x0$pair), tracts = nrow(x0)),
    r[var != "med", .(joint_rej = sum(p_joint < .05), gap_gt_0.1 = sum(abs(d) > .1), median_abs_gap = median(abs(d)), max_abs_gap = max(abs(d)), median_abs_slope10 = median(abs(slope10)), slope_rej = sum(slope_p < .05))],
    r[var == "med", .(rate_slope10 = slope10, rate_gap = d)]) }
grid <- CJ(rule = c("low", "high", "either"), cut = c(seq(.1, .9, .1), 1.1))
cache <- list(); res <- list()
for (i in seq_len(nrow(grid))) { ru <- grid$rule[i]; cc <- grid$cut[i]
  drop <- sh[share >= cc & (ru == "either" | side == ru), unique(pair)]
  keep <- setdiff(unique(d$pair), drop); key <- paste(sort(keep), collapse = ",")
  if (is.null(cache[[key]])) cache[[key]] <- bal(keep)
  res[[i]] <- c(list(rule = ru, cut = cc), cache[[key]]) }
res <- rbindlist(res)[, cut_lab := ifelse(cut > 1, "none", sprintf("%.0f%%", 100 * cut))]
# residualising on distance to the nearest metro centre instead of / on top of dropping pairs
allp <- unique(d$pair); CT <- " + i(dm_bin)"
rc <- rbindlist(list(c(list(rule = "metro-distance control", cut = 1.1), bal(allp, CT)),
  c(list(rule = "low 30% + metro-distance control", cut = .3), bal(setdiff(allp, sh[share >= .3 & side == "low", unique(pair)]), CT)),
  c(list(rule = "high 30% + metro-distance control", cut = .3), bal(setdiff(allp, sh[share >= .3 & side == "high", unique(pair)]), CT))))
rc[, cut_lab := ifelse(cut > 1, "none", sprintf("%.0f%%", 100 * cut))]
res <- rbind(res, rc)
fwrite(res, file.path(OUT, "c24_rural_rule_sensitivity.csv"))
sink(file.path(OUT, "c24_rural_rule_sensitivity.txt"), split = TRUE)
cat("Metros (>= 0.5m) that straddle a ranked border: share of the metro's population within 50 mi of the line on each side\n")
print(str, nrows = 100)
cat("\nBalance of the low side within 50 mi by rule and share cutoff c (pairs dropped when a metro >= 0.5m has >= c of its population in the window on that side).\n",
    "21 outcomes/controls: joint_rej = joint p < .05 (5-mi bins); gap = mean(30-50 mi) - mean(0-10 mi) in SD units; rate_slope10 = effective-rate slope per 10 mi (SD).\n\n")
for (ru in c("low", "high", "either")) { cat("== rule:", ru, "side\n")
  print(as.data.frame(res[rule == ru, .(cut = cut_lab, pairs, tracts, joint_rej, gap_gt_0.1, median_abs_gap = round(median_abs_gap, 3), max_abs_gap = round(max_abs_gap, 2),
    median_abs_slope10 = round(median_abs_slope10, 3), slope_rej, rate_slope10 = round(rate_slope10, 3), rate_gap = round(rate_gap, 2))]), row.names = FALSE); cat("\n") }
cat("== residualised on distance to the nearest metro centre (>= 0.5m; bins 0-10 ... 150+ mi), bin coefficients and slopes net of it\n")
print(as.data.frame(rc[, .(rule, cut = cut_lab, pairs, tracts, joint_rej, gap_gt_0.1, median_abs_gap = round(median_abs_gap, 3), max_abs_gap = round(max_abs_gap, 2),
    median_abs_slope10 = round(median_abs_slope10, 3), slope_rej, rate_slope10 = round(rate_slope10, 3), rate_gap = round(rate_gap, 2))]), row.names = FALSE)
sink()
res <- res[!grepl("control", rule)]
# ---- figure
pl <- melt(res[, .(rule, cut, pairs, `covariates with joint p < .05 (of 21)` = as.numeric(joint_rej), `covariates with |gap 30-50 vs 0-10| > 0.1 SD` = as.numeric(gap_gt_0.1),
                   `median |gap| (SD)` = median_abs_gap, `max |gap| (SD)` = max_abs_gap, `pairs kept` = as.numeric(pairs), `rate slope per 10 mi (SD)` = rate_slope10)][, -"pairs"],
           id.vars = c("rule", "cut"))
hl <- melt(rc[rule == "metro-distance control", .(`covariates with joint p < .05 (of 21)` = as.numeric(joint_rej), `covariates with |gap 30-50 vs 0-10| > 0.1 SD` = as.numeric(gap_gt_0.1),
                   `median |gap| (SD)` = median_abs_gap, `max |gap| (SD)` = max_abs_gap, `rate slope per 10 mi (SD)` = rate_slope10)], measure.vars = 1:5)
g <- ggplot(pl, aes(cut, value, colour = rule)) + geom_hline(data = hl, aes(yintercept = value), linetype = "22", colour = "grey40", linewidth = .4) +
  geom_line(linewidth = .5) + geom_point(size = 1.2) +
  facet_wrap(~ variable, scales = "free_y", ncol = 3) + scale_x_continuous(breaks = c(seq(.1, .9, .2), 1.1), labels = c("10%", "30%", "50%", "70%", "90%", "none")) +
  scale_colour_manual("metro share measured on the", values = c(low = "#2e8b57", high = "#c0392b", either = "#2a64a8"), labels = c(low = "low-tax side", high = "high-tax side", either = "either side")) +
  labs(x = "share cutoff c: a pair is dropped when a metro >= 0.5m has >= c of its population within 50 mi of the line on that side",
       y = NULL, title = "Low-tax side within 50 mi: balance as the metro-pair rule varies",
       subtitle = "21 outcomes and controls, 5-mi bins, pair FE, SE by county; lower cutoff = more pairs dropped. Dashed grey: all pairs, residualised on distance to the nearest metro centre") +
  theme_minimal(base_size = 8) + theme(legend.position = "bottom")
ggsave(file.path(OUT, "figures/c24_rural_rule_sensitivity.png"), g, width = 10, height = 6, dpi = 150)
