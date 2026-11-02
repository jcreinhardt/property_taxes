# Border-region design for rents: does SALT-cap exposure matter more close to a state border than further away from it?
# Tract-year rent index (build/altos: lr_adj, log rent at a fixed type x bedroom mix; tract-years with >= MIN listings) on 2010
# tracts within 50 mi of their nearest ranked border pair (data/built/geo/iv_tract_side.rds, T = 30; side, distance bin, near
# = < 10 mi). Exposure E_i = lost federal subsidy per owner household (build/exposure/05, d_salt_own, $100/yr, winsorised p1/p99;
# ZIP where uncensored, county otherwise).
#   lr_it = a_i + g_{pair x side x t} + h_{pair x dbin x t} + sum_t c_t E_i 1[t] + sum_t b_t near_i E_i 1[t]
# c_t: the exposure gradient among tracts of the same side and distance bin away from the line; b_t: the extra gradient near
# the line, where a differently exposed market is within reach. Also the plain exposure event study (tract + pair x side x
# year FE) separately for near (< 10 mi) and far (10-50 mi) tracts and by distance bin. Ref 2017; weighted by 2016 returns
# (ZIP returns split across its tracts by parcel count); SE by county.
# ALTOS_INDEX=repeat: the repeat-listing (same-unit) tract index of build/altos/03 (lr_rep, tract-years with >= MIN linked
# pairs) as the outcome instead; outputs with the suffix _repeat.
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/border_iv"); invisible(out_dir("designs/border_iv/figures")); L <- file.path(DATA, "built")
REP <- identical(Sys.getenv("ALTOS_INDEX"), "repeat"); P <- paste0("d17_rent_exposure_near_far", if (REP) "_repeat" else ""); IDX <- if (REP) "repeat-listing rent index" else "rent index"
MIN <- 5; YRS <- 2013:2024; MX <- 50
wins <- \(x) { q <- quantile(x, c(.01, .99), na.rm = TRUE); pmin(pmax(x, q[1]), q[2]) / 100 }
pad <- \(x, w) { x <- as.character(x); paste0(strrep("0", pmax(0, w - nchar(x))), x) }
s <- unique(readRDS(file.path(L, "geo/iv_tract_side.rds"))[T == 30, .(tract, pair, high, dist_mi, dbin, near, cty)])[dist_mi < MX]
ez <- fread(file.path(L, "exposure/exposure_components_zip.csv"))[clean == TRUE, .(zip = pad(geo, 5), ez = wins(d_salt_own), ret_z = returns16)]
ec <- fread(file.path(L, "exposure/exposure_components_county.csv"))[, .(cty = pad(geo, 5), ecty = wins(d_salt_own), ret_c = returns16)]
tz <- fread(file.path(L, "exposure/salt_exposure_tract_zip2016.csv"), colClasses = c(tract = "character", zip = "character"))[, .(tract = pad(tract, 11), zip = pad(zip, 5), np = n)]
tz[, sh := np / sum(np), by = zip]
s <- merge(s, tz, by = "tract", all.x = TRUE); s <- merge(s, ez, by = "zip", all.x = TRUE); s <- merge(s, ec, by = "cty", all.x = TRUE)
s[, nt_c := .N, by = cty][, `:=`(E = fcoalesce(ez, ecty), w = fcoalesce(ret_z * sh, ret_c / nt_c), src = fifelse(!is.na(ez), "zip", "county"))]
s <- s[!is.na(E)][, E_near := E * near]
tr <- if (REP) readRDS(file.path(L, "altos/tract_year_repeat.rds"))[n_rep >= MIN & !is.na(lr_rep) & y %in% YRS, .(tract, y, lr = lr_rep, n = n_rep)] else
  readRDS(file.path(L, "altos/tract_year.rds"))[n >= MIN & y %in% YRS, .(tract, y, lr = lr_adj, n)]
d <- merge(tr, s, by = "tract"); d[, nY := uniqueN(y), by = tract]; d <- d[nY >= 6]   # tracts seen in at least 6 of the 12 years
d[, dbin := factor(as.character(dbin), c("[0,5)", "[5,10)", "[10,20)", "[20,30)", "[30,50)"))]
ct <- function(m, pat) { x <- as.data.table(coeftable(m), keep.rownames = "term")[grepl(pat, term)]
  x[, .(y = as.integer(sub("^y::(\\d+):.*", "\\1", term)), b = Estimate, se = `Std. Error`)] }
z0 <- \(...) data.table(y = 2017L, b = 0, se = 0, ...)
# DDD: exposure gradient far from the line (c_t) and the extra gradient near it (b_t)
m1 <- feols(lr ~ i(y, E, ref = 2017) + i(y, E_near, ref = 2017) | tract + pair^high^y + pair^dbin^y, d, weights = ~w, cluster = ~cty)
res <- rbind(rbind(ct(m1, ":E$"), z0())[, term := "c_t: exposure x year (within side x distance bin)"],
             rbind(ct(m1, ":E_near$"), z0())[, term := "b_t: near x exposure x year (extra gradient < 10 mi)"])[, spec := "DDD"]
# plain exposure event studies by distance to the line
es <- function(x, lab) { m <- feols(lr ~ i(y, E, ref = 2017) | tract + pair^high^y, x, weights = ~w, cluster = ~cty)
  rbind(ct(m, ":E$"), z0())[, `:=`(term = lab, spec = "by distance", n = nobs(m), tracts = uniqueN(x$tract))] }
res2 <- rbind(es(d[near == 1], "near: < 10 mi"), es(d[near == 0], "far: 10-50 mi"), es(d, "all within 50 mi"),
              rbindlist(lapply(levels(d$dbin), \(b) es(d[dbin == b], paste("bin", b)))))
fwrite(rbind(res, res2, fill = TRUE), file.path(OUT, paste0(P, ".csv")))
sink(file.path(OUT, paste0(P, ".txt")), split = TRUE)
cat(sprintf("Tracts: %d (%d near) in %d pairs; tract-years %d; exposure from ZIP for %.0f%% of tracts; weighted by 2016 returns; SE by county\n",
            uniqueN(d$tract), uniqueN(d[near == 1, tract]), uniqueN(d$pair), nrow(d), 100 * mean(unique(d[, .(tract, src)])$src == "zip")))
cat("\n== DDD: log rent on exposure x year and near x exposure x year; tract + pair x side x year + pair x distance-bin x year FE (% per $100/yr)\n")
print(dcast(res[, .(term, y, cell = sprintf("%.2f (%.2f)", 100 * b, 100 * se))], term ~ y, value.var = "cell"))
cat("\n== Exposure x year by distance to the line; tract + pair x side x year FE (% per $100/yr)\n")
print(dcast(res2[, .(term = sprintf("%s [%d tracts]", term, tracts), y, cell = sprintf("%.2f (%.2f)", 100 * b, 100 * se))], term ~ y, value.var = "cell"))
sink()
pd <- rbind(res, res2[term %in% c("near: < 10 mi", "far: 10-50 mi")], fill = TRUE)
g <- ggplot(pd, aes(y, 100 * b, colour = term)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey40") +
  geom_pointrange(aes(ymin = 100 * (b - 1.96 * se), ymax = 100 * (b + 1.96 * se)), position = position_dodge(.5), size = .15) + geom_line(position = position_dodge(.5), linewidth = .3) +
  facet_wrap(~ spec, ncol = 1, scales = "free_y") + scale_x_continuous(breaks = seq(2013, 2024, 2)) +
  labs(x = NULL, y = "% change in asking rent per $100/yr lost subsidy per owner (vs 2017)", colour = NULL,
       title = "Asking rents and SALT-cap exposure near and away from state borders",
       subtitle = sprintf("Tract %s (Altos); tracts within 50 mi of a ranked border pair; weighted by 2016 returns; 95%% CI, SE by county", IDX)) +
  theme_minimal(base_size = 9) + theme(legend.position = "bottom", legend.direction = "vertical")
ggsave(file.path(OUT, "figures", paste0(P, ".png")), g, width = 8, height = 9, dpi = 150)
