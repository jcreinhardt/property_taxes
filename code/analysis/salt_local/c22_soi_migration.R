# Migration in the IRS SOI data. The finest public SOI migration geography is the county (county-to-county flows of
# returns n1 / exemptions n2 / AGI, from address changes between consecutive filing years); SOI ZIP files have return
# counts but no flows. Flow year label: file "1718" (address on the TY2017 vs TY2018 return, i.e. moves during ~2018) = 2018.
# Ref 2017 (file 1617). Files 1112-2122 -> 2012-2022. Dyads with < 20 returns are suppressed (folded into "other flows").
# (1) Cross-border county dyads: county pairs on opposite sides of the same ranked state border (sides from local_juris/R/26,
#     county side/pair = majority of its tracts; both counties' median tract distance < 50 mi). Directed flow o -> d; within
#     each undirected dyad and year, compare high -> low with low -> high: log returns ~ i(y, from_high, ref 2017) |
#     directed dyad + undirected dyad x year. Dyads with both directions unsuppressed in all 11 years.
# (2) County out-migration rates (returns): all moves out, moves to another state; and log AGI per out-migrant return.
#     ~ i(y, ex_z, ref 2017) | county + state x year (county SALT exposure, SD); and border DDD near x high
#     (near = county's median tract distance < 20 mi) | county + pair^high^y + pair^near^y. SE by county.
# (3) ZIP net growth: log number of returns by ZIP (SOI ZIP files, TY2012-2022; ZIPs with returns in all years). ZIP side /
#     pair / distance from its tracts (tract -> modal ZIP of sales, a8). Border DDD near (< 10 mi) x high | ZIP +
#     pair^high^y + pair^dbin^y; and ZIP exposure x year | ZIP + state x year. SE by county.
suppressPackageStartupMessages({library(data.table); library(fixest); library(duckdb); library(ggplot2)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/salt_local"); M <- file.path(ROOT, "data/SOI/migration_county")
yrs <- 12:22
fl <- rbindlist(lapply(yrs, \(yy) { x <- fread(file.path(M, sprintf("countyoutflow%02d%02d.csv", yy - 1, yy)), colClasses = "character", encoding = "Latin-1")
  x[, .(o = sprintf("%02d%03d", as.integer(y1_statefips), as.integer(y1_countyfips)), ds = as.integer(y2_statefips), dc = as.integer(y2_countyfips),
        name = y2_countyname, n1 = as.numeric(n1), agi = as.numeric(agi), y = 2000L + yy)] }))
fl[n1 < 0, n1 := NA][agi < 0 | is.na(n1), agi := NA]
fl[, o_st := substr(o, 1, 2)]
tot <- fl[, .(nonmig = sum(n1[ds == as.integer(o_st) & sprintf("%02d%03d", ds, dc) == o & grepl("Non-migrants", name)], na.rm = TRUE),
              out_all = sum(n1[ds == 97 & dc == 0], na.rm = TRUE), out_xst = sum(n1[ds == 97 & dc == 3], na.rm = TRUE),
              agi_all = sum(agi[ds == 97 & dc == 0], na.rm = TRUE)), by = .(o, y)]
tot <- tot[nonmig > 0][, `:=`(base = nonmig + out_all)][, `:=`(r_out = 100 * out_all / base, r_xst = 100 * out_xst / base,
                                                               l_agi_pr = log(agi_all / out_all))]
dy <- fl[ds <= 56 & dc > 0 & !grepl("Non-migrants", name), .(o, d = sprintf("%02d%03d", ds, dc), y, n1)][!is.na(n1) & o != d]

# county side / pair / distance from tracts
tr <- unique(readRDS(file.path(ROOT, "data/local_juris/iv_tract_side.rds"))[, .(tract, pair, high, dist_mi, cty)])
mode1 <- \(x) names(which.max(table(x)))
cs <- tr[, .(pair = mode1(pair)), by = cty]
cs <- merge(tr, cs[, .(cty, pm = pair)], by = "cty")[pair == pm, .(pair = pair[1], high = as.integer(mode1(high)), dist = median(dist_mi)), by = cty]
ex <- fread(file.path(ROOT, "data/local_juris/salt_exposure_county2016.csv"))[, cty := sprintf("%05d", county)]

# ---- (1) cross-border dyads ----
bc <- cs[dist < 50]
x <- merge(dy, bc[, .(o = cty, po = pair, ho = high)], by = "o")
x <- merge(x, bc[, .(d = cty, pd = pair, hd = high)], by = "d")[po == pd & ho != hd]
x[, `:=`(ud = paste(pmin(o, d), pmax(o, d)), dd = paste(o, d), from_high = ho)]
x[, k := .N, by = dd]; x <- x[k == length(yrs)][, kk := uniqueN(dd), by = ud][kk == 2]
x <- merge(x, ex[, .(o = cty, ex_o = ex_z)], by = "o", all.x = TRUE)
m1 <- feols(log(n1) ~ i(y, from_high, ref = 2017) | dd + ud^y, x, cluster = ~o)
m1b <- feols(log(n1) ~ i(y, from_high, ref = 2017) + i(y, I(from_high * ex_o), ref = 2017) | dd + ud^y, x[!is.na(ex_o)], cluster = ~o)
# ---- (2) county rates ----
cr <- merge(tot, ex[, .(o = cty, ex_z)], by = "o")[, st := substr(o, 1, 2)]
cr <- merge(cr, cs[, .(o = cty, pair, high, near = as.integer(dist < 20))], by = "o", all.x = TRUE)
cr[, bal := uniqueN(y) == length(yrs), by = o]; cr <- cr[bal == TRUE]
cr[!is.finite(l_agi_pr), l_agi_pr := NA]
cb <- cr[!is.na(pair)][, nh := near * high]
# ---- (3) ZIP returns ----
con <- dbConnect(duckdb(), file.path(ROOT, "data/database.db"), read_only = TRUE)
zr <- as.data.table(dbGetQuery(con, "SELECT zipcode, year::INT y, returns FROM returns_zipcode WHERE year BETWEEN 2012 AND 2022 AND zipcode NOT IN (0, 99999)"))
dbDisconnect(con, shutdown = TRUE)
zr[, zip := sprintf("%05d", zipcode)][, bal := uniqueN(y) == 11 & all(returns > 0), by = zip]; zr <- zr[bal == TRUE]
tz <- fread(file.path(ROOT, "data/local_juris/salt_exposure_tract_zip2016.csv"), colClasses = "character")
cat("tract-zip file columns:", paste(names(tz), collapse = " "), "\n")
zc <- intersect(c("zip", "zip5", "zipcode"), names(tz))[1]; ezc <- "exz_z"   # z-score of winsorised ZIP exposure (a8)
tz <- tz[, .(tract, zip = sprintf("%05d", as.integer(get(zc))), ezip = as.numeric(get(ezc)))]
zt <- merge(tr, tz, by = "tract")
zs <- zt[, .(pm = mode1(pair)), by = zip]
zs <- merge(zt, zs, by = "zip")[pair == pm, .(pair = pair[1], high = as.integer(mode1(high)), dist = median(dist_mi), cty = mode1(cty), ezip = ezip[1]), by = zip]
zs[, `:=`(near = as.integer(dist < 10), dbin = cut(dist, c(0, 5, 10, 20, 30, 50, Inf), right = FALSE))]
zp <- merge(zr, zs, by = "zip")[, `:=`(lret = log(returns), nh = near * high, st = substr(cty, 1, 2))]
zm <- merge(zr, unique(tz[, .(zip, ezip)])[, .(ezip = ezip[1]), by = zip], by = "zip")
zm <- merge(zm, unique(zt[, .(zip, cty)])[, .(cty = cty[1]), by = zip], by = "zip")[, `:=`(lret = log(returns), st = substr(cty, 1, 2))]

tid <- \(m, lab, pat) as.data.table(coeftable(m), keep.rownames = "term")[grepl(pat, term)][, .(spec = lab, y = as.integer(sub("^y::(\\d+).*", "\\1", term)), b = Estimate, se = `Std. Error`)]
res <- rbind(
  tid(m1, "(1) cross-border dyads: log returns, high->low vs low->high", "from_high$"),
  tid(m1b, "(1) cross-border dyads: high->low x origin exposure", "ex_o"),
  tid(feols(r_out ~ i(y, ex_z, ref = 2017) | o + st^y, cr, cluster = ~o), "(2) county out-migration rate (%), x exposure", "ex_z"),
  tid(feols(r_xst ~ i(y, ex_z, ref = 2017) | o + st^y, cr, cluster = ~o), "(2) county out-of-state rate (%), x exposure", "ex_z"),
  tid(feols(l_agi_pr ~ i(y, ex_z, ref = 2017) | o + st^y, cr, cluster = ~o), "(2) log AGI per out-migrant return, x exposure", "ex_z"),
  tid(feols(r_xst ~ i(y, nh, ref = 2017) | o + pair^high^y + pair^near^y, cb, cluster = ~o), "(2) county out-of-state rate (%), border near x high", "nh"),
  tid(feols(lret ~ i(y, nh, ref = 2017) | zip + pair^high^y + pair^dbin^y, zp, cluster = ~cty), "(3) ZIP log returns, border near x high", "nh"),
  tid(feols(lret ~ i(y, ezip, ref = 2017) | zip + st^y, zm[is.finite(ezip)], cluster = ~cty), "(3) ZIP log returns, x ZIP exposure", "ezip"))
res <- rbind(res, unique(res[, .(spec)])[, `:=`(y = 2017L, b = 0, se = 0)])[order(spec, y)]
fwrite(res, file.path(OUT, "c22_soi_migration.csv"))
sink(file.path(OUT, "c22_soi_migration.txt"), split = TRUE)
cat("SOI county flows 2012-2022 (file year y-1 -> y), ref 2017.\n")
cat("(1) cross-border dyads with both directions unsuppressed every year:", uniqueN(x$ud), "county pairs,", uniqueN(x$po), "border pairs | median returns per directed flow:", median(x$n1), "\n")
cat("(2) counties (balanced):", uniqueN(cr$o), "| mean out-migration rate", round(mean(cr$r_out), 2), "% | out-of-state", round(mean(cr$r_xst), 2), "% | border counties:", uniqueN(cb$o), "near-high:", uniqueN(cb[nh == 1, o]), "\n")
cat("(3) ZIPs with returns every year 2012-22:", uniqueN(zr$zip), "| in border design:", uniqueN(zp$zip), "near-high:", uniqueN(zp[nh == 1, zip]), "| with ZIP exposure:", uniqueN(zm[is.finite(ezip), zip]), "\n\n")
print(dcast(res[, .(spec, y, cell = sprintf("%.3f (%.3f)", b, se))], y ~ spec, value.var = "cell"))
sink()
g <- ggplot(res, aes(y, b)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey40") +
  geom_ribbon(aes(ymin = b - 1.96 * se, ymax = b + 1.96 * se), fill = "#2a64a8", alpha = .15) + geom_line(colour = "#2a64a8") + geom_point(colour = "#2a64a8", size = 1) +
  facet_wrap(~ spec, scales = "free_y", ncol = 2, labeller = label_wrap_gen(55)) + scale_x_continuous(breaks = 2012:2022) +
  labs(x = "migration year (address change between tax years y-1 and y; ZIP: tax year)", y = "coefficient relative to 2017",
       title = "IRS SOI migration: cross-border county flows, county out-migration, ZIP return counts",
       subtitle = "95% CI. (1) SE by origin county; (2)-(3) SE by county. Dashed line: SALT cap (first affected moves in 2018).") +
  theme_minimal(base_size = 8)
ggsave(file.path(OUT, "figures/c22_soi_migration.png"), g, width = 11, height = 11, dpi = 150)
