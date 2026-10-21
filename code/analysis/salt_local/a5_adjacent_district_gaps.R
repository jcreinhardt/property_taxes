# (b) at borders: gaps in property tax per pupil between adjacent school districts (TIGER cb 2019 unified + elementary;
# shared boundary after a 50 m buffer), cross-state pairs vs same-state pairs, FY2012-2023 (F-33). Outcome per pair-year:
# |log ptax per pupil_i - log ptax per pupil_j|; event study of cross-state x year relative to same-state pairs, pair FE +
# year FE, ref 2017, SE clustered by state pair (cross) / state (same). Also by SALT contrast of the two states.
suppressPackageStartupMessages({library(data.table); library(sf); library(tigris); library(fixest); library(ggplot2)})
options(tigris_use_cache = TRUE)
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/salt_local")
sd <- rbind(school_districts(cb = TRUE, year = 2019, type = "unified", progress_bar = FALSE)[, c("GEOID", "STATEFP")],
            school_districts(cb = TRUE, year = 2019, type = "elementary", progress_bar = FALSE)[, c("GEOID", "STATEFP")])
sd <- st_make_valid(st_transform(sd[!sd$STATEFP %in% c("02", "15", "72"), ], 5070)); sd <- sd[!duplicated(sd$GEOID), ]
nbl <- st_intersects(st_buffer(sd, 50), sd)
e <- unique(rbindlist(lapply(seq_along(nbl), \(i) if (length(nbl[[i]]) > 1) data.table(a = sd$GEOID[i], b = sd$GEOID[setdiff(nbl[[i]], i)]))))[a < b]
e[, `:=`(sa = substr(a, 1, 2), sb = substr(b, 1, 2))][, cross := as.integer(sa != sb)]
p <- readRDS(file.path(ROOT, "data/local_juris/district_finance.rds"))[ptax > 0 & enr >= 100][, bal := uniqueN(y) == 12, by = leaid][bal == TRUE]
p[, lptpp := log(ptax / enr)]
x <- merge(merge(e, p[, .(a = leaid, y, la = lptpp)], by = "a", allow.cartesian = TRUE), p[, .(b = leaid, y, lb = lptpp)], by = c("b", "y"))
x[, `:=`(gap = abs(la - lb), pid = paste(a, b), stpair = ifelse(cross == 1, paste(pmin(sa, sb), pmax(sa, sb)), sa))]
ex <- fread(file.path(ROOT, "data/local_juris/salt_exposure_county2016.csv"))
exs <- ex[, .(ex_st = weighted.mean(excess_pp, returns)), by = .(st = sprintf("%02d", county %/% 1000))]
x <- merge(merge(x, exs[, .(sa = st, exa = ex_st)], by = "sa"), exs[, .(sb = st, exb = ex_st)], by = "sb")
x[, dex := abs(exa - exb)][, dex_z := dex / sd(unique(x[cross == 1, .(stpair, dex)])$dex)]
m1 <- feols(gap ~ i(y, cross, ref = 2017) | pid + y, x, cluster = ~stpair)
m2 <- feols(gap ~ i(y, cross, ref = 2017) + i(y, I(cross * dex_z), ref = 2017) | pid + y, x, cluster = ~stpair)
tab <- function(m, pat) as.data.table(coeftable(m), keep.rownames = "term")[grepl(pat, term), .(term, est = round(Estimate, 4), se = round(`Std. Error`, 4))]
sink(file.path(OUT, "a5_adjacent_district_gaps.txt"))
cat("Adjacent district pairs (balanced, >= 100 pupils): cross-state", uniqueN(x[cross == 1, pid]), "| same-state", uniqueN(x[cross == 0, pid]), "\n")
cat("Mean |gap| in log property tax per pupil by year:\n"); print(x[, .(gap = round(mean(gap), 3)), by = .(y, cross)][order(cross, y)] |> dcast(y ~ cross, value.var = "gap"))
cat("\n== Cross-state x year (ref 2017), pair + year FE\n"); print(tab(m1, "cross"))
cat("\n== Adding cross x SALT contrast of the two states (SD units)\n"); print(tab(m2, "dex"))
sink(); cat(readLines(file.path(OUT, "a5_adjacent_district_gaps.txt")), sep = "\n")
