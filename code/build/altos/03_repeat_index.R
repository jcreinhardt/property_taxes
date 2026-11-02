# Repeat-listing rent index (same unit across years) at ZIP and tract level, data/built/altos/{zip,tract}_year_repeat.rds.
# Input: data/altos/altos_repeat_unit.parquet (01_altos_unit_month.sql): one row per unit and consecutive pair of listing years
# (y0 < y; price = median asking rent of the unit's months in the year), dlp = log rent change. Bailey-Muth-Nourse: within a
# geography, dlp on +1[y] -1[y0] year dummies (OLS, 2017 = 0); solved per geography from the aggregated (y0, y) cells with the
# normal equations (Laplacian of the pair graph). Years not linked to 2017 by any chain of pairs get NA (the unit ids change in
# 2018: ids used up to 2018 never reappear after 2018, so every chain across the cap passes through 2018).
# Two problems of the export are handled here: (i) a listing that straddles a year end is counted as a repeat with dlp = 0 (the
# same spell priced in both years): 35-83% of consecutive-year pairs are exact zeros, the share varies with the feed, and their
# mean change is 0.2%/yr against 2-5% actual rent growth. The main index lr_rep drops exact-zero changes (at the cost of the
# genuine unchanged rents, 10-18% of pairs two years apart); lr_rep0 keeps them. (ii) |dlp| > 1 (a ~e-fold change, 0.3% of pairs,
# mostly unit-id collisions) are dropped. Cells: n_rep = pairs involving the year (y or y0), n_end = pairs ending in it.
# Also: the national chained index (all units pooled) against the fixed-mix index of 02, output/build/altos/03_repeat_index.txt.
suppressPackageStartupMessages({library(data.table); library(duckdb); library(readxl); library(ggplot2)})
source("code/R/paths.R"); IN <- file.path(DATA, "altos"); OUT <- file.path(DATA, "built/altos"); O <- out_dir("build/altos"); invisible(out_dir("build/altos/figures"))
BASE <- 2017L; TRIM <- 1; options(width = 250)
con <- dbConnect(duckdb()); q <- \(s) as.data.table(dbGetQuery(con, s))
cells <- function(id) q(sprintf("SELECT %s AS g, y0::INT y0, y::INT y, (dlp <> 0)::INT nz, count(*) n, sum(dlp) s FROM '%s/altos_repeat_unit.parquet'
                                  WHERE abs(dlp) <= %f AND %s IS NOT NULL GROUP BY ALL", id, IN, TRIM, id))
cz <- cells("zip"); ctr <- cells("tract"); dbDisconnect(con, shutdown = TRUE)
# BMN per geography from (y0, y, n, s) cells: index relative to BASE on the connected component of BASE
bmn <- function(y0, y, n, s, base = BASE) {
  yrs <- sort(unique(c(y0, y))); k <- length(yrs); ib <- match(base, yrs)
  if (is.na(ib)) return(list(y = yrs, lr = rep(NA_real_, k)))
  i0 <- match(y0, yrs); i1 <- match(y, yrs)
  A <- matrix(0, k, k); A[cbind(i0, i1)] <- n; A <- A + t(A)                        # pairs between years
  comp <- logical(k); comp[ib] <- TRUE
  repeat { new <- colSums(A[comp, , drop = FALSE] > 0) > 0 & !comp; if (!any(new)) break; comp[new] <- TRUE }
  idx <- setdiff(which(comp), ib); lr <- rep(NA_real_, k); lr[ib] <- 0
  if (length(idx)) { L <- -A; diag(L) <- rowSums(A)                                 # X'X
    b <- numeric(k); for (j in seq_along(n)) { b[i1[j]] <- b[i1[j]] + s[j]; b[i0[j]] <- b[i0[j]] - s[j] }   # X'y
    lr[idx] <- solve(L[idx, idx, drop = FALSE], b[idx]) }
  list(y = yrs, lr = lr) }
index <- function(cl, id) {
  nz <- cl[nz == 1, bmn(y0, y, n, s), by = g][, .(g, y, lr_rep = lr)]
  al <- cl[, .(n = sum(n), s = sum(s)), by = .(g, y0, y)][, bmn(y0, y, n, s), by = g][, .(g, y, lr_rep0 = lr)]
  cnt <- rbind(cl[nz == 1, .(g, y = y, n_end = n, n0 = 0L)], cl[nz == 1, .(g, y = y0, n_end = 0L, n0 = n)])[, .(n_rep = sum(n_end + n0), n_end = sum(n_end)), by = .(g, y)]
  r <- Reduce(\(a, b) merge(a, b, by = c("g", "y"), all = TRUE), list(nz, al, cnt))
  r[is.na(n_rep), `:=`(n_rep = 0L, n_end = 0L)][order(g, y)] |> setnames("g", id) }
hud <- as.data.table(read_excel(file.path(DATA, "hud_zip_to_county_crosswalk.xlsx")))[order(ZIP, -RES_RATIO)][, .SD[1], by = ZIP][, .(zip = ZIP, fips = COUNTY)]
z <- merge(index(cz, "zip"), hud, by = "zip", all.x = TRUE)[, st := substr(fips, 1, 2)]
tr <- index(ctr, "tract")[, `:=`(fips = substr(tract, 1, 5), st = substr(tract, 1, 2))]
saveRDS(z, file.path(OUT, "zip_year_repeat.rds")); saveRDS(tr, file.path(OUT, "tract_year_repeat.rds"))
# national chained index: all pairs pooled, by treatment of zeros and by gap, against the fixed-mix index (listing-weighted mean of lr_adj)
nat <- function(cl, lab) { r <- cl[, .(n = sum(n), s = sum(s)), by = .(y0, y)][, bmn(y0, y, n, s)]; data.table(y = r$y, lr = r$lr, what = lab) }
nz <- cz[nz == 1]; al <- cz[, .(n = sum(n), s = sum(s)), by = .(y0, y)]
# and the mean within-ZIP change since 2017 over the ZIPs with >= 10 listings (02) and >= 10 linked pairs here in every year 2012-2025
fx <- merge(readRDS(file.path(OUT, "zip_year.rds"))[n >= 10, .(zip, y, lr_adj)], z[n_rep >= 10 & !is.na(lr_rep), .(zip, y, lr_rep, lr_rep0)], by = c("zip", "y"))
fx <- fx[y >= 2012][, nY := .N, by = zip][nY == uniqueN(fx[y >= 2012, y])]
for (v in c("lr_adj", "lr_rep", "lr_rep0")) fx[, (v) := get(v) - get(v)[y == BASE], by = zip]
bz <- melt(fx[, .(`fixed-mix (lr_adj)` = mean(lr_adj), `repeat, zeros dropped (lr_rep)` = mean(lr_rep), `repeat, zeros kept (lr_rep0)` = mean(lr_rep0)), by = y], id.vars = "y",
           variable.name = "what", value.name = "lr")[, what := sprintf("balanced ZIPs (%d), mean within-ZIP change: %s", uniqueN(fx$zip), what)]
nn <- rbind(nat(nz, "national pooled, zeros dropped (lr_rep)"), nat(al, "national pooled, zeros kept (lr_rep0)"), nat(nz[y - y0 >= 2], "national pooled, zeros dropped, pairs >= 2 years apart"),
            nat(nz[y - y0 == 1], "national pooled, zeros dropped, consecutive years only"), bz[, .(y, lr, what = as.character(what))])
pairs <- cz[, .(pairs = sum(n), zeros = sum(n[nz == 0]), gap1 = sum(n[y - y0 == 1])), by = .(y)][order(y)]
sink(file.path(O, "03_repeat_index.txt"), split = TRUE)
cat("Repeat-listing pairs by end year (|dlp| <= ", TRIM, "): all, exact zeros, consecutive-year pairs\n", sep = "")
print(as.data.frame(pairs), row.names = FALSE)
cat("\nNational chained log rent index (all units pooled) and mean within-ZIP change on balanced ZIPs, 2017 = 0 (x100):\n")
print(as.data.frame(dcast(nn[, .(what = factor(what, unique(what)), y, v = sprintf("%.1f", 100 * lr))], what ~ y, value.var = "v", fill = "")), row.names = FALSE)
for (k in list(list("ZIP", z, "zip"), list("tract", tr, "tract"))) {
  d <- k[[2]]; cat("\n==", k[[1]], "x year:", nrow(d), "rows,", uniqueN(d[[k[[3]]]]), "units\n")
  print(as.data.frame(d[, .(units = .N, linked = sum(!is.na(lr_rep)), n5 = sum(n_rep >= 5 & !is.na(lr_rep)), n10 = sum(n_rep >= 10 & !is.na(lr_rep)),
                            n30 = sum(n_rep >= 30 & !is.na(lr_rep)), pairs_end = sum(n_end), lr_rep = round(mean(lr_rep, na.rm = TRUE), 3),
                            lr_rep0 = round(mean(lr_rep0, na.rm = TRUE), 3)), by = y][order(y)]), row.names = FALSE) }
bal <- z[y %in% 2013:2024 & n_rep >= 10 & !is.na(lr_rep)][, .N, by = zip][N == 12]
cat("\nZIPs with >= 10 linked pairs in every year 2013-2024:", nrow(bal), "\n")
sink()
g <- ggplot(nn, aes(y, 100 * lr, colour = what)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey40") +
  geom_line() + geom_point(size = .8) + scale_x_continuous(breaks = seq(2011, 2025, 2)) +
  labs(x = NULL, y = "log rent x 100, 2017 = 0", colour = NULL, title = "Altos asking rents: repeat-listing index against the fixed-mix index") + scale_colour_brewer(palette = "Dark2") +
  theme_minimal(base_size = 9) + theme(legend.position = "bottom", legend.direction = "vertical")
ggsave(file.path(O, "figures/03_repeat_index.png"), g, width = 8, height = 6, dpi = 150)
