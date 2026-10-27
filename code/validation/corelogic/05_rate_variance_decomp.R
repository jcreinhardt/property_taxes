# Variance decomposition of nominal property-tax rates across the four Census layers (state, county, incorporated place,
# unified school district). 1% building sample, 2016, residential.
# Nominal rate of a tax-area cell (fips x tax area code, or fips x municipality_name where no code) = slope of the median
# regression bill ~ a + r*AV on the cell's non-homestead parcels (>= 30, >= 50% of bills within 1% of the line); else the
# pooled cell slope if the pooled fit passes; else missing. Every parcel in the cell gets the cell's rate.
# Rate per market value (comparable across assessment ratios) = rate on AV x median(AV/MV) of the cell's non-homesteads x the
# sales ratio ratio_c (median MV / sale price 2015-17, tax area or county; from the SALT build). CoreLogic MV equals AV in
# fractional-assessment states (IL, LA, NY, MI), so MV alone is not market value. Cells with a market-value rate outside
# 0.05-6% are dropped as implausible (counted in the output).
# Layers from point-in-polygon on parcel coordinates (cb 2020 states, cb 2019 counties, cb 2017 places with LSAD != 57 and
# unified school districts; AK, HI have no place/unsd files here).
suppressPackageStartupMessages({library(sf); library(dplyr); library(tidyr); library(duckdb); library(quantreg); library(fixest)})
sf_use_s2(FALSE)
source("code/R/paths.R"); OUT <- out_dir("validation/corelogic")
TIG <- path.expand("~/Library/Caches/tigris")
con <- dbConnect(duckdb())
d <- dbGetQuery(con, sprintf("SELECT clip, st, lpad(fips::VARCHAR,5,'0') fips, coalesce(nullif(trim(tac),''), 'M:' || upper(trim(muni_name)), '_') cell,
  bill16 bill, av16 av, mv16 mv, coalesce(hs16, false) hs, lat, lon, ratio_c
  FROM '%s/data/built/corelogic/parcel_1pct/parcels.parquet' WHERE st NOT IN ('GU','VI','PR')", ROOT))
dbDisconnect(con, shutdown = TRUE)

# ---- 1. layers by point-in-polygon ----
lay_pq <- file.path(DATA, "built/corelogic/parcel_layers_1pct.parquet")
if (!file.exists(lay_pq)) {
  pts <- d |> filter(!is.na(lat), lat != 0) |> select(clip, lat, lon) |> st_as_sf(coords = c("lon", "lat"), crs = 4269)
  rd <- function(kind) do.call(rbind, lapply(list.files(TIG, sprintf("^cb_2017_\\d{2}_%s_500k\\.shp$", kind), full.names = TRUE),
    \(f) st_read(f, quiet = TRUE) |> st_transform(4269) |> select(any_of(c("GEOID", "LSAD")))))
  jn <- function(poly, nm) st_join(pts, poly |> select(GEOID) |> rename(!!nm := GEOID), join = st_within) |> st_drop_geometry() |> distinct(clip, .keep_all = TRUE)
  L <- jn(st_read(file.path(TIG, "cb_2020_us_state_500k.shp"), quiet = TRUE) |> st_transform(4269), "g_state") |>
    left_join(jn(st_read(file.path(TIG, "cb_2019_us_county_500k.shp"), quiet = TRUE) |> st_transform(4269), "g_county"), by = "clip") |>
    left_join(jn(rd("place") |> filter(LSAD != "57"), "g_place"), by = "clip") |>
    left_join(jn(rd("unsd"), "g_unsd"), by = "clip")
  dbWriteTable(con <- dbConnect(duckdb()), "L", L); dbExecute(con, sprintf("COPY L TO '%s' (FORMAT parquet)", lay_pq)); dbDisconnect(con, shutdown = TRUE)
}
con <- dbConnect(duckdb()); L <- dbGetQuery(con, sprintf("SELECT * FROM '%s'", lay_pq)); dbDisconnect(con, shutdown = TRUE)
d <- d |> left_join(L, by = "clip")

# ---- 2. cell nominal rates ----
fitg <- function(g) {
  if (nrow(g) < 30) return(tibble(slope = NA_real_, on = NA_real_))
  r <- tryCatch(coef(rq(bill ~ av, tau = .5, data = g)), error = function(e) c(NA, NA))
  tibble(slope = r[2], on = mean(abs(g$bill - (r[1] + r[2] * g$av)) / g$bill <= .01))
}
b <- d |> filter(bill > 0, av > 0)
r_non <- b |> filter(!hs) |> group_by(fips, cell) |> group_modify(~ fitg(.x)) |> ungroup() |> rename(s_non = slope, on_non = on)
r_all <- b |> group_by(fips, cell) |> group_modify(~ fitg(.x)) |> ungroup() |> rename(s_all = slope, on_all = on)
avmv <- b |> filter(!hs, mv > 0) |> group_by(fips, cell) |> summarise(avmv = median(av / mv), ratio_c = median(ratio_c), .groups = "drop")
rates <- full_join(r_non, r_all, by = c("fips", "cell")) |> left_join(avmv, by = c("fips", "cell")) |>
  mutate(rate = case_when(on_non >= .5 & s_non > 0 ~ s_non, on_all >= .5 & s_all > 0 ~ s_all),
         source = case_when(on_non >= .5 & s_non > 0 ~ "non-homestead fit", on_all >= .5 & s_all > 0 ~ "pooled fit"),
         rate_mv = rate * avmv * ratio_c, implausible = !is.na(rate) & (is.na(rate_mv) | rate_mv < .0005 | rate_mv > .06),
         rate = ifelse(implausible, NA, rate), rate_mv = ifelse(implausible, NA, rate_mv))
saveRDS(rates, file.path(OUT, "cell_nominal_rates_1pct.rds"))
d <- d |> left_join(rates |> select(fips, cell, rate, rate_mv, source), by = c("fips", "cell")) |>
  mutate(rate = 100 * rate, rate_mv = 100 * rate_mv)   # percent

# ---- 3. coverage ----
cov <- d |> summarise(parcels = n(), coords = mean(!is.na(g_state)), state = mean(!is.na(g_state)), county = mean(!is.na(g_county)),
  place = mean(!is.na(g_place)), unsd = mean(!is.na(g_unsd)), all4 = mean(!is.na(g_state) & !is.na(g_county) & !is.na(g_place) & !is.na(g_unsd)),
  has_rate = mean(!is.na(rate)), rate_and_all4 = mean(!is.na(rate) & !is.na(g_place) & !is.na(g_unsd) & !is.na(g_county)),
  rate_and_3_uninc = mean(!is.na(rate) & !is.na(g_unsd) & !is.na(g_county))) |> mutate(across(-parcels, \(x) round(100 * x, 1)))
cov_st <- d |> group_by(st) |> summarise(n = n(), all4 = round(100 * mean(!is.na(g_county) & !is.na(g_place) & !is.na(g_unsd)), 1),
  has_rate = round(100 * mean(!is.na(rate)), 1), both = round(100 * mean(!is.na(rate) & !is.na(g_county) & !is.na(g_place) & !is.na(g_unsd)), 1)) |> arrange(desc(n))

# ---- 4. variance decomposition ----
layers <- c("g_state", "g_county", "g_place", "g_unsd")
fe_r2 <- function(df, y, fes) if (!length(fes)) 0 else fixest::r2(feols(as.formula(paste(y, "~ 1 |", paste(fes, collapse = "+"))), df, warn = FALSE, notes = FALSE), "r2")
decomp <- function(df, y, lab) {
  df <- df |> filter(!is.na(.data[[y]]), is.finite(.data[[y]]))
  subs <- lapply(0:15, \(m) layers[bitwAnd(m, c(1, 2, 4, 8)) > 0])
  key <- \(s) if (!length(s)) "none" else paste(sort(s), collapse = ",")
  R <- setNames(sapply(subs, \(s) fe_r2(df, y, s)), sapply(subs, key))
  shap <- sapply(layers, \(l) sum(sapply(Filter(\(s) !(l %in% s), subs), \(s)
            factorial(length(s)) * factorial(3 - length(s)) / factorial(4) * (R[[key(c(s, l))]] - R[[key(s)]]))))
  nest <- c(R[[key("g_state")]], R[[key(c("g_state", "g_county"))]] - R[[key("g_state")]],
            R[[key(c("g_state", "g_county", "g_place"))]] - R[[key(c("g_state", "g_county"))]], R[[key(layers)]] - R[[key(c("g_state", "g_county", "g_place"))]])
  nest2 <- c(nest[1:2], R[[key(layers)]] - R[[key(c("g_state", "g_county", "g_unsd"))]], R[[key(c("g_state", "g_county", "g_unsd"))]] - R[[key(c("g_state", "g_county"))]])
  tibble(sample = lab, outcome = y, N = nrow(df), sd = round(sd(df[[y]]), 3), layer = c("state", "county", "place", "school district", "residual (within place x district)"),
         nested_pct = round(100 * c(nest, 1 - R[[key(layers)]]), 1),
         nested_district_first_pct = round(100 * c(nest2, 1 - R[[key(layers)]]), 1), shapley_pct = round(100 * c(shap, 1 - R[[key(layers)]]), 1))
}
A <- d |> filter(!is.na(rate), !is.na(g_county), !is.na(g_place), !is.na(g_unsd))
U <- d |> filter(!is.na(rate), !is.na(g_county), !is.na(g_unsd)) |> mutate(g_place = coalesce(g_place, paste0("uninc_", g_county)))
res <- bind_rows(decomp(A, "rate", "all four layers"), decomp(A, "rate_mv", "all four layers"),
                 decomp(U, "rate", "+ unincorporated as county-level place"), decomp(U, "rate_mv", "+ unincorporated as county-level place"),
                 decomp(A |> mutate(rate_mv = rate_mv - ave(rate_mv, g_state)), "rate_mv", "all four, state-demeaned (within-state only)"))
write.csv(res, file.path(OUT, "rate_variance_decomp.csv"), row.names = FALSE)
sink(file.path(OUT, "rate_variance_decomp.txt"))
cat("Coverage (% of parcels in the 1% sample):\n"); print(as.data.frame(cov), row.names = FALSE)
cat("\nRate sources (cells):\n"); print(table(rates$source, rates$implausible, useNA = "ifany", dnn = c("source", "dropped implausible")))
cat("\nCell rates kept (%):\n"); print(round(100 * quantile(rates$rate, c(.01, .1, .5, .9, .99), na.rm = TRUE), 2)); print(round(100 * quantile(rates$rate_mv, c(.01, .1, .5, .9, .99), na.rm = TRUE), 2))
cat("\nBy state (all4 = in a county, an incorporated place and a unified district; rate = cell rate available; both):\n")
print(as.data.frame(cov_st), row.names = FALSE)
cat("\nVariance decomposition (rate in % of AV; rate_mv in % of market value). nested = state, then county, then place, then\n",
    "district; shapley = order-free average contribution. Residual = variation across tax areas within place x district.\n")
for (s in split(res, paste(res$sample, res$outcome))) { cat("\n--", s$sample[1], "|", s$outcome[1], "| N =", s$N[1], "| sd =", s$sd[1], "\n")
  print(as.data.frame(s |> select(layer, nested_pct, nested_district_first_pct, shapley_pct)), row.names = FALSE) }
sink(); cat(readLines(file.path(OUT, "rate_variance_decomp.txt")), sep = "\n")
