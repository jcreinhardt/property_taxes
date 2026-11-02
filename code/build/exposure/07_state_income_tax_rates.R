# Effective state individual income tax rates by state and tax year 2012-2022, three measures -> data/built/exposure/state_income_tax_rates.csv
#   eff_stc  : collections / AGI. State government individual income tax (Census State Tax Collections, item T40; NA = no tax
#              = 0; DC is not in the STC database, so its eff_stc is NA), averaged over the two fiscal years that span tax year t (FY t and FY t+1, most FYs end June 30), divided
#              by SOI AGI of the state's residents in tax year t (state returns files, A00100, all AGI stubs). Moves with
#              law and with the income mix (capital gains make it rise in 2021).
#   eff_acs  : TAXSIM-imputed state income tax on each year's ACS households (acs_salt.stateinctax from build/acs/02), summed
#              over the state, over total personal income of the same households. Law x that year's residents.
#   eff_law  : law only. One fixed national sample of 2016 ACS households (all tenures, heads with spouse income, children,
#              no itemised deductions) is pushed through every state's income tax for every year 2012-2022 with TAXSIM 35
#              (usincometaxes, WebAssembly); eff_law = sum of state tax / sum of income, eff_law_top the same for the top
#              10% of sample households by income, mtr_law_top their mean state marginal rate on wages. Varies only with
#              state tax law; the sample is the same everywhere, so differences across state-years are law differences.
# Inputs: data/external/stc/STC_Historical_DB_2025.xlsx (Census STC historical database, April 2026 release, downloaded
#   2026-10-27 from www2.census.gov/programs-surveys/stc/datasets/historical/State-Government-Tax-Collections.zip);
#   data/SOI/state_returns/{12..22}in5[45]cm*.csv|xlsx (SOI state returns by AGI stub); data/database.db (acs_salt, acs_microdata).
# Cache: data/built/exposure/taxsim_state_law.csv (the TAXSIM run, ~20 min); delete it to rerun.
suppressPackageStartupMessages({library(data.table); library(readxl); library(DBI); library(usincometaxes)})
source("code/R/paths.R"); BUILT <- file.path(DATA, "built/exposure"); OUT <- out_dir("build/exposure")
yrs <- 2012:2022; M_LAW <- 3000L; set.seed(20261027)
fips <- data.table(s = c(1,2,4,5,6,8,9,10,11,12,13,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,44,45,46,47,48,49,50,51,53,54,55,56),
                   state = c("AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"))

# ---- 1. Census STC: individual income tax by state and fiscal year ------------------------------------------------------------
f <- file.path(DATA, "external/stc/STC_Historical_DB_2025.xlsx")
stc <- as.data.table(read_excel(f, skip = 1, col_names = names(read_excel(f, n_max = 0)), col_types = "text"))[!is.na(Year)]
stc <- stc[, .(fy = as.integer(Year), state = substr(Name, 1, 2), fy_end = `FY Ending Date`, t40 = fcoalesce(as.numeric(T40), 0), total_tax = as.numeric(C105))][state != "US"]
stopifnot(stc[fy %in% 2012:2023, .N] == 50 * 12, all(stc$state %in% fips$state))   # 50 states; DC is not in the STC database
cat("STC: 7 states with no individual income tax (T40 = 0 or NA):", stc[fy == 2019 & t40 == 0, paste(state, collapse = " ")], "\n")

# ---- 2. SOI state returns: AGI by state and tax year ----------------------------------------------------------------------------
rd_soi <- function(y) { yy <- y - 2000
  fs <- list.files(file.path(DATA, "SOI/state_returns"), pattern = sprintf("^%02din5[45]cm", yy), full.names = TRUE); stopifnot(length(fs) == 1)
  if (grepl("xlsx$", fs)) { x <- as.data.table(read_excel(fs, col_names = FALSE, col_types = "text", .name_repair = "minimal")); h <- which(toupper(x[[1]]) == "STATE")[1]
    setnames(x, as.character(unlist(x[h]))); x <- x[-(1:h)] } else x <- fread(fs, colClasses = "character")
  setnames(x, toupper(names(x))); x[AGI_STUB == "0" & STATE %in% fips$state, .(state = STATE, y = y, n1 = as.numeric(gsub(",", "", N1)), agi = as.numeric(gsub(",", "", A00100)))] }
soi <- rbindlist(lapply(yrs, rd_soi)); stopifnot(nrow(soi) == 51 * length(yrs), !anyNA(soi))
cat("SOI AGI ($bn): CA", soi[state == "CA", paste(y, round(agi / 1e6))], "\n")

# ---- 3. ACS households x law, each year ---------------------------------------------------------------------------------------
con <- db_open("database.db")
acs <- as.data.table(dbGetQuery(con, "SELECT year y, state, sum(hhwt) hh, sum(stateinctax * hhwt) tax, sum(inctot * hhwt) inc
  FROM acs_salt WHERE stateinctax IS NOT NULL AND inctot > 0 AND year BETWEEN 2012 AND 2022 GROUP BY 1, 2"))[, eff_acs := 100 * tax / inc]
stopifnot(nrow(acs) == 51 * length(yrs))

# ---- 4. Law only: a fixed 2016 national sample through every state's law, every year -----------------------------------------
f_law <- file.path(BUILT, "taxsim_state_law.csv")
if (!file.exists(f_law)) {
  hh <- as.data.table(dbGetQuery(con, "SELECT h.serial, h.hhwt, h.age, h.marst, h.nchild,
      CASE WHEN h.incwage = 9999999 THEN 0 ELSE h.incwage END wage_h, CASE WHEN h.inctot = 9999999 THEN 0 ELSE h.inctot END inc_h, s.sage, s.wage_s, s.inc_s
    FROM acs_microdata h LEFT JOIN (SELECT year, serial, min(age) sage, sum(CASE WHEN incwage = 9999999 THEN 0 ELSE incwage END) wage_s,
      sum(CASE WHEN inctot = 9999999 THEN 0 ELSE inctot END) inc_s FROM acs_microdata WHERE relate = 2 AND year = 2016 GROUP BY 1, 2) s USING (year, serial)
    WHERE h.year = 2016 AND h.pernum = 1 AND h.gq IN (1, 2) AND h.statefip <= 56"))
  hh[, married := marst == 1L]
  hh[, `:=`(mstat = fifelse(married, "married, jointly", fifelse(nchild > 0, "head of household", "single")), page = as.integer(age),
            sage = fifelse(married, fcoalesce(as.integer(sage), as.integer(age)), 0L), depx = pmin(nchild, 9L), pwages = pmax(0, wage_h),
            swages = fifelse(married, pmax(0, fcoalesce(wage_s, 0)), 0))]
  hh[, otherprop := pmax(0, inc_h + fifelse(married, fcoalesce(inc_s, 0), 0) - pwages - swages)]
  hh[, `:=`(age1 = fifelse(depx >= 1, 10L, 0L), age2 = fifelse(depx >= 2, 10L, 0L), age3 = fifelse(depx >= 3, 10L, 0L), proptax = 0, mortgage = 0)]
  smp <- hh[sample(.N, M_LAW, replace = TRUE, prob = hhwt)][, hid := .I][, income := pwages + swages + otherprop]
  smp[, top := income >= quantile(income, 0.9)]
  cat("law-only sample:", nrow(smp), "households; mean income", round(mean(smp$income)), "; top-10% cut", round(quantile(smp$income, .9)), "\n")
  g <- CJ(hid = smp$hid, state = fips$state, year = yrs)
  g <- merge(g, smp[, .(hid, mstat, page, sage, depx, age1, age2, age3, pwages, swages, otherprop, proptax, mortgage)], by = "hid")[, taxsimid := .I]
  cols <- c("taxsimid", "year", "state", "mstat", "page", "sage", "depx", "age1", "age2", "age3", "pwages", "swages", "otherprop", "proptax", "mortgage")
  out <- vector("list", ceiling(nrow(g) / 100000L)); t0 <- Sys.time()
  for (i in seq_along(out)) { idx <- ((i - 1) * 100000L + 1):min(i * 100000L, nrow(g))
    r <- as.data.table(taxsim_calculate_taxes(as.data.frame(g[idx, ..cols]), marginal_tax_rates = "Wages", return_all_information = FALSE))
    out[[i]] <- r[, .(taxsimid, siitax, srate)]; cat(sprintf("TAXSIM chunk %d/%d, %.0f s\n", i, length(out), as.numeric(Sys.time() - t0, units = "secs"))) }
  law <- merge(g[, .(taxsimid, hid, state, year)], rbindlist(out), by = "taxsimid")
  law <- merge(law, smp[, .(hid, income, top)], by = "hid")
  fwrite(law[, .(hid, state, y = year, siitax, srate, income, top)], f_law) }
dbDisconnect(con, shutdown = TRUE)
law <- fread(f_law)
lw <- law[, .(eff_law = 100 * sum(siitax) / sum(income), eff_law_top = 100 * sum(siitax[top]) / sum(income[top]), mtr_law_top = mean(srate[top])), by = .(state, y)]

# ---- 5. Assemble ---------------------------------------------------------------------------------------------------------------
p <- merge(soi, stc[, .(state, y = fy, t40_fy = t40)], by = c("state", "y"), all.x = TRUE)
p <- merge(p, stc[, .(state, y = fy - 1L, t40_fy_next = t40)], by = c("state", "y"), all.x = TRUE)
p[, eff_stc := 100 * 0.5 * (t40_fy + t40_fy_next) / agi]
p <- merge(p, acs[, .(state, y, eff_acs)], by = c("state", "y"))
p <- merge(p, lw, by = c("state", "y"))
p <- merge(p, fips, by = "state")[order(state, y)]
stopifnot(nrow(p) == 51 * length(yrs), !anyNA(p[state != "DC"]), p[is.na(eff_stc), all(state == "DC")])
fwrite(p[, .(state, s, y, n1, agi, t40_fy, t40_fy_next, eff_stc, eff_acs, eff_law, eff_law_top, mtr_law_top)], file.path(BUILT, "state_income_tax_rates.csv"))

sink(file.path(OUT, "07_state_income_tax_rates.txt"), split = TRUE)
cat("Effective state income tax rates (% of income), 2012-2022. eff_stc = collections / SOI AGI; eff_acs = TAXSIM on each year's ACS households; eff_law = fixed 2016 sample, law only.\n\n")
cat("Correlation across state-years: levels and within-state first differences\n")
d <- p[state != "DC", .(y, d_stc = eff_stc - shift(eff_stc), d_acs = eff_acs - shift(eff_acs), d_law = eff_law - shift(eff_law)), by = state][!is.na(d_stc)]
print(round(cor(p[state != "DC", .(eff_stc, eff_acs, eff_law, eff_law_top)]), 2)); print(round(cor(d[, .(d_stc, d_acs, d_law)]), 2))
cat("\nSD of first differences (pp):", d[, sprintf("stc %.2f | acs %.2f | law %.2f", sd(d_stc), sd(d_acs), sd(d_law))], "\n")
cat("\nMeans by year:\n"); print(p[, .(eff_stc = round(mean(eff_stc, na.rm = TRUE), 2), eff_acs = round(mean(eff_acs), 2), eff_law = round(mean(eff_law), 2), eff_law_top = round(mean(eff_law_top), 2)), by = y])
cat("\nLargest law changes (eff_law, pp vs previous year):\n"); print(d[order(-abs(d_law))][1:20, .(state, y, d_law = round(d_law, 2), d_stc = round(d_stc, 2), d_acs = round(d_acs, 2))])
cat("\nLevels 2016 by state:\n"); print(p[y == 2016][order(-eff_law), .(state, eff_stc = round(eff_stc, 2), eff_acs = round(eff_acs, 2), eff_law = round(eff_law, 2), eff_law_top = round(eff_law_top, 2), mtr_law_top = round(mtr_law_top, 2))], nrows = 60)
sink()
