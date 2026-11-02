# Shared by 13_near_border_event_study.R and 14_null_robustness.R (sourced after code/R/paths.R). Loads the yearly panels once
# and merges them with a tract table `d` (low side, tract / pair / cty / leaid / pop plus instrument columns `zs`):
#   nb_panels(d, zs) -> list of data.tables with columns y, yv-able outcomes, pair, cty, the instruments, and the unit id:
#     cl (CoreLogic tract-year 2012-24), ac (ACS 5-yr tract 2013-23), mv (Data Axle move-outs 2013-21),
#     f (F-33 districts FY2012-23), sd (CoG school districts by county 2012-22), ar (CoG all local govts, 2012/2017/2022).
#   Districts and counties get the population-weighted mean of each instrument over their tracts in `d`, and enter when
#   >= 50% of their population (all tracts of iv_tract_side) is in `d`.
#   es(p, z, fe_unit, cl, ref) -> yearly coefficients of z x year (ref = 0) with unit FE + pair x year FE, SE by `cl`.
.nb <- new.env()
.nb$allt <- readRDS(file.path(DATA, "built/geo/iv_tract_side.rds"))[T == 30, .(tract, leaid, cty, pop)]
.nb$cl <- readRDS(file.path(DATA, "built/corelogic/tract_year.rds"))[n >= 50 & y <= 2024, .(tract, y, n, lp50 = log(p50), lbill = log(bill / n), lav = log(av / n), eff50 = 100 * eff50, turn = 100 * n_sales / n)]
.nb$ac <- readRDS(file.path(DATA, "built/acs/acs_tract_year.rds"))[, .(tract, y, linc, lhval, lpop, sh_own)]
.nb$mv <- { m <- dq(sprintf("SELECT * FROM '%s/data/built/dataaxle/border_moveout/moveout_tract_full.parquet'", ROOT))
  cut <- c(`2012` = 3, `2013` = 15, `2014` = 27, `2015` = 40, `2016` = 54, `2017` = 65, `2018` = 79, `2019` = 90, `2020` = 102, `2021` = 114)
  merge(m, data.table(y = 2013:2021, months = diff(cut)), by = "y")[n >= 50][, .(tract, y, moveout = 100 * moves / n * 12 / months)] }
.nb$f <- { f <- readRDS(file.path(DATA, "built/census_finances/district_finance.rds"))[enr > 0, .(leaid, y, cty = conum, lptax = log(ptax / enr), lloc = log(loc / enr), lst = log(strev / enr), lspend = log(spend / enr), lenr = log(enr))]
  f[!is.finite(lptax), lptax := NA]; f }
.nb$cog <- { con <- db_open("census_finances_test.db"); lg <- function(x) ifelse(x > 0, log(x), NA_real_)
  sd <- as.data.table(dbGetQuery(con, "SELECT lpad(county_fips::VARCHAR, 5, '0') cty, year y, property_tax_rev, total_tax_rev, total_expenditure FROM govt_finances_sd_by_county"))
  ar <- as.data.table(dbGetQuery(con, "SELECT lpad(county_fips::VARCHAR, 5, '0') cty, year y, property_tax_rev, total_tax_rev, total_expenditure FROM govt_finances_county_area WHERE year IN (2012, 2017, 2022)"))
  dbDisconnect(con, shutdown = TRUE)
  list(sd = sd[, .(cty, y, lptax = lg(property_tax_rev), ltax = lg(total_tax_rev), lexp = lg(total_expenditure))],
       ar = ar[, .(cty, y, lptax = lg(property_tax_rev), ltax = lg(total_tax_rev), lexp = lg(total_expenditure))]) }
nb_panels <- function(d, zs) {
  tr <- d[, c("tract", "pair", "cty", zs), with = FALSE]
  agg <- function(key) { u <- merge(.nb$allt[!is.na(get(key)), .(pop_all = sum(pop, na.rm = TRUE)), by = key],
      d[!is.na(get(key)), c(list(pop_in = sum(pop, na.rm = TRUE), pair = names(which.max(table(pair)))), lapply(.SD, \(z) weighted.mean(z, pop, na.rm = TRUE))), by = key, .SDcols = zs], by = key)
    u[pop_in / pop_all >= .5][, c(key, "pair", zs), with = FALSE] }
  dis <- agg("leaid"); cnt <- agg("cty")
  list(cl = merge(.nb$cl, tr, by = "tract"), ac = merge(.nb$ac, tr, by = "tract"), mv = merge(.nb$mv, tr, by = "tract"),
       f = merge(.nb$f, dis, by = "leaid"), sd = merge(.nb$cog$sd, cnt, by = "cty"), ar = merge(.nb$cog$ar, cnt, by = "cty"),
       dis = dis, cnt = cnt)   # unit-level instruments, for merging other district / county panels (18)
}
es <- function(p, z, fe_unit, cl, ref = 2017, extra = "") { x <- p[is.finite(yv)]; if (uniqueN(x$y) < 3) return(NULL)
  m <- feols(as.formula(sprintf("yv ~ i(y, %s, ref = %d)%s | %s + pair^y", z, ref, extra, fe_unit)), x, cluster = as.formula(paste0("~", cl)))
  ct <- as.data.table(coeftable(m), keep.rownames = "term")[grepl(paste0("^y::\\d+:", z, "$"), term)]
  rbind(ct[, .(y = as.integer(sub("y::(\\d+):.*", "\\1", term)), b = Estimate, se = `Std. Error`)], data.table(y = ref, b = 0, se = 0))[, `:=`(z = z, n = nobs(m))] }
# ---- extended district / county outcome panels (the set of 18), merged with the unit-level instrument tables dis / cnt that
# nb_panels() returns: nb_extra_panels(dis, cnt) -> list(panels = list(f, se, sd, ar), vars = list(f = c(...), ...), src = c(...)).
# F-33 items logged per pupil (0 -> NA), CoG items logged per capita (ACS 2013-17 county population); SEDA in levels.
nb_extra_panels <- function(dis, cnt) {
  lg <- function(x) ifelse(is.finite(x) & x > 0, log(x), NA_real_)
  fv <- c(totrev = "total revenue", fedrev = "federal revenue", title1 = "Title I (C14)", idea = "IDEA special ed (C15)", nutrition = "child nutrition (C25)",
          st_formula = "state formula aid (C01)", st_sped = "state special ed (C05)", st_transp = "state transportation (C06)", st_comp = "state compensatory (C07)",
          loc_parent = "parent govt contributions (T02)", loc_othertax = "other local taxes (T99)", loc_cities = "from cities/counties (D23)", tuition = "tuition from pupils (A07)",
          lunch = "school lunch charges (A09)", activity = "district activity receipts (A13)", fees = "student fees (A15)", private_contrib = "private contributions (U50)",
          totexp = "total expenditure", inst = "instruction", ssvc = "support services", pupil_support = "pupil support (E17)", staff_support = "instr. staff support (E07)",
          gen_admin = "general administration (E08)", sch_admin = "school administration (E09)", om = "operations & maintenance (V40)", transp = "pupil transportation (V45)",
          food = "food services (E11)", capout = "capital outlay", construction = "construction (F12)", equip = "equipment (K09+K10)", pay_private = "payments to private schools (V91)",
          pay_charter = "payments to charter schools (V92)", pay_othersys = "payments to other school systems (Q11)", interest = "interest on debt (I86)",
          salaries = "salaries & wages (Z32)", benefits = "employee benefits (Z33)", inst_salaries = "instruction salaries (V11)", debt_end = "long-term debt outstanding (_41F)",
          debt_issued = "long-term debt issued (_21F)", cash = "cash & securities (W01+W31+W61)")
  f <- readRDS(file.path(DATA, "built/census_finances/district_finance.rds"))[enr > 0]
  for (k in names(fv)) f[, (k) := lg(get(k) / enr)]
  f <- merge(f[, c("leaid", "y", "conum", names(fv)), with = FALSE], dis, by = "leaid"); setnames(f, "conum", "cty")
  se <- merge(readRDS(file.path(DATA, "built/acs/seda_district_year.rds"))[y >= 2012, .(leaid, y, ach, ses)], dis, by = "leaid")
  se <- merge(se, unique(f[, .(leaid, cty)]), by = "leaid", all.x = TRUE)[is.na(cty), cty := substr(leaid, 1, 2)]
  sv <- c(ach = "district test scores (SEDA, SD)", ses = "district SES (SEDA)")
  con <- db_open("census_finances_test.db")
  sd <- as.data.table(dbGetQuery(con, "SELECT lpad(county_fips::VARCHAR, 5, '0') cty, year y, federal_ig_rev, state_ig_rev, total_capital_outlay, lt_debt_outstanding, lt_debt_issued, cash_securities, interest_on_debt FROM govt_finances_sd_by_county"))
  ar <- as.data.table(dbGetQuery(con, "SELECT lpad(county_fips::VARCHAR, 5, '0') cty, year y, general_sales_tax_rev, ind_income_tax_rev, federal_ig_rev, state_ig_rev, other_own_source, enterprise_rev,
    total_capital_outlay, lt_debt_outstanding, lt_debt_issued, interest_on_debt, n_units, n_municipal, n_special_dist,
    amount_e12 e_educ, amount_e24 e_fire, amount_e32 e_health, amount_e36 e_hospitals, amount_e44 e_highways, amount_e50 e_housing, amount_e52 e_libraries, amount_e61 e_parks, amount_e62 e_police,
    amount_e74 + amount_e75 + amount_e77 + amount_e79 e_welfare, amount_e80 e_sewer, amount_e81 e_solidwaste, amount_e89 e_general
    FROM govt_finances_county_area WHERE year IN (2012, 2017, 2022)")); dbDisconnect(con, shutdown = TRUE)
  cpop <- fread(file.path(DATA, "external/acs/acs5_tract_2017.csv"), colClasses = c(GEOID = "character"))[, .(cpop = sum(popE, na.rm = TRUE)), by = .(cty = substr(GEOID, 1, 5))]
  sdv <- c(federal_ig_rev = "school districts: federal revenue", state_ig_rev = "school districts: state revenue", total_capital_outlay = "school districts: capital outlay",
           lt_debt_outstanding = "school districts: LT debt outstanding", lt_debt_issued = "school districts: LT debt issued", cash_securities = "school districts: cash & securities", interest_on_debt = "school districts: interest on debt")
  arv <- c(general_sales_tax_rev = "all local govts: general sales tax", ind_income_tax_rev = "all local govts: individual income tax", federal_ig_rev = "all local govts: federal revenue",
           state_ig_rev = "all local govts: state revenue", other_own_source = "all local govts: other own-source revenue", enterprise_rev = "all local govts: enterprise revenue",
           total_capital_outlay = "all local govts: capital outlay", lt_debt_outstanding = "all local govts: LT debt outstanding", lt_debt_issued = "all local govts: LT debt issued", interest_on_debt = "all local govts: interest on debt",
           e_educ = "spending: education (E12)", e_fire = "spending: fire (E24)", e_health = "spending: health (E32)", e_hospitals = "spending: hospitals (E36)", e_highways = "spending: highways (E44)",
           e_housing = "spending: housing & comm. dev. (E50)", e_libraries = "spending: libraries (E52)", e_parks = "spending: parks (E61)", e_police = "spending: police (E62)",
           e_welfare = "spending: public welfare (E74-79)", e_sewer = "spending: sewerage (E80)", e_solidwaste = "spending: solid waste (E81)", e_general = "spending: general government (E89)",
           n_units = "number of local governments", n_municipal = "number of municipalities", n_special_dist = "number of special districts")
  sd <- merge(merge(sd, cpop, by = "cty"), cnt, by = "cty"); for (k in names(sdv)) sd[, (k) := lg(get(k) / cpop)]
  ar <- merge(merge(ar, cpop, by = "cty"), cnt, by = "cty"); for (k in setdiff(names(arv), c("n_units", "n_municipal", "n_special_dist"))) ar[, (k) := lg(get(k) / cpop)]
  list(panels = list(f = f, se = se, sd = sd, ar = ar), vars = list(f = fv, se = sv, sd = sdv, ar = arv), units = c(f = "leaid", se = "leaid", sd = "cty", ar = "cty"),
       src = c(f = "F-33 district, log per pupil", se = "SEDA district", sd = "CoG school districts by county, log per capita", ar = "CoG all local govts, census years, log per capita"))
}
