# (1) Which F-33 (Annual Survey of School System Finances, district files elsecYY, FY2012-2023) fields are filled
#     consistently? The public file has no missing codes for most items; a 0 is either a true zero or not applicable. Per
#     field and year: share of district records non-missing and share non-zero; stability = min/max of the non-zero share
#     across years and share of always-present districts that are non-zero in every year.
# (2) Property tax by state, FY/tax year 2017, three sources on the same counties (those with CoreLogic bills in 2017):
#     CoreLogic: sum of 2017 tax bills over all parcels on the roll (county_year.rds from the full roll, all property types);
#     Census of Governments 2017: property tax of all local governments in the county area (census year);
#     F-33 FY2017: school-district property tax (T06), and T06 + parent-government contributions (T02), districts mapped
#     to counties by CONUM. Ratios to the CoreLogic total.
suppressPackageStartupMessages({library(data.table); library(duckdb); library(ggplot2)})
ROOT <- "/Users/jr2728/PhD/projects/property_taxes"; OUT <- file.path(ROOT, "output/analysis/salt_local"); E <- file.path(ROOT, "data/external/elsec")
rd <- function(yy) { if (yy == 22) x <- as.data.table(readxl::read_excel(file.path(E, "elsec22.xlsx"), col_types = "text")) else {
  fn <- file.path(E, sprintf("elsec%02d.txt", yy)); hd <- gsub('"', "", strsplit(readLines(fn, 1), ",")[[1]])
  x <- fread(fn, skip = 1, header = FALSE, colClasses = "character", fill = TRUE); setnames(x, seq_along(hd), hd)
  x <- x[, ..hd] }
  x[, y := 2000L + yy][] }
F <- rbindlist(lapply(12:23, rd), fill = TRUE)
F <- F[!substr(sprintf("%05d", as.integer(CONUM)), 1, 2) %in% c("02", "15")]   # lower 48 + DC (STATE is the Census state code, not FIPS)
id <- c("STATE", "IDCENSUS", "PID6", "UNIT_TYPE", "FIPST", "NAME", "CONUM", "CSA", "CBSA", "SCHLEV", "NCESID", "YRDATA", "y")
id <- intersect(id, names(F))
num <- setdiff(names(F), id)
for (v in num) set(F, j = v, value = suppressWarnings(as.numeric(F[[v]])))
lab <- c(V33 = "fall enrolment", TOTALREV = "total revenue", TFEDREV = "federal revenue", TSTREV = "state revenue", TLOCREV = "local revenue",
  T02 = "parent-government contributions", T06 = "property taxes", T09 = "general sales taxes", T15 = "public utility taxes",
  T40 = "individual + corporate income taxes", T99 = "other taxes", D11 = "from other school systems", D23 = "from cities and counties",
  A07 = "tuition (pupils)", A09 = "school lunch", A11 = "transportation fees", A15 = "school activity receipts",
  U11 = "interest earnings", C01 = "state: general formula aid",
  C05 = "state: special education", C07 = "state: transportation", 
  C14 = "fed: Title I", C15 = "fed: children with disabilities", B11 = "fed: direct, impact aid",
  TOTALEXP = "total expenditure", TCURELSC = "current spending, el-sec", TCURINST = "current spending, instruction",
  Z32 = "salaries and wages, total", Z34 = "employee benefits, total", TCAPOUT = "capital outlay",
  F12 = "construction", I86 = "interest on debt", `_19H` = "LT debt outstanding, start", `_41F` = "LT debt outstanding, end",
  `_21F` = "LT debt issued", `_31F` = "LT debt retired", `_61V` = "ST debt, start", `_66V` = "ST debt, end",
  W01 = "assets: sinking funds", W31 = "assets: bond funds", W61 = "assets: other funds")
L <- melt(F[, c("y", "NCESID", num), with = FALSE], id.vars = c("y", "NCESID"), variable.factor = FALSE)   # NCESID: the one id present every year
fill <- L[, .(nonmiss = mean(!is.na(value)), nonzero = mean(!is.na(value) & value != 0)), by = .(field = variable, y)]
always <- F[, .N, by = NCESID][N == uniqueN(F$y), NCESID]
stab <- L[NCESID %in% always, .(all_years = all(!is.na(value) & value != 0)), by = .(field = variable, NCESID)][, .(sh_nonzero_all_yrs = mean(all_years)), by = field]
fs <- fill[, .(nonmiss_min = min(nonmiss), nonzero_mean = mean(nonzero), nonzero_min = min(nonzero), nonzero_max = max(nonzero),
               yrs_present = sum(nonmiss > 0)), by = field]
fs <- merge(fs, stab, by = "field", all.x = TRUE)
fs[, label := fifelse(field %in% names(lab), lab[field], "")]
added <- fs[yrs_present < 12, field]
fs <- fs[yrs_present == 12 | field %in% names(lab)][order(-nonzero_mean)][, nonmiss_min := fifelse(yrs_present < 12, NA_real_, nonmiss_min)]

fwrite(fs, file.path(OUT, "c11_f33_field_fill.csv"))
sink(file.path(OUT, "c11_f33_fill_and_ptax_compare.txt"), split = TRUE)
cat("F-33 district records FY2012-2023 (lower 48 + DC):", nrow(F), "| per year:", paste(range(F[, .N, by = y]$N), collapse = "-"),
    "| fields:", length(num), "| districts present every year:", length(always), "\n")
cat("Fields non-missing in every record of every year:", fs[nonmiss_min == 1, .N], "of", nrow(fs), "| fields added after 2019 (COVID relief etc.):",
    paste(added, collapse = " "), "\n\n")
pr <- fs[, .(field, label, nonzero_mean = round(100 * nonzero_mean, 1), nonzero_min = round(100 * nonzero_min, 1),
             nonzero_max = round(100 * nonzero_max, 1), nonzero_every_year = round(100 * sh_nonzero_all_yrs, 1))]
cat("Share of district records with a non-zero value (%), fields present in all 12 years, sorted; labelled ones are the main items:\n")
print(pr, nrows = 200)

# ---- (2) property tax by state, 2017 ----
cy <- readRDS(file.path(ROOT, "data/local_juris/county_year.rds"))[y == 2017 & bill_all > 0, .(county = as.integer(fips), cl = bill_all / 1e3)]  # $k
con <- dbConnect(duckdb(), file.path(ROOT, "data/census_finances_test.db"), read_only = TRUE)
cog <- as.data.table(dbGetQuery(con, "SELECT county_fips::INT county, property_tax_rev cog FROM govt_finances_county_area WHERE year = 2017"))
dbDisconnect(con, shutdown = TRUE)
f17 <- F[y == 2017, .(county = as.integer(CONUM), t06 = fcoalesce(T06, 0), t02 = fcoalesce(T02, 0))][, .(t06 = sum(t06), t02 = sum(t02)), by = county]
# CT: the 2017 Census of Governments uses the new planning regions (091xx), so CT is compared as one state-wide unit
ct <- \(x) x[county %/% 1000 == 9, county := 9000L][, lapply(.SD, sum), by = county]
cy <- ct(cy); cog <- ct(cog); f17 <- ct(f17)
m <- merge(merge(cy, cog, by = "county"), f17, by = "county", all.x = TRUE)
m[is.na(t06), `:=`(t06 = 0, t02 = 0)][, st := sprintf("%02d", county %/% 1000)]
stc <- m[, .(cl = sum(cl), cog = sum(cog), t06 = sum(t06), t02 = sum(t02), n_cty = .N), by = st]
ncty <- cog[, .N, by = .(st = sprintf("%02d", county %/% 1000))]
stc <- merge(stc, ncty, by = "st")[, cty_cov := n_cty / N]
fips <- unique(as.data.table(tigris::fips_codes)[, .(st = state_code, ab = state)])
stc <- merge(stc, fips, by = "st")[!ab %in% c("AK", "HI")]
stc[, `:=`(r_cog = cog / cl, r_t06 = t06 / cl, r_t0602 = (t06 + t02) / cl, sh_school = t06 / cog)]
fwrite(stc, file.path(OUT, "c11_ptax_state_compare.csv"))
cat("\n== Property tax 2017 by state, counties with CoreLogic bills ($bn) and ratios\n")
cat("US totals ($bn): CoreLogic", round(sum(stc$cl) / 1e6, 1), "| CoG all local", round(sum(stc$cog) / 1e6, 1), "| F-33 school T06",
    round(sum(stc$t06) / 1e6, 1), "| T06 + T02", round(sum(stc$t06 + stc$t02) / 1e6, 1), "\n")
print(stc[order(-r_t06), .(ab, cty_cov = round(cty_cov, 2), cl_bn = round(cl / 1e6, 2), cog_bn = round(cog / 1e6, 2), t06_bn = round(t06 / 1e6, 2),
  r_cog = round(r_cog, 2), r_t06 = round(r_t06, 2), r_t0602 = round(r_t0602, 2))], nrows = 60)
sink()
pl <- melt(stc[, .(ab, `All local governments (Census of Governments)` = r_cog, `School districts: property tax (F-33 T06)` = r_t06,
                   `School districts: T06 + parent-government contributions (T02)` = r_t0602)], id.vars = "ab")
pl[, ab := factor(ab, stc[order(r_t06), ab])]
g <- ggplot(pl, aes(value, ab, colour = variable, shape = variable)) + geom_vline(xintercept = 1, colour = "grey50") +
  geom_point(size = 2) + scale_colour_manual(NULL, values = c("#7f7f7f", "#2a64a8", "#d1772b")) + scale_shape_manual(NULL, values = c(4, 16, 1)) +
  scale_x_continuous(breaks = seq(0, 2.5, .25)) + coord_cartesian(xlim = c(0, 2)) +
  labs(x = "property tax relative to CoreLogic 2017 tax bills (sum over all parcels) in the same counties",
       y = NULL, title = "Property tax 2017 by state: Census sources relative to CoreLogic bills",
       subtitle = "Counties with CoreLogic bills in 2017. CoreLogic = all property tax billed on real-property parcels (all levying units).\nSchool share = F-33 school-district property tax / CoreLogic. States sorted by the school share; 0 = dependent school systems.") +
  theme_minimal(base_size = 9) + theme(legend.position = "bottom", legend.direction = "vertical")
ggsave(file.path(OUT, "figures/c11_ptax_state_compare.png"), g, width = 8, height = 10, dpi = 150)
