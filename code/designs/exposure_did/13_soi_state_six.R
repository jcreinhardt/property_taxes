# State-level interstate migration, the six "high-SALT" states of Diop & Green (2026, SSRN) vs the other 44 (2026-10-27).
# Diop & Green regress state-to-state SOI flows in levels on Post x HighSALT (HighSALT = CA NY CT PA MA IL, Post = their
# "2018-2019", pre = 2001-2017) with White SEs on dyad-years and find +49% emigration. Our exposure designs absorb all
# state-level variation with state x year FE, so this script estimates their comparison directly on the SOI state files:
#   y_st ~ i(year, high, ref = 2017) | state + year, weighted by 2016 returns, SE clustered by state (50 states, 6 treated).
# Outcomes per state-year from data/SOI/migration_state/state{out,in}flowYYYY.csv (files 1112-2122 -> years 2012-2022;
# year = second year of the file, i.e. the address change between the TY(y-1) and TY(y) returns, moves during ~y):
#   out-of-state returns ("Total Migration-US", n1), in-migrants, non-migrants; flows to the nine no-income-tax states
#   (AK FL NV NH SD TN TX WA WY); AGI of each group. Rates in % of (non-migrants + out-migrants).
# Also: the paper's 2x2 DiD (post 2018-19) with a long (2012-17) and a short (2016-17) pre-period, and the match of the
# paper's post-period CA/NY averages (Table A.2, x 49) to our file years, to see which SOI files their "2018-2019" are.
suppressPackageStartupMessages({library(data.table); library(fixest); library(ggplot2)})
source("code/R/paths.R"); OUT <- out_dir("designs/exposure_did"); invisible(out_dir("designs/exposure_did/figures")); M <- file.path(DATA, "SOI/migration_state")
yrs <- 12:22
rd <- \(kind, yy) { x <- fread(file.path(M, sprintf("state%s%02d%02d.csv", kind, yy - 1, yy)), colClasses = "character", encoding = "Latin-1")
  setnames(x, tolower(names(x))); x[, y := 2000L + yy][] }
nit <- c(2, 12, 32, 33, 46, 47, 48, 53, 56)                     # no-income-tax states
p <- rbindlist(lapply(yrs, \(yy) {
  o <- rd("outflow", yy); i <- rd("inflow", yy)
  o[, `:=`(s = as.integer(y1_statefips), d = as.integer(y2_statefips))]
  i[, `:=`(s = as.integer(y2_statefips), d = as.integer(y1_statefips))]
  st  <- o[d == s & grepl("Non-migrants", y2_state_name), .(s, y, nonmig = as.numeric(n1), agi_st = as.numeric(agi))]
  out <- o[d == 97 & grepl("Migration.US$", y2_state_name), .(s, out = as.numeric(n1), agi_out = as.numeric(agi))]   # 1112/1213 lack the hyphen
  inn <- i[d == 97 & grepl("Migration.US$", y1_state_name), .(s, inn = as.numeric(n1), agi_in = as.numeric(agi))]
  on  <- o[d %in% nit & d != s, .(out_nit = sum(as.numeric(n1))), by = s]
  dc  <- o[d == 11, .(s, out_dc = as.numeric(n1))]
  Reduce(\(a, b) merge(a, b, by = "s", all.x = TRUE), list(st, out, inn, on, dc)) }))[s <= 56]
p[, high := as.integer(s %in% c(6, 36, 9, 42, 25, 17))]
p[, base := nonmig + out]
p[, `:=`(r_out = 100 * out / base, r_net = 100 * (out - inn) / base, r_nit = 100 * out_nit / base, l_out = log(out),
         net_agi = 100 * (agi_out - agi_in) / agi_st, agi_pr_out = agi_out / out)]
p <- merge(p, p[y == 2016, .(s, w = base)], by = "s")                 # weights: 2016 returns
stopifnot(p[, uniqueN(y), by = s][, all(V1 == length(yrs))])

v <- c(l_out = "log out-of-state returns", r_out = "out-of-state rate (pp of returns)", r_net = "net out-migration rate (pp)",
       r_nit = "rate to no-income-tax states (pp)", net_agi = "net AGI outflow (% of stayers' AGI)")
es <- \(k) { m <- feols(as.formula(sprintf("%s ~ i(y, high, ref = 2017) | s + y", k)), p, weights = ~w, cluster = ~s)
  as.data.table(coeftable(m), keep.rownames = "t")[, .(var = k, label = v[k], y = as.integer(sub("y::(\\d+):high", "\\1", t)), b = Estimate, se = `Std. Error`)] }
res <- rbindlist(lapply(names(v), es))
res <- rbind(res, data.table(var = names(v), label = v, y = 2017L, b = 0, se = 0))[order(var, y)]
fwrite(res, file.path(OUT, "13_soi_state_six.csv"))
dd <- \(pre) { q <- p[y %in% c(pre, 2018:2019)][, post := as.integer(y >= 2018)]
  m <- feols(l_out ~ post:high | s + y, q, weights = ~w, cluster = ~s); sprintf("%+.3f (%.3f)", coef(m), se(m)) }
ab <- c(`6` = "CA", `36` = "NY", `17` = "IL", `9` = "CT", `25` = "MA", `42` = "PA", `34` = "NJ", `12` = "FL", `48` = "TX")
paper <- data.table(st = c("CA", "NY", "FL", "TX"), post49 = 49 * c(6.856, 5.066, 4.997, 4.231))   # Table A.2, post-TCJA emigration per destination state x 49
mt <- p[s %in% c(6, 36, 12, 48), .(st = ab[as.character(s)], y, x = out - out_dc)]
mt <- merge(paper, mt[, .(files_1718_1819 = mean(x[y %in% 2018:2019]), files_1819_1920 = mean(x[y %in% 2019:2020])), by = st], by = "st")

sink(file.path(OUT, "13_soi_state_six.txt"), split = TRUE)
cat("SOI state files 2012-2022 (file y-1 -> y), 50 states; high = CA NY CT PA MA IL (Diop & Green 2026); weights = 2016 returns; SE by state; ref 2017.\n\n")
cat("Raw: out-of-state rate (%) | net out-migration rate (%) | AGI per out-migrant return ($k)\n")
print(dcast(p[s %in% as.integer(names(ab))][, .(st = ab[as.character(s)], y, cell = sprintf("%.2f | %+.2f | %.0f", r_out, r_net, agi_pr_out))], st ~ y, value.var = "cell"))
cat("\nMeans by group and year: out rate | net rate | rate to no-income-tax states | net AGI outflow (% of stayers' AGI)\n")
print(dcast(p[, .(cell = sprintf("%.2f | %+.2f | %.2f | %+.2f", mean(r_out), mean(r_net), mean(r_nit), mean(net_agi))), by = .(high, y)], high ~ y, value.var = "cell"))
cat("\nEvent study, high x year (ref 2017) | state + year, weighted by 2016 returns, SE clustered by state:\n")
print(dcast(res[, .(label, y, cell = sprintf("%+.3f (%.3f)", b, se))], y ~ label, value.var = "cell"))
cat("\n2x2 DiD, log out-of-state returns, post = 2018-19: pre 2012-17", dd(2012:2017), "| pre 2016-17", dd(2016:2017), "\n")
cat("\nWhich SOI files are the paper's 'post' years? Paper's post-TCJA emigration per destination x 49 vs our out-of-state returns excl. DC:\n")
print(mt[, .(st, paper_post_x49_k = round(post49), files_1718_1819_k = round(files_1718_1819 / 1000), files_1819_1920_k = round(files_1819_1920 / 1000))])
cat("(the 1819 + 1920 pair matches CA and NY; file 1920 = address changes between the TY2019 and TY2020 returns, i.e. moves during 2020)\n")
cat("\nNote: file 1617 (our 2017) shows out-of-state rates ~30% above adjacent years in every state; year FE absorb it in logs.\n")
sink()

res[, label := factor(label, v)]
g <- ggplot(res, aes(y, b)) + geom_hline(yintercept = 0, colour = "grey60") + geom_vline(xintercept = 2017.5, linetype = 2, colour = "grey40") +
  geom_vline(xintercept = 2019.5, linetype = 3, colour = "grey60") +
  geom_ribbon(aes(ymin = b - 1.96 * se, ymax = b + 1.96 * se), fill = "#2a64a8", alpha = .15) + geom_line(colour = "#2a64a8") + geom_point(colour = "#2a64a8", size = 1) +
  facet_wrap(~ label, scales = "free_y", ncol = 2) + scale_x_continuous(breaks = 2012:2022) +
  labs(x = "migration year (address change between tax years y-1 and y)", y = "six high-SALT states vs other 44, relative to 2017",
       title = "Interstate migration from CA, NY, CT, PA, MA, IL vs the other states (IRS SOI state files)",
       subtitle = "state + year FE, weighted by 2016 returns; 95% CI, SE by state. Dashed: SALT cap; dotted: first COVID file (1920).") +
  theme_ptax(9)
ggsave(file.path(OUT, "figures/13_soi_state_six.png"), g, width = 10, height = 8, dpi = 150)
