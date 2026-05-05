gc()
rm(list = ls())

library(tidyverse)
library(duckdb)
library(ggplot2)
library(glmnet)
library(Matrix)
library(ranger)
library(binsreg)
library(fixest)
library(broom)
library(stringr)
library(doParallel)

n_cores <- 10
options(ranger.num.threads = n_cores)
registerDoParallel(cores = n_cores)

con <- dbConnect(duckdb::duckdb(), dbdir = "../../../data/database.db", read_only = TRUE)

period_colors <- c(
    "2012-2014" = "#23373b",
    "2015-2017" = "#4a7c82",
    "2018-2020" = "#c06028",
    "2021-2023" = "#eb811b"
)

########################################################
# 1. Sample counts
########################################################

# Income variables:
#   hhincome : household-level (repeats on every person row; restrict to pernum = 1)
#   inctot, ftotinc, incwage : person-level
#
# Coverage (verified in section 2): all variables have full coverage within
# their respective universes — apparent missingness is entirely due to out-of-
# universe codes (children under 15/16, group quarters residents), not true NAs.

sample_counts <- dbGetQuery(con, "
    SELECT
        year,
        COUNT(*)                                                    AS n_persons,
        COUNT(DISTINCT serial)                                      AS n_households
    FROM acs_microdata
    GROUP BY year
    ORDER BY year
")

print(sample_counts)

# Primary keys:
#   Person:    year + serial + pernum  (unique; verified below)
#   Household: year + serial
# Note: works because this extract contains only single-year ACS samples.
#   If multi-year samples are added, 'sample' must be included in the key.
pk_check <- dbGetQuery(con, "
    SELECT COUNT(*) AS n_rows,
           COUNT(DISTINCT year || '_' || serial || '_' || pernum) AS n_unique
    FROM acs_microdata
")
stopifnot(pk_check$n_rows == pk_check$n_unique)

# Sample restriction used throughout:
#   gq IN (1,2,5) : housing unit residents only
#                   1 = households (1970 def), 2 = additional HHs (1990 def),
#                   5 = additional HHs (2000 def); excludes GQ institutions (3,4)
#   age >= 15     : income universe (ACS does not ask income of under-15s)
#   pernum = 1    : one row per household (for household-level variables)
hh_sample_dummy <- "pernum = 1 AND age >= 15 AND gq IN (1, 2, 5)"

########################################################
# 2. Coverage
########################################################

# Variable levels:
#   hhincome : HOUSEHOLD-level — same value on every person row; use pernum = 1
#   inctot   : person-level
#   ftotinc  : person-level
#   incwage  : person-level
#
# N/A codes:
#   inctot / hhincome / ftotinc : 9999999 (7 nines)
#   incwage                     : 999999  (6 nines) -- different!
#
# Income universe: persons age 15+, housing units (gq IN (1,2,5))

coverage <- dbGetQuery(con, "
    SELECT
        year,

        -- denominators
        SUM(CASE WHEN age >= 15 AND gq IN (1,2,5) THEN 1 ELSE 0 END)      AS n_persons_universe,
        SUM(CASE WHEN pernum = 1 AND gq IN (1,2,5) THEN 1 ELSE 0 END)     AS n_households_universe,
        -- incwage universe: age 16+ in housing units (empstat = 0 flags under-16s)
        SUM(CASE WHEN age >= 16 AND gq IN (1,2,5) THEN 1 ELSE 0 END)      AS n_incwage_universe,

        -- valid counts (raw, unweighted)
        SUM(CASE WHEN age >= 15 AND gq IN (1,2,5)
                  AND inctot != 9999999 THEN 1 ELSE 0 END)               AS n_inctot,
        SUM(CASE WHEN pernum = 1 AND gq IN (1,2,5)
                  AND hhincome != 9999999 THEN 1 ELSE 0 END)             AS n_hhincome,
        SUM(CASE WHEN age >= 15 AND gq IN (1,2,5)
                  AND ftotinc != 9999999 THEN 1 ELSE 0 END)              AS n_ftotinc,
        SUM(CASE WHEN age >= 16 AND gq IN (1,2,5)
                  AND incwage != 999999 THEN 1 ELSE 0 END)               AS n_incwage
    FROM acs_microdata
    GROUP BY year
    ORDER BY year
")

coverage <- coverage |>
    mutate(
        pct_inctot   = n_inctot   / n_persons_universe,
        pct_hhincome = n_hhincome / n_households_universe,
        pct_ftotinc  = n_ftotinc  / n_persons_universe,
        pct_incwage  = n_incwage  / n_incwage_universe
    )

print(coverage |> select(year,
    n_persons_universe, n_inctot,   pct_inctot,
    n_households_universe, n_hhincome, pct_hhincome,
    n_ftotinc, pct_ftotinc,
    n_incwage_universe, n_incwage, pct_incwage
))


########################################################
# 3. Migration variables
########################################################

# All migration variables are person-level, universe age 1+.
# N/A codes (out-of-universe = same house or non-mover):
#   migrate1   : 0
#   migrate1d  : 0
#   migplac1   : 000
#   migcounty1 : 000  -- also 000 when county not identifiable; use migrate1 to distinguish
#   migpuma1   : 00000
#   migmet131  : 00000
#
# migrate1 values: 1=same house, 2=moved within state, 3=moved between states, 4=abroad
# migcounty1 is state-dependent: must combine with migplac1 to identify county

# 3a. Distribution of migrate1 by year
migration_dist <- dbGetQuery(con, "
    SELECT
        year,
        SUM(CASE WHEN migrate1 = 1 THEN 1 ELSE 0 END)  AS same_house,
        SUM(CASE WHEN migrate1 = 2 THEN 1 ELSE 0 END)  AS moved_within_state,
        SUM(CASE WHEN migrate1 = 3 THEN 1 ELSE 0 END)  AS moved_between_states,
        SUM(CASE WHEN migrate1 = 4 THEN 1 ELSE 0 END)  AS abroad,
        SUM(CASE WHEN migrate1 = 0 THEN 1 ELSE 0 END)  AS na_under1,
        COUNT(*)                                        AS n_total
    FROM acs_microdata
    GROUP BY year ORDER BY year
")

print(migration_dist)

# 3b. Coverage of geographic migration variables among movers (migrate1 IN (2,3,4))
migration_coverage <- dbGetQuery(con, "
    SELECT
        year,
        SUM(CASE WHEN migrate1 IN (2,3,4) THEN 1 ELSE 0 END)                        AS n_movers,

        -- migplac1: state/country of prior residence
        SUM(CASE WHEN migrate1 IN (2,3,4) AND migplac1 != 0 THEN 1 ELSE 0 END)      AS n_migplac1,

        -- migcounty1: prior county; 000 = non-mover OR unidentifiable county
        SUM(CASE WHEN migrate1 IN (2,3,4) AND migcounty1 != 0 THEN 1 ELSE 0 END)    AS n_migcounty1,

        -- migpuma1: prior migration PUMA
        SUM(CASE WHEN migrate1 IN (2,3,4) AND migpuma1 != 0 THEN 1 ELSE 0 END)      AS n_migpuma1,

        -- migmet131: prior metro area (2013 delineations)
        SUM(CASE WHEN migrate1 IN (2,3,4) AND migmet131 != 0 THEN 1 ELSE 0 END)     AS n_migmet131
    FROM acs_microdata
    GROUP BY year ORDER BY year
")

migration_coverage <- migration_coverage |>
    mutate(
        pct_migplac1   = n_migplac1   / n_movers,
        pct_migcounty1 = n_migcounty1 / n_movers,
        pct_migpuma1   = n_migpuma1   / n_movers,
        pct_migmet131  = n_migmet131  / n_movers
    )

print(migration_coverage)


########################################################
# 4. Share of movers by hhincome decile (2017 vs 2018)
########################################################

mover_by_income <- dbGetQuery(con, "
    WITH heads AS (
        SELECT
            CASE
                WHEN year BETWEEN 2012 AND 2014 THEN '2012-2014'
                WHEN year BETWEEN 2015 AND 2017 THEN '2015-2017'
                WHEN year BETWEEN 2018 AND 2020 THEN '2018-2020'
                WHEN year BETWEEN 2021 AND 2023 THEN '2021-2023'
            END AS period,
            NTILE(10) OVER (PARTITION BY year ORDER BY hhincome) AS decile,
            CASE WHEN migrate1 IN (2,3,4) THEN 1 ELSE 0 END AS mover
        FROM acs_microdata
        WHERE year BETWEEN 2012 AND 2023
          AND pernum = 1 AND gq IN (1,2,5) AND hhincome != 9999999
    )
    SELECT
        period,
        decile,
        AVG(mover)   AS share_movers,
        COUNT(*)     AS n_households
    FROM heads
    GROUP BY period, decile
    ORDER BY period, decile
")

mover_by_income |>
    mutate(period = factor(period, levels = names(period_colors))) |>
    ggplot(aes(x = decile, y = share_movers, color = period, group = period)) +
    geom_line() +
    geom_point() +
    scale_color_manual(name = NULL, values = period_colors) +
    scale_x_continuous(breaks = 1:10) +
    labs(x = "Household income decile (year-specific)", y = "Share of household heads who moved") +
    theme(
        panel.background = element_rect(fill = "#fafafa", color = NA),
        plot.background  = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major = element_line(color = "grey90"),
        panel.grid.minor = element_blank(),
        axis.line.x = element_line(color = "grey30"),
        axis.line.y = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key = element_rect(fill = "#fafafa", color = NA),
        axis.text.x = element_text(color = "grey20"),
        axis.text.y = element_text(color = "grey20"),
        text = element_text(color = "grey20")
    )

########################################################
# 5. Share of movers by SALT decile (2017 vs 2018)
########################################################

# imp_salt is only populated for household heads in housing units (gq IN (1,2)).
# Excludes households with top-coded property taxes or 2024 (imp_salt IS NULL).

mover_by_salt <- dbGetQuery(con, "
    WITH heads AS (
        SELECT
            CASE
                WHEN year BETWEEN 2012 AND 2014 THEN '2012-2014'
                WHEN year BETWEEN 2015 AND 2017 THEN '2015-2017'
                WHEN year BETWEEN 2018 AND 2020 THEN '2018-2020'
                WHEN year BETWEEN 2021 AND 2023 THEN '2021-2023'
            END AS period,
            NTILE(10) OVER (PARTITION BY year ORDER BY imp_salt) AS decile,
            CASE WHEN migrate1 IN (2,3,4) THEN 1 ELSE 0 END AS mover
        FROM acs_microdata
        WHERE year BETWEEN 2012 AND 2023
          AND pernum = 1 AND gq IN (1, 2, 5)
          AND imp_salt IS NOT NULL
    )
    SELECT
        period,
        decile,
        AVG(mover)   AS share_movers,
        COUNT(*)     AS n_households
    FROM heads
    GROUP BY period, decile
    ORDER BY period, decile
")

mover_by_salt |>
    mutate(period = factor(period, levels = names(period_colors))) |>
    ggplot(aes(x = decile, y = share_movers, color = period, group = period)) +
    geom_line() +
    geom_point() +
    scale_color_manual(name = NULL, values = period_colors) +
    scale_x_continuous(breaks = 1:10) +
    labs(x = "SALT decile (year-specific)", y = "Share of household heads who moved") +
    theme(
        panel.background = element_rect(fill = "#fafafa", color = NA),
        plot.background  = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major = element_line(color = "grey90"),
        panel.grid.minor = element_blank(),
        axis.line.x = element_line(color = "grey30"),
        axis.line.y = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key = element_rect(fill = "#fafafa", color = NA),
        axis.text.x = element_text(color = "grey20"),
        axis.text.y = element_text(color = "grey20"),
        text = element_text(color = "grey20")
    )

########################################################
# 6. LASSO: biggest predictors of moving (household heads)
########################################################

heads_lasso <- dbGetQuery(con, "
    SELECT
        CASE WHEN migrate1 IN (2,3,4) THEN 1 ELSE 0 END AS mover,
        imp_proptx, imp_stateinctax, imp_totalwages,
        age, sex, marst, race, hispan,
        educ, empstat, uhrswork, wkswork1,
        ownershp, valueh, hhincome,
        metro, statefip, year, multgen,
        -- life events / family
        nchlt5, marrno, marrinyr, divinyr, widinyr, fertyr, gchouse,
        -- nativity / language
        citizen, speakeng,
        CASE WHEN bpl >= 100 THEN 1 ELSE 0 END AS foreign_born,
        yrsusa1,
        -- employment detail
        occ2010, ind, labforce, looking, occscore,
        CASE WHEN pwstate2 > 0 AND pwstate2 != statefip THEN 1 ELSE 0 END AS cross_state_commuter,
        -- housing detail
        rooms, bedrooms, builtyr2, unitsstr, mortgage,
        CAST(rent    AS DOUBLE) AS rent,
        CAST(owncost AS DOUBLE) AS owncost,
        -- education detail
        degfield, gradeatt,
        -- geography
        region, density
    FROM acs_microdata
    WHERE pernum = 1 AND gq IN (1, 2, 5) AND migrate1 != 0
      AND imp_salt IS NOT NULL AND hhincome != 9999999
      AND year BETWEEN 2013 AND 2023
") |>
    as_tibble() |>
    mutate(
        valueh = if_else(valueh == 9999999L | ownershp != 1L, 0, as.numeric(valueh)),
        owncost = if_else(owncost >= 99999, 0, owncost),
        across(c(age, imp_proptx, imp_stateinctax, imp_totalwages,
                 hhincome, valueh, uhrswork, wkswork1,
                 nchlt5, marrno, yrsusa1, occscore,
                 rooms, bedrooms, rent, owncost, density),
               ~ as.numeric(scale(as.numeric(.))))
    ) |>
    na.omit()

# Build sparse model matrix in parallel: split predictor groups across cores,
# build each block independently, then cbind. sparse.model.matrix is
# single-threaded so this is the only way to exploit multiple cores here.
factor_blocks <- list(
    ~ 0 + factor(sex) + factor(marst) + factor(race) + factor(hispan) +
          factor(educ) + factor(empstat) + factor(ownershp) +
          factor(metro) + factor(multgen) + factor(region),
    ~ 0 + factor(marrinyr) + factor(divinyr) + factor(widinyr) +
          factor(fertyr) + factor(gchouse) +
          factor(citizen) + factor(speakeng) +
          factor(labforce) + factor(looking),
    ~ 0 + factor(occ2010),
    ~ 0 + factor(ind),
    ~ 0 + factor(builtyr2) + factor(unitsstr) + factor(mortgage) +
          factor(degfield) + factor(gradeatt),
    ~ 0 + factor(statefip) + factor(year)
)

cont_vars <- c(
    "age", "imp_proptx", "imp_stateinctax", "imp_totalwages",
    "hhincome", "valueh", "uhrswork", "wkswork1",
    "nchlt5", "marrno", "yrsusa1", "occscore",
    "rooms", "bedrooms", "rent", "owncost", "density",
    "foreign_born", "cross_state_commuter"
)

X_cont <- as(as.matrix(heads_lasso[, cont_vars]), "sparseMatrix")

X_blocks <- parallel::mclapply(
    factor_blocks,
    function(f) sparse.model.matrix(f, data = heads_lasso),
    mc.cores = min(length(factor_blocks), 10)
)

X <- do.call(cbind, c(list(X_cont), X_blocks))

y <- heads_lasso$mover

message(sprintf("LASSO matrix: %d obs × %d predictors", nrow(X), ncol(X)))

set.seed(42)
cv_fit <- cv.glmnet(X, y, family = "gaussian", alpha = 1, nfolds = 5,
                    parallel = TRUE)

cat(sprintf("\nOptimal lambda: %.6f (lambda.1se: %.6f)\n",
            cv_fit$lambda.min, cv_fit$lambda.1se))

# Non-zero coefficients at the more parsimonious lambda.1se
coefs <- coef(cv_fit, s = "lambda.1se") |>
    as.matrix() |>
    as.data.frame() |>
    setNames("coefficient") |>
    tibble::rownames_to_column("variable") |>
    filter(variable != "(Intercept)", coefficient != 0) |>
    arrange(desc(abs(coefficient)))

cat(sprintf("\n%d non-zero coefficients (lambda.1se):\n", nrow(coefs)))
print(coefs)

# Collapse factor dummies: range from lowest to highest level (including ref = 0)
coefs_summary <- coefs |>
    mutate(
        base_var  = str_replace(variable, "^factor\\(([^)]+)\\).*", "\\1"),
        is_factor = str_detect(variable, "^factor\\(")
    ) |>
    group_by(base_var) |>
    summarise(
        is_factor = any(is_factor),
        range     = max(c(0, coefficient)) - min(c(0, coefficient)),
        .groups   = "drop"
    ) |>
    arrange(desc(range))

# Variance decomposition: for each predictor group, compute its component
# of the fitted values and measure its variance as a share of Var(ŷ).
# Groups won't sum to 100% due to inter-group covariances.
selected_vars <- coefs$variable
beta_selected <- setNames(coefs$coefficient, coefs$variable)
y_hat         <- as.numeric(X[, selected_vars, drop = FALSE] %*% beta_selected)

var_decomp <- coefs |>
    mutate(base_var = str_replace(variable, "^factor\\(([^)]+)\\).*", "\\1")) |>
    group_by(base_var) |>
    group_modify(function(df, key) {
        component <- as.numeric(X[, df$variable, drop = FALSE] %*% df$coefficient)
        tibble(var_component = var(component))
    }) |>
    ungroup() |>
    mutate(
        pct_var_yhat = round(100 * var_component / var(y_hat), 1),
        pct_var_y    = round(100 * var_component / var(y),     1)
    )

predictor_summary <- coefs_summary |>
    left_join(var_decomp |> select(base_var, pct_var_yhat, pct_var_y), by = "base_var") |>
    arrange(desc(pct_var_yhat))

cat("\nPredictor summary (coefficient range + variance decomposition):\n")
print(predictor_summary, n = Inf)

predictor_summary |>
    slice_max(range, n = 30) |>
    mutate(base_var = fct_reorder(base_var, range)) |>
    ggplot(aes(x = range, y = base_var, fill = is_factor)) +
    geom_col() +
    scale_fill_manual(
        values = c("TRUE" = "#4a7c82", "FALSE" = "#eb811b"),
        labels = c("TRUE" = "categorical", "FALSE" = "continuous"),
        name   = NULL
    ) +
    labs(x = "Coefficient range (highest − lowest level)", y = NULL,
         title = "Predictor importance (LASSO, household heads 2013-2023)") +
    theme(
        panel.background = element_rect(fill = "#fafafa", color = NA),
        plot.background  = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major = element_line(color = "grey90"),
        panel.grid.minor = element_blank(),
        axis.line.x = element_line(color = "grey30"),
        axis.line.y = element_line(color = "grey30"),
        text = element_text(color = "grey20")
    )

########################################################
# 7. Partial dependence of moving on SALT (random forest)
########################################################

# Train a joint forest on (y, x, z) then integrate out z via the standard
# PDP: for each grid value of x, replace every observation's SALT with that
# value, predict, and average. The result is E_z[f(x, z)] — the marginal
# effect of SALT holding the distribution of controls fixed.

pdp_data <- dbGetQuery(con, "
    SELECT
        CASE WHEN migrate1 IN (2,3,4) THEN 1 ELSE 0 END AS mover,
        CAST(imp_salt  AS DOUBLE) AS imp_salt,
        age,
        sex, marst, race, hispan, educ, empstat, ownershp, metro, multgen,
        CAST(hhincome  AS DOUBLE) AS hhincome,
        CAST(valueh    AS DOUBLE) AS valueh,
        CAST(uhrswork  AS DOUBLE) AS uhrswork,
        year,
        -- life events / family
        nchlt5, marrno, marrinyr, divinyr, widinyr, fertyr, gchouse,
        -- nativity / language
        citizen, speakeng,
        CASE WHEN bpl >= 100 THEN 1 ELSE 0 END AS foreign_born,
        yrsusa1,
        -- employment detail
        occ2010, ind, labforce, looking, occscore,
        CASE WHEN pwstate2 > 0 AND pwstate2 != statefip THEN 1 ELSE 0 END AS cross_state_commuter,
        -- housing detail
        rooms, bedrooms, builtyr2, unitsstr, mortgage,
        CAST(rent    AS DOUBLE) AS rent,
        CAST(owncost AS DOUBLE) AS owncost,
        -- education detail
        degfield, gradeatt
    FROM acs_microdata
    WHERE pernum = 1 AND gq IN (1, 2, 5) AND migrate1 != 0
      AND imp_salt IS NOT NULL AND hhincome != 9999999
      AND year BETWEEN 2012 AND 2023
      AND random() < .1
") |>
    as_tibble() |>
    mutate(
        valueh  = if_else(valueh  == 9999999, 0, valueh),
        owncost = if_else(owncost >= 99999,   0, owncost),
        across(c(sex, marst, race, hispan, educ, empstat, ownershp, multgen,
                 marrinyr, divinyr, widinyr, fertyr, gchouse,
                 citizen, speakeng, occ2010, ind, labforce, looking,
                 builtyr2, unitsstr, mortgage, degfield, gradeatt),
               factor)
    ) |> 
    na.omit()
    
# Train on pre-TCJA period only; year excluded from formula so the forest
# generalises to post-2017 observations at prediction time.
pdp_train <- pdp_data |> filter(year %in% 2012:2017)

rf_pdp <- ranger(
    mover ~ imp_salt + age + sex + marst + race + hispan + educ +
            empstat + ownershp + multgen +
            hhincome + valueh + uhrswork +
            nchlt5 + marrno + yrsusa1 + occscore +
            rooms + bedrooms + rent + owncost +
            foreign_born + cross_state_commuter +
            marrinyr + divinyr + widinyr + fertyr + gchouse +
            citizen + speakeng + labforce + looking +
            occ2010 + ind + builtyr2 + unitsstr + mortgage +
            degfield + gradeatt,
    data          = pdp_train,
    num.trees     = 500,
    min.node.size = 50,
    seed          = 42
)

# PDP grid: 50 quantile-spaced values of imp_salt up to 99th percentile
salt_grid <- quantile(pdp_data$imp_salt, probs = seq(0, 0.99, length.out = 50))

# A subsample of 2000 rows is sufficient for accurate marginal averaging;
# draw once and reuse across all grid points.
set.seed(42)
pdp_sample <- pdp_data[sample(nrow(pdp_data), min(2000L, nrow(pdp_data))), ]

pdp_curve <- map_dbl(salt_grid, function(x_val) {
    newdata          <- pdp_sample
    newdata$imp_salt <- x_val
    mean(predict(rf_pdp, data = newdata)$predictions)
})

pdp_df <- tibble(imp_salt = salt_grid, move_rate = pdp_curve)

ggplot(pdp_df, aes(imp_salt, move_rate)) +
    geom_line(linewidth = 1, color = "#2166ac") +
    scale_x_continuous(labels = scales::dollar_format(scale = 1e-3, suffix = "k")) +
    scale_y_continuous(labels = scales::percent_format(1)) +
    labs(
        x        = "SALT (state and local taxes paid)",
        y        = "Predicted move rate",
        title    = "Partial dependence of moving on SALT",
        subtitle = "E[mover | SALT = x], integrating over marginal distribution of controls"
    ) +
    theme(
        panel.background = element_rect(fill = "#fafafa", color = NA),
        plot.background  = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major = element_line(color = "grey90"),
        panel.grid.minor = element_blank(),
        axis.line.x      = element_line(color = "grey30"),
        axis.line.y      = element_line(color = "grey30"),
        axis.text        = element_text(color = "grey20"),
        text             = element_text(color = "grey20")
    )

# Residual plot by period: predict for every observation using the pre-period
# forest (which learned the SALT-moving relationship from 2012-2017), then
# compute actual - predicted. Binning residuals by SALT and period shows
# whether high-SALT households moved more or less than the pre-period model
# expects — i.e., where the relationship shifted post-TCJA.
# Quantile cut points can repeat when many observations share the same SALT.
# Use unique breaks and gracefully fall back to NA bins when there is no spread.
salt_breaks <- stats::quantile(
    pdp_data$imp_salt,
    probs = seq(0, 1, length.out = 21),
    na.rm = TRUE
) |>
    unique()

pdp_data <- pdp_data |>
    mutate(
        predicted = predict(rf_pdp, data = pdp_data)$predictions,
        resid     = mover - predicted,
        period    = case_when(
            year %in% 2012:2014 ~ "2012-2014",
            year %in% 2015:2017 ~ "2015-2017",
            year %in% 2018:2020 ~ "2018-2020",
            year %in% 2021:2023 ~ "2021-2023"
        ) |> factor(levels = names(period_colors)),
        salt_bin  = if (length(salt_breaks) > 1) {
            cut(
                imp_salt,
                breaks = salt_breaks,
                include.lowest = TRUE,
                labels = FALSE
            )
        } else {
            NA_integer_
        }
    )

resid_by_period <- pdp_data |>
    group_by(period, salt_bin) |>
    summarise(
        mean_resid = mean(resid),
        salt_mid   = mean(imp_salt),
        n          = n(),
        .groups    = "drop"
    )

ggplot(resid_by_period, aes(salt_mid, mean_resid, color = period)) +
    geom_hline(yintercept = 0, linetype = "dashed", color = "grey50") +
    geom_line(linewidth = 0.9) +
    geom_point(size = 1.5) +
    scale_x_continuous(labels = scales::dollar_format(scale = 1e-3, suffix = "k")) +
    scale_y_continuous(labels = scales::percent_format(1)) +
    scale_color_manual(values = period_colors, name = NULL) +
    labs(
        x        = "SALT (state and local taxes paid)",
        y        = "Actual - predicted move rate",
        title    = "Move rate residuals vs. SALT, by period",
        subtitle = "Residuals from forest trained on 2012-2017; zero = matches pre-period expectation"
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        axis.text         = element_text(color = "grey20"),
        text              = element_text(color = "grey20"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.position   = "bottom"
    ) + 
    xlim(0, 10000)

########################################################
# 8. Moving rate vs SALT — two-sided DML (Robinson) by period
########################################################

# y = f(x, Z, e), E[e|x,Z] = 0.
# Two-sided residualization (DML): train nuisance RFs for E[Y|Z] and E[X|Z]
# on pre-policy data (2012-2017). Residualize both mover and SALT, then plot
# E[ỹ | x̃, period] by period. Changes in the ỹ-x̃ slope across periods reflect
# changes in the structural SALT→moving effect, purged of demographic selection
# into both moving and SALT levels. OOB predictions used for pre-period to
# avoid overfitting; out-of-sample predict() for post-period.

salt_rf <- dbGetQuery(con, "
    SELECT
        CASE WHEN migrate1 IN (2,3,4) THEN 1 ELSE 0 END AS mover,
        CAST(imp_salt AS DOUBLE) AS imp_salt,
        age, sex, marst, race, hispan,
        educ, metro, statefip, year, multgen
    FROM acs_microdata
    WHERE pernum = 1 AND gq IN (1, 2, 5) AND migrate1 != 0
      AND imp_salt IS NOT NULL
      AND year BETWEEN 2012 AND 2023
      AND random() < 0.1
") |>
    as_tibble() |>
    mutate(
        period = case_when(
            year %in% 2012:2014 ~ "2012-2014",
            year %in% 2015:2017 ~ "2015-2017",
            year %in% 2018:2020 ~ "2018-2020",
            year %in% 2021:2023 ~ "2021-2023"
        ) |> factor(levels = names(period_colors)),
        across(c(sex, marst, race, hispan, educ, metro, multgen, statefip), factor),
        imp_salt = as.numeric(imp_salt)
    ) |>
    na.omit()

pre_idx   <- salt_rf$year %in% 2012:2017
pre_data  <- salt_rf[pre_idx, ]
post_data <- salt_rf[!pre_idx, ]

# Nuisance RF for E[Y|Z]: mover on demographics only (no SALT, no year/period)
rf_y <- ranger(
    mover ~ age + sex + marst + race + hispan + educ + metro + statefip + multgen,
    data = pre_data, num.trees = 200, min.node.size = 50, max.depth = 6, seed = 42
)

# Nuisance RF for E[X|Z]: SALT on demographics only
rf_x <- ranger(
    imp_salt ~ age + sex + marst + race + hispan + educ + metro + statefip + multgen,
    data = pre_data, num.trees = 200, min.node.size = 50, max.depth = 6, seed = 42
)

# Residualize: OOB for pre-period, out-of-sample for post-period
salt_rf$pred_y <- NA_real_
salt_rf$pred_x <- NA_real_
salt_rf$pred_y[pre_idx]  <- rf_y$predictions
salt_rf$pred_y[!pre_idx] <- predict(rf_y, data = post_data)$predictions
salt_rf$pred_x[pre_idx]  <- rf_x$predictions
salt_rf$pred_x[!pre_idx] <- predict(rf_x, data = post_data)$predictions

salt_rf <- salt_rf |>
    mutate(
        resid_y = mover    - pred_y,
        resid_x = imp_salt - pred_x
    )


binsreg(
    y          = salt_rf$resid_y,
    by         = salt_rf$period,
    x          = salt_rf$resid_x,
    nbins      = 20,
    samebinsby = TRUE,
    plotxrange = c(-2000, 7500)
)

########################################################
# 9. Moving rate vs SALT — DML controlling for income and home value
########################################################

# Same two-sided DML as section 7 but adds household income and home value
# to the nuisance controls (Z), purging income/wealth selection from the
# SALT-moving relationship. Valueh is set to 0 for renters and top-coded
# observations (consistent with section 6 treatment).

salt_rf2 <- dbGetQuery(con, "
    SELECT
        CASE WHEN migrate1 IN (2,3,4) THEN 1 ELSE 0 END AS mover,
        CAST(imp_salt AS DOUBLE) AS imp_salt,
        age, sex, marst, race, hispan,
        educ, year, multgen,
        hhincome, valueh, ownershp
    FROM acs_microdata
    WHERE pernum = 1 AND gq IN (1, 2, 5) AND migrate1 != 0
      AND imp_salt IS NOT NULL
      AND hhincome != 9999999
      AND year BETWEEN 2012 AND 2023
      AND random() < 0.1
") |>
    as_tibble() |>
    mutate(
        valueh = if_else(valueh == 9999999L | ownershp != 1L, 0, as.numeric(valueh)),
        period = case_when(
            year %in% 2012:2014 ~ "2012-2014",
            year %in% 2015:2017 ~ "2015-2017",
            year %in% 2018:2020 ~ "2018-2020",
            year %in% 2021:2023 ~ "2021-2023"
        ) |> factor(levels = names(period_colors)),
        across(c(sex, marst, race, hispan, educ, multgen, ownershp), factor),
        imp_salt = as.numeric(imp_salt),
        hhincome = as.numeric(hhincome)
    ) |>
    na.omit()

pre_idx2   <- salt_rf2$year %in% 2012:2017
pre_data2  <- salt_rf2[pre_idx2, ]
post_data2 <- salt_rf2[!pre_idx2, ]

rf_y2 <- ranger(
    mover ~ age + sex + marst + race + hispan + educ + multgen +
             hhincome + valueh + ownershp,
    data = pre_data2, num.trees = 200, min.node.size = 50, max.depth = 6, seed = 42
)

rf_x2 <- ranger(
    imp_salt ~ age + sex + marst + race + hispan + educ + multgen +
                hhincome + valueh + ownershp,
    data = pre_data2, num.trees = 200, min.node.size = 50, max.depth = 6, seed = 42
)

salt_rf2$pred_y <- NA_real_
salt_rf2$pred_x <- NA_real_
salt_rf2$pred_y[pre_idx2]  <- rf_y2$predictions
salt_rf2$pred_y[!pre_idx2] <- predict(rf_y2, data = post_data2)$predictions
salt_rf2$pred_x[pre_idx2]  <- rf_x2$predictions
salt_rf2$pred_x[!pre_idx2] <- predict(rf_x2, data = post_data2)$predictions

salt_rf2 <- salt_rf2 |>
    mutate(
        resid_y = mover    - pred_y,
        resid_x = imp_salt - pred_x
    )

binsreg(
    y          = salt_rf2$resid_y,
    by         = salt_rf2$period,
    x          = salt_rf2$resid_x,
    nbins      = 10,
    samebinsby = TRUE,
    plotxrange = c(-5000, 7500)
)

salt_rf2 |> feols(resid_y ~ i(year,resid_x))

########################################################
# 10. Moving rates by itemization group (TCJA design)
########################################################

# Three groups defined by itemization status under each rule set:
#
#   (a) Always itemized:          itemizes under both 2017 and TCJA rules
#   (b) Stopped itemizing (TCJA): itemizes under 2017 rules but not TCJA rules
#   (c) Never itemized:           does not itemize under either rule set
#
# 2017-law classification: imp_itemizes_2017 (SALT uncapped, std_ded fixed at
#   2017 levels for all years — already stored in DB; MFJ $12,700 / HoH $9,350 /
#   Single $6,350).
# TCJA-law classification: imp_itemizable_2018 (SALT capped $10k, mort int at
#   $750k cap) vs. fixed post-TCJA std_ded. Standard deduction computed from
#   marst and nchild matching the build script (MFJ $24k / HoH $18k / Single $12k)
#   and held fixed for all years so group assignment reflects the policy change,
#   not year-to-year inflation indexing.

item_data <- dbGetQuery(con, "
    SELECT
        year,
        CASE WHEN migrate1 IN (2,3,4) THEN 1 ELSE 0 END AS mover,
        imp_itemizes_2017,
        CAST(imp_itemizable_2017 AS DOUBLE)              AS imp_itemizable_2017,
        CAST(imp_itemizable_2018 AS DOUBLE)              AS imp_itemizable_2018,
        CAST(imp_salt            AS DOUBLE)              AS imp_salt,
        CASE
            WHEN marst = 1                 THEN 24000
            WHEN marst != 1 AND nchild > 0 THEN 18000
            ELSE                                12000
        END                                              AS tcja_std_ded,
        age, sex, marst, race, hispan, educ, metro, statefip, multgen
    FROM acs_microdata
    WHERE pernum = 1 AND gq IN (1, 2, 5) AND migrate1 != 0
      AND imp_itemizes_2017    IS NOT NULL
      AND imp_itemizable_2018  IS NOT NULL
      AND year BETWEEN 2012 AND 2023
") |>
    as_tibble() |>
    mutate(
        tcja_itemizes = as.integer(imp_itemizable_2018 > tcja_std_ded),
        group = case_when(
            imp_itemizes_2017 == 1L & tcja_itemizes == 1L ~ "Always itemized",
            imp_itemizes_2017 == 1L & tcja_itemizes == 0L ~ "Stopped itemizing (TCJA)",
            imp_itemizes_2017 == 0L                        ~ "Never itemized"
        ) |> factor(levels = c("Never itemized", "Stopped itemizing (TCJA)", "Always itemized")),
        across(c(sex, marst, race, hispan, educ, metro, multgen, statefip), factor)
    ) |>
    na.omit()

cat("\nOverall move rate by year:\n")
print(
    item_data |>
        group_by(year) |>
        summarise(move_rate = round(100 * mean(mover), 2), n = n(), .groups = "drop") |>
        as.data.frame()
)

cat("\nGroup composition:\n")
print(
    item_data |>
        count(group) |>
        mutate(pct = round(100 * n / sum(n), 1))
)

# Step 1: random forest to partial out demographics (no statefip).
# OOB predictions avoid in-sample overfitting.
rf_fit <- ranger(
    mover ~ age + sex + marst + race + hispan + educ + metro + multgen,
    data      = item_data,
    num.trees = 500,
    seed      = 42
)

item_data <- item_data |>
    mutate(adj_mover = mover - rf_fit$predictions + mean(mover))

# Step 2: group × year cell means with HC1-robust SEs via a saturated feols.
cell_fit <- feols(
    adj_mover ~ 0 + group:factor(year),
    data   = item_data,
    vcov   = "HC1"
)

mover_by_group_year <- tidy(cell_fit) |>
    mutate(
        group = case_when(
            grepl("Always itemized",   term) ~ "Always itemized",
            grepl("Stopped itemizing", term) ~ "Stopped itemizing (TCJA)",
            TRUE                             ~ "Never itemized"
        ) |> factor(levels = c("Never itemized", "Stopped itemizing (TCJA)", "Always itemized")),
        year = as.integer(regmatches(term, regexpr("\\d{4}", term)))
    ) |>
    select(group, year, share_movers = estimate, se = std.error)

group_colors <- c(
    "Never itemized"           = "#23373b",
    "Stopped itemizing (TCJA)" = "#c06028",
    "Always itemized"          = "#4a7c82"
)

ggplot(mover_by_group_year,
       aes(year, share_movers, color = group, fill = group, group = group)) +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey60") +
    geom_ribbon(aes(ymin = share_movers - 1.96 * se,
                    ymax = share_movers + 1.96 * se),
                alpha = 0.12, color = NA) +
    geom_line(linewidth = 0.9) +
    geom_point(size = 2) +
    annotate("text", x = 2017.7, y = Inf, label = "TCJA",
             vjust = 1.5, hjust = 0, color = "grey40", size = 3.5) +
    scale_color_manual(name = NULL, values = group_colors) +
    scale_fill_manual(name  = NULL, values = group_colors) +
    scale_x_continuous(breaks = 2012:2023) +
    scale_y_continuous(labels = scales::percent_format(1)) +
    labs(
        x     = NULL,
        y     = "Share of household heads who moved",
        title = "Moving rates by itemization group"
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key        = element_rect(fill = "#fafafa", color = NA),
        axis.text.x       = element_text(color = "grey20", angle = 45, hjust = 1),
        axis.text.y       = element_text(color = "grey20"),
        text              = element_text(color = "grey20")
    )

########################################################
# 10b. Moving rates by proximity to TCJA itemization threshold (MFJ)
########################################################

# For married filing jointly households (fixed $24k TCJA standard deduction),
# we stratify by how close their post-TCJA itemizable total is to the
# new threshold: ratio = imp_itemizable_2018 / 24000.
# Bands straddle the cutoff at 1.0:
#   < 1.0 → does not itemize under TCJA
#   ≥ 1.0 → still itemizes under TCJA
# Households near but below 1.0 are the marginal stopped-itemizers;
# those near but above are the marginal survivors.

band_levels <- c("< 60%", "60-80%", "80-100%", "100-120%", "120-140%", "140-160%", "> 160%")

thresh_data <- item_data |>
    filter(marst == "1") |>
    mutate(
        ratio = imp_itemizable_2018 / 24000,
        band  = case_when(
            ratio <  0.6 ~ "< 60%",
            ratio <  0.8 ~ "60-80%",
            ratio <  1.0 ~ "80-100%",
            ratio <  1.2 ~ "100-120%",
            ratio <  1.4 ~ "120-140%",
            ratio <  1.6 ~ "140-160%",
            TRUE         ~ "> 160%"
        ) |> factor(levels = band_levels)
    )

cat("\nMFJ threshold-proximity band sizes:\n")
print(thresh_data |> count(band) |> mutate(pct = round(100 * n / sum(n), 1)))

cell_fit_thresh <- feols(
    mover ~ 0 + band:factor(year),
    data = thresh_data,
    vcov = "HC1"
)

thresh_mover <- tidy(cell_fit_thresh) |>
    mutate(
        year = as.integer(str_extract(term, "\\d{4}")),
        band = str_remove(term, "^band") |>
                   str_remove(":factor\\(year\\)\\d{4}") |>
                   factor(levels = band_levels)
    ) |>
    select(band, year, share_movers = estimate, se = std.error)

ggplot(thresh_mover, aes(year, share_movers, color = band, fill = band, group = band)) +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey60") +
    geom_ribbon(aes(ymin = share_movers - 1.96 * se,
                    ymax = share_movers + 1.96 * se),
                alpha = 0.12, color = NA) +
    geom_line(linewidth = 0.9) +
    geom_point(size = 2) +
    annotate("text", x = 2017.7, y = Inf, label = "TCJA",
             vjust = 1.5, hjust = 0, color = "grey40", size = 3.5) +
    scale_x_continuous(breaks = 2012:2023) +
    scale_y_continuous(labels = scales::percent_format(1)) +
    labs(
        x        = NULL,
        y        = "Share of household heads who moved",
        title    = "Moving rates by proximity to TCJA itemization threshold (MFJ)",
        subtitle = "Ratio = TCJA itemizable total / $24k standard deduction; bands straddle cutoff at 100%",
        color    = NULL,
        fill     = NULL
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key        = element_rect(fill = "#fafafa", color = NA),
        axis.text.x       = element_text(color = "grey20", angle = 45, hjust = 1),
        axis.text.y       = element_text(color = "grey20"),
        text              = element_text(color = "grey20"),
        legend.position   = "bottom"
    )

########################################################
# 10c. Move rate by pre-TCJA itemizable total (MFJ, $3k bins)
########################################################

# For MFJ households, the TCJA raised the standard deduction from $12.7k to
# $24k. Anyone with imp_itemizable_2017 in [$12.7k, $24k] was a pre-TCJA
# itemizer who stopped after TCJA (the standard deduction strictly dominates
# their capped post-TCJA itemizable). We bin by imp_itemizable_2017 in $3k
# increments to see (a) how move rates vary around the $24k threshold and
# (b) how itemization groups map to bins (sanity check).

BIN_W        <- 3000
MFJ_STD_2017 <- 12700
MFJ_STD_2018 <- 24000
BIN_MAX      <- 45000

mfj_data <- item_data |>
    filter(marst == "1") |>
    mutate(
        bin_start = floor(imp_itemizable_2017 / BIN_W) * BIN_W,
        bin_mid   = bin_start + BIN_W / 2,
        period    = if_else(year >= 2018, "Post-TCJA (2018–2023)", "Pre-TCJA (2012–2017)") |>
                    factor(levels = c("Pre-TCJA (2012–2017)", "Post-TCJA (2018–2023)"))
    ) |>
    filter(bin_start >= 0, bin_start <= BIN_MAX)

period_colors_2 <- c(
    "Pre-TCJA (2012–2017)"  = "#4a7c82",
    "Post-TCJA (2018–2023)" = "#c06028"
)

# (a) Move rate by bin × period
mfj_mover <- mfj_data |>
    group_by(period, bin_start, bin_mid) |>
    summarise(
        share_movers = mean(mover),
        se           = sqrt(share_movers * (1 - share_movers) / n()),
        n            = n(),
        .groups      = "drop"
    )

ggplot(mfj_mover, aes(bin_mid, share_movers, color = period, fill = period, group = period)) +
    geom_vline(xintercept = MFJ_STD_2017, linetype = "dotted", color = "grey50") +
    geom_vline(xintercept = MFJ_STD_2018, linetype = "dashed",  color = "grey40") +
    annotate("text", x = MFJ_STD_2017 + 300, y = Inf,
             label = "Old std ded\n($12.7k)", vjust = 1.4, hjust = 0, size = 2.8, color = "grey40") +
    annotate("text", x = MFJ_STD_2018 + 300, y = Inf,
             label = "TCJA std ded\n($24k)",  vjust = 1.4, hjust = 0, size = 2.8, color = "grey40") +
    geom_ribbon(aes(ymin = share_movers - 1.96 * se,
                    ymax = share_movers + 1.96 * se),
                alpha = 0.12, color = NA) +
    geom_line(linewidth = 0.9) +
    geom_point(size = 1.8) +
    scale_x_continuous(
        labels = scales::dollar_format(scale = 1e-3, suffix = "k"),
        breaks = seq(0, BIN_MAX, by = 6000)
    ) +
    scale_y_continuous(labels = scales::percent_format(1)) +
    scale_color_manual(values = period_colors_2, name = NULL) +
    scale_fill_manual(values  = period_colors_2, name = NULL) +
    labs(
        x        = "Pre-TCJA itemizable total (SALT + mortgage interest, $3k bins)",
        y        = "Share of household heads who moved",
        title    = "Move rate by pre-TCJA itemizable total (MFJ)",
        subtitle = "Dotted: old std ded $12.7k  •  Dashed: TCJA std ded $24k"
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key        = element_rect(fill = "#fafafa", color = NA),
        axis.text.x       = element_text(color = "grey20"),
        axis.text.y       = element_text(color = "grey20"),
        text              = element_text(color = "grey20"),
        legend.position   = "bottom"
    )

# (b) Sanity check: itemization group composition by bin
# Expected: [0-12.7k] → 100% Never; [12.7k-24k] → 100% Stopped;
# [24k+] → mix of Stopped and Always depending on how much SALT got capped.
mfj_comp <- mfj_data |>
    group_by(bin_start, bin_mid, group) |>
    summarise(n = n(), .groups = "drop") |>
    group_by(bin_start) |>
    mutate(pct = n / sum(n)) |>
    ungroup()

ggplot(mfj_comp, aes(bin_mid, pct, fill = group)) +
    geom_col(width = BIN_W * 0.85, position = "stack") +
    geom_vline(xintercept = MFJ_STD_2017, linetype = "dotted", color = "white", linewidth = 0.8) +
    geom_vline(xintercept = MFJ_STD_2018, linetype = "dashed",  color = "white", linewidth = 0.8) +
    scale_fill_manual(values = group_colors, name = NULL) +
    scale_x_continuous(
        labels = scales::dollar_format(scale = 1e-3, suffix = "k"),
        breaks = seq(0, BIN_MAX, by = 6000)
    ) +
    scale_y_continuous(labels = scales::percent_format()) +
    labs(
        x        = "Pre-TCJA itemizable total ($3k bins)",
        y        = "Share in group",
        title    = "Itemization group composition by bin (MFJ, sanity check)",
        subtitle = "Dotted: old std ded $12.7k  •  Dashed: TCJA std ded $24k"
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key        = element_rect(fill = "#fafafa", color = NA),
        axis.text.x       = element_text(color = "grey20"),
        axis.text.y       = element_text(color = "grey20"),
        text              = element_text(color = "grey20"),
        legend.position   = "bottom"
    )

########################################################
# (c) Sanity check: mean additional federal tax from SALT cap by bin
# Expected: ~$0 for bins below $12.7k (never itemized, no SALT deduction
# to lose); rising for [$12.7k, $24k] stopped-itemizers; higher still above
# $24k where always-itemizers have large SALT being capped at $10k.
########################################################

salt_burden_mfj <- dbGetQuery(con, "
    SELECT
        CAST(imp_itemizable_2017 AS DOUBLE) AS imp_itemizable_2017,
        imp_additional_fed_tax_salt
    FROM acs_microdata
    WHERE pernum = 1 AND gq IN (1, 2, 5)
      AND marst = 1
      AND imp_itemizable_2017         IS NOT NULL
      AND imp_additional_fed_tax_salt IS NOT NULL
      AND year BETWEEN 2013 AND 2023
") |>
    as_tibble() |>
    mutate(
        bin_start = floor(imp_itemizable_2017 / BIN_W) * BIN_W,
        bin_mid   = bin_start + BIN_W / 2
    ) |>
    filter(bin_start >= 0, bin_start <= BIN_MAX) |>
    group_by(bin_start, bin_mid) |>
    summarise(
        mean_additional_tax = mean(imp_additional_fed_tax_salt),
        n                   = n(),
        .groups             = "drop"
    )

ggplot(salt_burden_mfj, aes(bin_mid, mean_additional_tax)) +
    geom_vline(xintercept = MFJ_STD_2017, linetype = "dotted", color = "grey50") +
    geom_vline(xintercept = MFJ_STD_2018, linetype = "dashed",  color = "grey40") +
    annotate("text", x = MFJ_STD_2017 + 300, y = Inf,
             label = "Old std ded\n($12.7k)", vjust = 1.4, hjust = 0, size = 2.8, color = "grey40") +
    annotate("text", x = MFJ_STD_2018 + 300, y = Inf,
             label = "TCJA std ded\n($24k)",  vjust = 1.4, hjust = 0, size = 2.8, color = "grey40") +
    geom_col(width = BIN_W * 0.85, fill = "#4a7c82") +
    scale_x_continuous(
        labels = scales::dollar_format(scale = 1e-3, suffix = "k"),
        breaks = seq(0, BIN_MAX, by = 6000)
    ) +
    scale_y_continuous(labels = scales::dollar_format()) +
    labs(
        x        = "Pre-TCJA itemizable total ($3k bins)",
        y        = "Mean additional federal tax (SALT cap)",
        title    = "SALT reform burden by pre-TCJA itemizable total (MFJ, sanity check)",
        subtitle = "Positive = more federal tax paid due to reduced SALT deductibility"
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key        = element_rect(fill = "#fafafa", color = NA),
        axis.text.x       = element_text(color = "grey20"),
        axis.text.y       = element_text(color = "grey20"),
        text              = element_text(color = "grey20")
    )

########################################################
# 10d. Move rate: itemizers vs. non-itemizers at equal SALT level
########################################################

# At a given SALT level, itemizers face a lower effective SALT cost than
# non-itemizers because they can deduct it against federal income tax.
# Variation in itemization status within a SALT bin comes mainly from
# differences in mortgage interest (itemizers have enough total itemizable
# to clear the standard deduction; non-itemizers don't). We ask whether
# this effective-cost difference predicts lower mobility among itemizers.

SALT_BIN_W <- 1000

salt_item_data <- dbGetQuery(con, "
    SELECT
        CAST(imp_salt AS DOUBLE)                         AS imp_salt,
        imp_itemizes_2017,
        CASE WHEN migrate1 IN (2,3,4) THEN 1 ELSE 0 END AS mover,
        year
    FROM acs_microdata
    WHERE pernum = 1 AND gq IN (1, 2, 5) AND migrate1 != 0
      AND imp_salt          IS NOT NULL
      AND imp_itemizes_2017 IS NOT NULL
      AND year BETWEEN 2013 AND 2023
") |>
    as_tibble() |>
    mutate(
        bin_start = floor(imp_salt / SALT_BIN_W) * SALT_BIN_W,
        bin_mid   = bin_start + SALT_BIN_W / 2
    )

SALT_MAX <- round(quantile(salt_item_data$imp_salt, 0.95), -3)

# --- (a) Itemization rate by SALT bin ---
item_rate_by_salt <- salt_item_data |>
    filter(bin_start >= 0, bin_start <= SALT_MAX) |>
    group_by(bin_start, bin_mid) |>
    summarise(pct_itemize = mean(imp_itemizes_2017), n = n(), .groups = "drop")

threshold_10pct <- item_rate_by_salt |>
    filter(pct_itemize >= 0.10) |>
    slice_min(bin_start, n = 1, with_ties = FALSE) |>
    pull(bin_mid)

cat(sprintf("\n10%% itemization threshold: SALT bin mid = $%.0f\n", threshold_10pct))

ggplot(item_rate_by_salt, aes(bin_mid, pct_itemize)) +
    geom_hline(yintercept = 0.10, linetype = "dashed", color = "grey50") +
    geom_vline(xintercept = threshold_10pct, linetype = "dotted", color = "#c06028") +
    geom_line(linewidth = 0.8, color = "#23373b") +
    geom_point(size = 1.5, color = "#23373b") +
    annotate("text", x = threshold_10pct + SALT_BIN_W * 0.4, y = 0.10,
             label = sprintf("$%.0fk", threshold_10pct / 1000),
             vjust = -0.5, hjust = 0, size = 3, color = "#c06028") +
    scale_x_continuous(labels = scales::dollar_format(scale = 1e-3, suffix = "k")) +
    scale_y_continuous(labels = scales::percent_format(1)) +
    labs(
        x        = "SALT (property tax + state income tax, $1k bins)",
        y        = "Share itemizing (2017 rules)",
        title    = "Itemization rate by SALT level",
        subtitle = "Dashed: 10% threshold  •  Dotted: first bin to cross it"
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key        = element_rect(fill = "#fafafa", color = NA),
        axis.text.x       = element_text(color = "grey20"),
        axis.text.y       = element_text(color = "grey20"),
        text              = element_text(color = "grey20")
    )

# --- (b) Move rate by year × itemization status, faceted by $2k SALT bin ---
# $2k bins keep the number of facets manageable (~8-10 panels).
# Each facet shows whether the itemizer/non-itemizer gap changes around TCJA.

item_colors  <- c("Does not itemize" = "#23373b", "Itemizes" = "#c06028")
SALT_BIN_W2  <- 2000

mover_salt_year <- salt_item_data |>
    mutate(
        bin2_start = floor(imp_salt / SALT_BIN_W2) * SALT_BIN_W2,
        bin2_label = sprintf("$%dk–%dk", bin2_start / 1000,
                             (bin2_start + SALT_BIN_W2) / 1000),
        itemizes   = factor(imp_itemizes_2017, 0:1,
                            labels = c("Does not itemize", "Itemizes"))
    ) |>
    filter(bin2_start >= 0, bin2_start <= SALT_MAX) |>
    group_by(bin2_start, bin2_label, year, itemizes) |>
    summarise(
        share_movers = mean(mover),
        se           = sqrt(share_movers * (1 - share_movers) / n()),
        n            = n(),
        .groups      = "drop"
    ) |>
    filter(n >= 15) |>
    mutate(bin2_label = factor(bin2_label,
                               levels = unique(bin2_label[order(bin2_start)])))

ggplot(mover_salt_year,
       aes(year, share_movers, color = itemizes, fill = itemizes, group = itemizes)) +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey60") +
    geom_ribbon(aes(ymin = share_movers - 1.96 * se,
                    ymax = share_movers + 1.96 * se),
                alpha = 0.12, color = NA) +
    geom_line(linewidth = 0.7) +
    geom_point(size = 1.3) +
    facet_wrap(~ bin2_label, ncol = 4, scales = "free_y") +
    scale_x_continuous(breaks = seq(2013, 2023, by = 2)) +
    scale_y_continuous(labels = scales::percent_format(1)) +
    scale_color_manual(values = item_colors, name = NULL) +
    scale_fill_manual(values  = item_colors, name = NULL) +
    labs(
        x        = NULL,
        y        = "Share of household heads who moved",
        title    = "Move rate by year: itemizers vs. non-itemizers, by SALT bin",
        subtitle = "Each facet = $2k SALT bin; dashed = TCJA (2018)"
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        strip.background  = element_rect(fill = "grey92", color = NA),
        strip.text        = element_text(color = "grey20", size = 8),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key        = element_rect(fill = "#fafafa", color = NA),
        axis.text.x       = element_text(color = "grey20", angle = 45, hjust = 1, size = 7),
        axis.text.y       = element_text(color = "grey20", size = 7),
        text              = element_text(color = "grey20"),
        legend.position   = "bottom"
    )

########################################################
# 11. Move rate by additional federal tax from SALT cap, by period
########################################################

# Households are sorted into four groups by their additional federal tax from
# the SALT cap (imp_additional_fed_tax_salt), with thresholds fixed on the
# full pooled sample. The "no/minimal cost" group (< $500) serves as a
# reference — these households are largely unaffected by the SALT cap.
# The remaining three groups are equal-count terciles among affected households.

tax_cost_data <- dbGetQuery(con, "
    SELECT
        year,
        imp_additional_fed_tax_salt,
        CASE WHEN migrate1 IN (2,3,4) THEN 1 ELSE 0 END AS mover
    FROM acs_microdata
    WHERE pernum = 1 AND gq IN (1, 2, 5) AND migrate1 != 0
      AND imp_additional_fed_tax_salt IS NOT NULL
      AND year BETWEEN 2012 AND 2023
") |> as_tibble()

tax_bin_breaks <- c(500, 2000, 4000, 6000, 8000)
tax_bin_labels <- c("<$500", "$500–2k", "$2k–4k", "$4k–6k", "$6k–8k", "$8k–10k")
tax_bin_colors <- setNames(
    c("#cccccc", RColorBrewer::brewer.pal(5, "YlOrRd")),
    tax_bin_labels
)

tax_cost_data <- tax_cost_data |>
    filter(imp_additional_fed_tax_salt < 10000) |>
    mutate(tax_bin = cut(
        imp_additional_fed_tax_salt,
        breaks = c(-Inf, tax_bin_breaks),
        labels = tax_bin_labels,
        right  = FALSE
    ))

mover_by_tax_bin <- tax_cost_data |>
    group_by(tax_bin, year) |>
    summarise(
        share_movers = mean(mover),
        se           = sqrt(share_movers * (1 - share_movers) / n()),
        n            = n(),
        .groups      = "drop"
    )
 
ggplot(mover_by_tax_bin,
       aes(year, share_movers, color = tax_bin, fill = tax_bin, group = tax_bin)) +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey60") +
    geom_ribbon(aes(ymin = share_movers - 1.96 * se,
                    ymax = share_movers + 1.96 * se),
                alpha = 0.12, color = NA) +
    geom_line(linewidth = 0.9) +
    geom_point(size = 2) +
    annotate("text", x = 2017.7, y = Inf, label = "TCJA",
             vjust = 1.5, hjust = 0, color = "grey40", size = 3.5) +
    scale_x_continuous(breaks = 2012:2023) +
    scale_y_continuous(labels = scales::percent_format(1)) +
    scale_color_manual(values = tax_bin_colors, name = NULL) +
    scale_fill_manual(values  = tax_bin_colors, name = NULL) +
    labs(
        x        = NULL,
        y        = "Share of household heads who moved",
        title    = "Move rate over time by SALT reform tax cost",
        subtitle = "$2k fixed bins; >$10k grouped together; <$500 = reference (unaffected)"
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key        = element_rect(fill = "#fafafa", color = NA),
        axis.text.x       = element_text(color = "grey20", angle = 45, hjust = 1),
        axis.text.y       = element_text(color = "grey20"),
        text              = element_text(color = "grey20"),
        legend.position   = "bottom"
    )

########################################################
# 12. Binned DiD event studies: each cost bin vs. <$500 control group
########################################################

# For each of the five cost bins, estimate a separate event study using
# households in that bin and in the <$500 reference group (unaffected by
# the SALT cap). The five event studies are overlaid in a single plot.
# Reference year = 2017. Controls + year FE + state FE; cluster by state.

did_data_binned <- dbGetQuery(con, "
    SELECT
        CAST(imp_additional_fed_tax_salt AS DOUBLE)      AS salt_cost,
        CASE WHEN migrate1 IN (2,3,4) THEN 1 ELSE 0 END AS mover,
        year,
        age, sex, marst, race, hispan, educ,
        metro, multgen, statefip
    FROM acs_microdata
    WHERE pernum = 1 AND gq IN (1, 2, 5) AND migrate1 != 0
      AND imp_additional_fed_tax_salt IS NOT NULL
      AND imp_additional_fed_tax_salt <  10000
      AND year BETWEEN 2013 AND 2023
      AND random() < 0.25
") |>
    as_tibble() |>
    mutate(
        bin = cut(salt_cost,
                  breaks = c(-Inf, 2000, 4000, 6000, 8000, Inf),
                  labels = c("<$2k", "$2k–4k", "$4k–6k", "$6k–8k", "$8k–10k"),
                  right  = FALSE),
        across(c(sex, marst, race, hispan, educ, metro, multgen, statefip), factor)
    )

treatment_bins <- c("$2k–4k", "$4k–6k", "$6k–8k", "$8k–10k")
bin_es_colors  <- setNames(RColorBrewer::brewer.pal(4, "YlOrRd"), treatment_bins)

es_results <- bind_rows(lapply(treatment_bins, function(b) {
    dat <- did_data_binned |>
        filter(bin %in% c("<$2k", b)) |>
        mutate(treated = as.integer(bin == b))

    fit <- feols(
        mover ~ i(year, treated, ref = 2016) + age + sex + marst + race +
            hispan + educ + metro + multgen | year + statefip,
        data = dat, vcov = ~statefip
    )

    tidy(fit) |>
        filter(str_detect(term, "year::")) |>
        mutate(
            year = as.integer(str_extract(term, "\\d{4}")),
            bin  = b
        )
})) |>
    mutate(bin = factor(bin, levels = treatment_bins))

ggplot(es_results,
       aes(year, estimate, color = bin, fill = bin, group = bin)) +
    geom_hline(yintercept = 0, color = "grey70") +
    geom_vline(xintercept = 2017.5, linetype = "dashed", color = "grey50") +
    geom_ribbon(aes(ymin = estimate - 1.96 * std.error,
                    ymax = estimate + 1.96 * std.error),
                alpha = 0.10, color = NA) +
    geom_line(linewidth = 0.9) +
    geom_point(size = 2) +
    annotate("text", x = 2017.7, y = Inf, label = "TCJA",
             vjust = 1.5, hjust = 0, color = "grey40", size = 3.5) +
    scale_x_continuous(breaks = 2013:2023) +
    scale_y_continuous(labels = scales::percent_format(accuracy = 0.01)) +
    scale_color_manual(values = bin_es_colors, name = "SALT reform cost") +
    scale_fill_manual(values  = bin_es_colors, name = "SALT reform cost") +
    labs(
        x        = NULL,
        y        = "Additional probability of moving (pp)",
        title    = "Event study: move probability by SALT reform cost bin",
        subtitle = "Each bin vs. <$500 control group; ref year = 2017; controls + year + state FE"
    ) +
    theme(
        panel.background  = element_rect(fill = "#fafafa", color = NA),
        plot.background   = element_rect(fill = "#fafafa", color = NA),
        panel.grid.major  = element_line(color = "grey90"),
        panel.grid.minor  = element_blank(),
        axis.line.x       = element_line(color = "grey30"),
        axis.line.y       = element_line(color = "grey30"),
        legend.background = element_rect(fill = "#fafafa", color = NA),
        legend.key        = element_rect(fill = "#fafafa", color = NA),
        axis.text.x       = element_text(color = "grey20", angle = 45, hjust = 1),
        axis.text.y       = element_text(color = "grey20"),
        text              = element_text(color = "grey20"),
        legend.position   = "bottom"
    )

dbDisconnect(con, shutdown = TRUE)




