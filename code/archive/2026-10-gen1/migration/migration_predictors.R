#!/usr/bin/env Rscript

# Migration predictors screen (10% ACS sample, household heads):
#   1. Bivariate R²: share of Var(mover) explained by each predictor alone
#   2. LASSO selection, then variance decomposition of selected predictors

gc()
rm(list = ls())

library(duckdb)
library(DBI)
library(dplyr)
library(glue)
library(purrr)
library(tibble)
library(stringr)
library(cli)
library(scales)
library(Matrix)
library(glmnet)

DB_PATH <- "../../../data/database.db"
OUTPUT_DIR <- "../../../output/analysis/migration/migration_predictors"
if (!dir.exists(OUTPUT_DIR)) {
  dir.create(OUTPUT_DIR, recursive = TRUE)
}

SAMPLE_FRAC <- 0.10
SEED        <- 42L

VAR_GROUPS <- list(
  housing = c(
    "gq", "ownershp", "ownershpd", "mortgage", "mortgag2",
    "mortamt1", "mortamt2", "taxincl", "insincl", "propinsr",
    "proptx99", "owncost", "rent", "rentgrs", "rentmeal",
    "hhincome", "valueh", "vacancy", "rooms", "builtyr2",
    "unitsstr", "bedrooms"
  ),
  household_composition = c(
    "coupletype", "ssmc", "nfams", "nsubfam", "ncouples",
    "nmothers", "nfathers", "multgen", "multgend", "cbnsubfam",
    "nchild", "nchlt5", "related", "gchouse"
  ),
  demographics = c(
    "sex", "age", "birthqtr", "birthyr", "marst", "marrno",
    "marrinyr", "yrmarr", "divinyr", "widinyr", "fertyr",
    "race", "raced", "hispan", "hispand"
  ),
  nativity_language = c(
    "bpl", "bpld", "citizen", "yrnatur", "yrimmig",
    "yrsusa1", "yrsusa2", "language", "languaged", "speakeng"
  ),
  education = c(
    "school", "educ", "educd", "gradeatt", "gradeattd", "schltype",
    "degfield", "degfieldd", "degfield2", "degfield2d"
  ),
  labor_occupation = c(
    "empstat", "empstatd", "labforce", "occ", "occ2010", "ind",
    "indnaics", "wkswork1", "uhrswork", "looking", "occscore"
  ),
  income = c("inctot", "ftotinc", "incwage")
)

PREDICTORS <- unique(unlist(VAR_GROUPS, use.names = FALSE))

VAR_TO_GROUP <- stats::setNames(
  rep(names(VAR_GROUPS), lengths(VAR_GROUPS)),
  PREDICTORS
)

CONTINUOUS_VARS <- c(
  "age", "birthyr", "yrmarr", "yrsusa1", "yrsusa2", "yrnatur", "yrimmig",
  "occscore", "uhrswork", "wkswork1", "nchild", "nchlt5", "nfams", "rooms",
  "bedrooms", "marrno"
)

MISSING_NUMERIC <- c(9999999L, 9999998L, 9999997L, 999999L, 99999L, 9999L)

clean_predictor <- function(x, var_name) {
  x <- if (is.factor(x)) as.vector(x) else x
  num <- suppressWarnings(as.numeric(x))
  if (!all(is.na(num))) {
    x <- num
    x[x %in% MISSING_NUMERIC] <- NA_real_
    if (var_name %in% c("hhincome", "valueh", "inctot", "ftotinc", "incwage")) {
      x[x >= 9999990] <- NA_real_
    }
    if (var_name %in% c("rent", "owncost", "rentgrs")) {
      x[x >= 99990] <- NA_real_
    }
    if (var_name == "valueh") {
      x[x < 0] <- NA_real_
    }
  }
  x
}

collapse_factor_levels <- function(x, max_levels = 40L) {
  fx <- factor(x)
  if (nlevels(fx) <= max_levels) {
    return(fx)
  }
  tab <- sort(table(fx), decreasing = TRUE)
  keep <- names(tab)[seq_len(max_levels)]
  out <- fx
  levels(out) <- c(levels(fx), "_other_")
  out[!as.character(fx) %in% keep] <- "_other_"
  droplevels(out)
}

r_squared_mover <- function(y, x, var_name, min_n = 500L) {
  ok <- !is.na(y) & !is.na(x)
  if (sum(ok) < min_n) {
    return(NA_real_)
  }
  y <- y[ok]
  x <- x[ok]
  vy <- stats::var(y)
  if (!is.finite(vy) || vy <= 0) {
    return(NA_real_)
  }
  if (dplyr::n_distinct(x) < 2L) {
    return(NA_real_)
  }
  use_factor <- !var_name %in% CONTINUOUS_VARS
  if (use_factor) {
    x <- collapse_factor_levels(x)
  }
  fit <- tryCatch(
    stats::lm(if (use_factor) y ~ x else y ~ x),
    error = function(e) NULL
  )
  if (is.null(fit)) {
    return(NA_real_)
  }
  y_hat <- stats::fitted(fit)
  stats::cor(y, y_hat)^2
}

prepare_lasso_data <- function(dat) {
  out <- dat
  for (v in PREDICTORS) {
    out[[v]] <- clean_predictor(out[[v]], v)
  }

  # Drop only truly degenerate predictors (constant after cleaning)
  keep <- PREDICTORS[vapply(
    PREDICTORS,
    function(v) dplyr::n_distinct(out[[v]], na.rm = TRUE) >= 2L,
    logical(1)
  )]

  dropped <- setdiff(PREDICTORS, keep)
  if (length(dropped) > 0L) {
    cli::cli_alert_warning(
      "Dropped {length(dropped)} constant predictors: {paste(dropped, collapse = ', ')}"
    )
  }

  factor_keep <- setdiff(keep, CONTINUOUS_VARS)
  cont_keep   <- intersect(keep, CONTINUOUS_VARS)

  for (v in factor_keep) {
    x <- collapse_factor_levels(out[[v]])
    x <- factor(x, levels = c(levels(x), "_missing_"))
    x[is.na(x)] <- "_missing_"
    out[[v]] <- x
  }

  for (v in cont_keep) {
    x <- as.numeric(out[[v]])
    med <- stats::median(x, na.rm = TRUE)
    if (!is.finite(med)) {
      med <- 0
    }
    x[is.na(x)] <- med
    out[[v]] <- as.numeric(scale(x))
  }

  out |>
    select(mover, all_of(keep)) |>
    stats::na.omit()
}

build_lasso_matrix <- function(lasso_dat) {
  cont_preds <- intersect(CONTINUOUS_VARS, names(lasso_dat))
  factor_preds <- setdiff(names(lasso_dat), c("mover", cont_preds))

  X_cont <- if (length(cont_preds) > 0L) {
  as(Matrix::Matrix(as.matrix(lasso_dat[, cont_preds, drop = FALSE])), "sparseMatrix")
  } else {
    NULL
  }

  # High-cardinality codes get their own blocks (like acs_income_exploration.R)
  heavy_factors <- c("occ", "occ2010", "ind", "indnaics", "educd", "raced", "hispand",
                     "bpld", "languaged", "degfieldd", "degfield2d")
  heavy_present   <- intersect(heavy_factors, factor_preds)
  routine_factors <- setdiff(factor_preds, heavy_present)

  make_block <- function(vars) {
    vars <- vars[vapply(
      vars,
      function(v) dplyr::n_distinct(lasso_dat[[v]], na.rm = TRUE) >= 2L,
      logical(1)
    )]
    if (length(vars) == 0L) {
      return(NULL)
    }
    rhs <- paste(sprintf("factor(%s)", vars), collapse = " + ")
    fml <- stats::as.formula(paste("~ 0 +", rhs))
    Matrix::sparse.model.matrix(fml, data = lasso_dat)
  }

  group_blocks <- purrr::imap(VAR_GROUPS, function(vars, grp) {
    vars <- intersect(vars, routine_factors)
    make_block(vars)
  })

  heavy_blocks <- lapply(heavy_present, function(v) {
    make_block(v)
  })

  factor_blocks <- c(group_blocks, heavy_blocks)

  X_blocks <- purrr::compact(factor_blocks)
  if (is.null(X_cont)) {
    do.call(Matrix::cbind2, X_blocks)
  } else {
    do.call(Matrix::cbind2, c(list(X_cont), X_blocks))
  }
}

# ── Pull 10% sample ──────────────────────────────────────────────────────────

con <- DBI::dbConnect(
  duckdb::duckdb(),
  dbdir = DB_PATH,
  read_only = TRUE
)
DBI::dbExecute(con, sprintf("SELECT setseed(%s)", SEED / 1e4))

select_cols <- paste(PREDICTORS, collapse = ", ")
pull_sql <- glue::glue("
  SELECT
    CASE WHEN migrate1 IN (2, 3, 4) THEN 1.0 ELSE 0.0 END AS mover,
    {select_cols}
  FROM acs_microdata
  WHERE pernum = 1
    AND gq IN (1, 2, 5)
    AND migrate1 != 0
    AND random() < {SAMPLE_FRAC}
")

cli::cli_alert_info("Pulling {SAMPLE_FRAC * 100}% sample from ACS microdata...")
dat <- DBI::dbGetQuery(con, pull_sql) |>
  tibble::as_tibble()
DBI::dbDisconnect(con, shutdown = TRUE)

cli::cli_alert_success(
  "N = {nrow(dat)} rows; mover rate = {scales::percent(mean(dat$mover), 0.1)}"
)

y <- dat$mover

# ── 1. Bivariate R² ──────────────────────────────────────────────────────────

results_bivariate <- purrr::imap_dfr(VAR_GROUPS, function(vars, group_name) {
  purrr::map_dfr(vars, function(v) {
    x <- clean_predictor(dat[[v]], v)
    r2 <- r_squared_mover(y, x, v)
    tibble::tibble(
      variable          = v,
      group             = group_name,
      n_nonmiss         = sum(!is.na(x)),
      n_levels          = dplyr::n_distinct(x[!is.na(x)]),
      r_squared         = r2,
      pct_var_explained = 100 * r2
    )
  })
}) |>
  mutate(group = factor(group, levels = names(VAR_GROUPS))) |>
  arrange(desc(r_squared))

utils::write.csv(
  results_bivariate,
  file.path(OUTPUT_DIR, "migration_predictors_bivariate.csv"),
  row.names = FALSE
)

cli::cli_h2("Bivariate R² ({SAMPLE_FRAC * 100}% sample)")
print(
  results_bivariate |>
    select(group, variable, pct_var_explained) |>
    slice_head(n = 15),
  n = 15
)

# ── 2. LASSO + variance decomposition ────────────────────────────────────────

cli::cli_h2("LASSO selection + variance decomposition")

lasso_dat <- prepare_lasso_data(dat)
cli::cli_alert_info(
  "LASSO estimation sample: N = {nrow(lasso_dat)} after listwise deletion"
)

X <- build_lasso_matrix(lasso_dat)
y_lasso <- lasso_dat$mover

cli::cli_alert_info(
  "Design matrix: {nrow(X)} rows, {ncol(X)} predictors"
)

set.seed(SEED)
cv_fit <- glmnet::cv.glmnet(
  X,
  y_lasso,
  family     = "gaussian",
  alpha      = 1,
  nfolds     = 5,
  parallel   = FALSE
)

cli::cli_alert_info(
  "lambda.min = {signif(cv_fit$lambda.min, 4)}; lambda.1se = {signif(cv_fit$lambda.1se, 4)}"
)

coefs <- coef(cv_fit, s = "lambda.1se") |>
  as.matrix() |>
  as.data.frame() |>
  stats::setNames("coefficient") |>
  tibble::rownames_to_column("variable") |>
  filter(variable != "(Intercept)", coefficient != 0) |>
  arrange(desc(abs(coefficient)))

cli::cli_alert_success("{nrow(coefs)} non-zero coefficients at lambda.1se")

utils::write.csv(
  coefs,
  file.path(OUTPUT_DIR, "migration_predictors_lasso_coefs.csv"),
  row.names = FALSE
)

selected_vars <- coefs$variable
beta_selected <- stats::setNames(coefs$coefficient, selected_vars)
y_hat <- as.numeric(X[, selected_vars, drop = FALSE] %*% beta_selected)
model_r2 <- stats::cor(y_lasso, y_hat)^2

var_decomp <- coefs |>
  mutate(
    base_var = stringr::str_replace(
      variable,
      "^factor\\(([^)]+)\\).*",
      "\\1"
    )
  ) |>
  group_by(base_var) |>
  group_modify(function(df, key) {
    comp <- as.numeric(X[, df$variable, drop = FALSE] %*% df$coefficient)
    tibble(var_component = stats::var(comp))
  }) |>
  ungroup()

coef_ranges <- coefs |>
  mutate(
    base_var = stringr::str_replace(
      variable,
      "^factor\\(([^)]+)\\).*",
      "\\1"
    )
  ) |>
  group_by(base_var) |>
  summarise(
    is_factor = any(stringr::str_detect(variable, "^factor\\(")),
    coef_range = max(c(0, coefficient)) - min(c(0, coefficient)),
    n_levels_selected = dplyr::n(),
    .groups = "drop"
  )

results_lasso <- var_decomp |>
  left_join(coef_ranges, by = "base_var") |>
  mutate(
    group        = VAR_TO_GROUP[base_var],
    pct_var_yhat = round(100 * var_component / stats::var(y_hat), 1),
    pct_var_y    = round(100 * var_component / stats::var(y_lasso), 1)
  ) |>
  arrange(desc(pct_var_yhat))

utils::write.csv(
  results_lasso,
  file.path(OUTPUT_DIR, "migration_predictors_lasso_variance.csv"),
  row.names = FALSE
)

cli::cli_alert_info(
  "Joint LASSO fit R² = {round(100 * model_r2, 2)}% of Var(mover)"
)
cli::cli_alert_info(
  "Variance shares sum to >100% of Var(y_hat) when predictors covary"
)

print(
  results_lasso |>
    select(
      group, base_var, is_factor, n_levels_selected,
      coef_range, pct_var_yhat, pct_var_y
    ) |>
    as.data.frame(),
  row.names = FALSE
)
