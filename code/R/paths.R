# code/R/paths.R -- repository root and the standard folders. Every script starts with
#   source("code/R/paths.R")
# and is run from the repository root (Rscript code/<...>/script.R). ROOT is found from the git root of the working
# directory, or taken from the environment variable PTAX_ROOT (set it on a cluster copy of the repository).
# Also loads code/R/db.R (DuckDB connections) and code/R/theme.R (ggplot theme).
ROOT <- Sys.getenv("PTAX_ROOT")
if (!nzchar(ROOT)) ROOT <- tryCatch(rprojroot::find_root(rprojroot::is_git_root), error = function(e) normalizePath(getwd()))
DATA <- file.path(ROOT, "data")     # git-ignored: raw downloads, cluster exports, built panels
OUTPUT <- file.path(ROOT, "output") # figures and small tables (tracked), everything else ignored

# out_dir("analysis/salt_local") -> output/analysis/salt_local, created if missing.
out_dir <- function(...) { d <- file.path(OUTPUT, ...); dir.create(d, showWarnings = FALSE, recursive = TRUE); d }

source(file.path(ROOT, "code/R/db.R"))
source(file.path(ROOT, "code/R/theme.R"))
