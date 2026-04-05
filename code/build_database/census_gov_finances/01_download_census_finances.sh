#!/bin/bash
# Download Census Bureau Annual Survey of Local Government Finances
# (ALFIN) Individual Unit Files for 2012-2022.
#
# IMPORTANT: ZIP file names vary across years and Census program cycles.
# Before running, verify each URL by browsing the FTP index:
#   curl -s <URL_BASE>/ | grep -i "zip\|IndFin\|Fin[0-9]"
# Update ZIP_NAMES below if any filename is wrong; the script will
# fail loudly rather than silently skip a missing year.
#
# Census-of-Governments years (full enumeration): 2012, 2017, 2022
# Annual survey years (sampled): 2013-2016, 2018-2021
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${SCRIPT_DIR}/../../../data/census_gov_finances/raw"

mkdir -p "$OUT_DIR"

# ---- URL bases by year ----
# CoG years use the 'cog' path; survey years use 'gov-finances'
declare -A URL_BASES
URL_BASES[2012]="https://www2.census.gov/programs-surveys/cog/datasets/2012"
URL_BASES[2013]="https://www2.census.gov/programs-surveys/gov-finances/datasets/2013"
URL_BASES[2014]="https://www2.census.gov/programs-surveys/gov-finances/datasets/2014"
URL_BASES[2015]="https://www2.census.gov/programs-surveys/gov-finances/datasets/2015"
URL_BASES[2016]="https://www2.census.gov/programs-surveys/gov-finances/datasets/2016"
URL_BASES[2017]="https://www2.census.gov/programs-surveys/cog/datasets/2017"
URL_BASES[2018]="https://www2.census.gov/programs-surveys/gov-finances/datasets/2018"
URL_BASES[2019]="https://www2.census.gov/programs-surveys/gov-finances/datasets/2019"
URL_BASES[2020]="https://www2.census.gov/programs-surveys/gov-finances/datasets/2020"
URL_BASES[2021]="https://www2.census.gov/programs-surveys/gov-finances/datasets/2021"
URL_BASES[2022]="https://www2.census.gov/programs-surveys/cog/datasets/2022"

# ---- ZIP file names ----
# These follow the Census naming conventions but MUST be verified before
# running. Pattern: YYIndFin_1_YY.zip (survey) or FinYY_1.zip (CoG).
declare -A ZIP_NAMES
ZIP_NAMES[2012]="Fin12_1.zip"
ZIP_NAMES[2013]="13IndFin_1_13.zip"
ZIP_NAMES[2014]="14IndFin_1_14.zip"
ZIP_NAMES[2015]="15IndFin_1_15.zip"
ZIP_NAMES[2016]="16IndFin_1_16.zip"
ZIP_NAMES[2017]="Fin17_1.zip"
ZIP_NAMES[2018]="18IndFin_1_18.zip"
ZIP_NAMES[2019]="19IndFin_1_19.zip"
ZIP_NAMES[2020]="20IndFin_1_20.zip"
ZIP_NAMES[2021]="21IndFin_1_21.zip"
ZIP_NAMES[2022]="Fin22_1.zip"

# ---- Download and extract ----
for year in $(seq 2012 2022); do
  base="${URL_BASES[$year]}"
  zipname="${ZIP_NAMES[$year]}"
  url="${base}/${zipname}"
  dest_dir="${OUT_DIR}/${year}"
  mkdir -p "$dest_dir"

  echo "=== ${year} ==="

  # Check URL exists before downloading
  if ! curl --head --silent --fail "$url" > /dev/null 2>&1; then
    echo "ERROR: URL not reachable: ${url}" >&2
    echo "Browse ${base}/ to find the correct filename and update ZIP_NAMES." >&2
    exit 1
  fi

  # Download (skip if already present)
  if [ -f "${dest_dir}/${zipname}" ]; then
    echo "  Already downloaded: ${zipname}"
  else
    echo "  Downloading: ${url}"
    curl -L -o "${dest_dir}/${zipname}" "$url"
  fi

  # Extract
  echo "  Extracting..."
  unzip -o "${dest_dir}/${zipname}" -d "$dest_dir"
  echo "  Done."
done

echo ""
echo "All years downloaded to: ${OUT_DIR}"
echo "Next step: verify file names and format in each year directory,"
echo "then run 02_import_census_finances.R"
