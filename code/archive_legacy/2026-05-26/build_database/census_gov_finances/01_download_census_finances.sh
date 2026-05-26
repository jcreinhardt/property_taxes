#!/bin/bash
# Download Census Bureau Annual Survey of Local Government Finances
# (ALFIN) Individual Unit Files for 2012-2022.
#
# URLs verified from each year's public-use-datasets page on census.gov.
# Census-of-Governments years (full enumeration): 2012, 2017, 2022
# Annual survey years (sampled): 2013-2016, 2018-2021
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${SCRIPT_DIR}/../../../data/census_gov_finances/raw"

mkdir -p "$OUT_DIR"

get_url() {
  case "$1" in
    2012) echo "https://www2.census.gov/programs-surveys/gov-finances/tables/2012/summary-tables/2012_Individual_Unit_file.zip" ;;
    2013) echo "https://www2.census.gov/programs-surveys/gov-finances/tables/2013/summary-tables/2013_Individual_Unit_file.zip" ;;
    2014) echo "https://www2.census.gov/programs-surveys/gov-finances/datasets/2014/public-use-datasets/2014-individual-unit-file.zip" ;;
    2015) echo "https://www2.census.gov/programs-surveys/gov-finances/datasets/2015/public-use-datasets/2015-individual-unit-file.zip" ;;
    2016) echo "https://www2.census.gov/programs-surveys/gov-finances/datasets/2016/public-use-datasets/2016_Individual_Unit_file.zip" ;;
    2017) echo "https://www2.census.gov/programs-surveys/gov-finances/tables/2017/2017_Individual_Unit_File.zip" ;;
    2018) echo "https://www2.census.gov/programs-surveys/gov-finances/tables/2018/2018_Individual_Unit_File.zip" ;;
    2019) echo "https://www2.census.gov/programs-surveys/gov-finances/tables/2019/2019_Individual_Unit_File.zip" ;;
    2020) echo "https://www2.census.gov/programs-surveys/gov-finances/tables/2020/2020_Individual_Unit_File.zip" ;;
    2021) echo "https://www2.census.gov/programs-surveys/gov-finances/tables/2021/2021_Individual_Unit_File.zip" ;;
    2022) echo "https://www2.census.gov/programs-surveys/gov-finances/tables/2022/2022_Individual_Unit_File.zip" ;;
    *) echo "ERROR: no URL defined for year $1" >&2; exit 1 ;;
  esac
}

for year in $(seq 2012 2022); do
  url="$(get_url "$year")"
  zipname="$(basename "$url")"
  dest_dir="${OUT_DIR}/${year}"
  mkdir -p "$dest_dir"

  echo "=== ${year} ==="

  if ! curl --head --silent --fail "$url" > /dev/null 2>&1; then
    echo "ERROR: URL not reachable: ${url}" >&2
    exit 1
  fi

  if [ -f "${dest_dir}/${zipname}" ]; then
    echo "  Already downloaded: ${zipname}"
  else
    echo "  Downloading: ${url}"
    curl -L -o "${dest_dir}/${zipname}" "$url"
  fi

  echo "  Extracting..."
  unzip -o "${dest_dir}/${zipname}" -d "$dest_dir"
  echo "  Done."
done

echo ""
echo "All years downloaded to: ${OUT_DIR}"
echo "Next step: run 02_import_census_finances.R"
