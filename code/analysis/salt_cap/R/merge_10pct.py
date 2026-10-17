# merge_10pct.py — append the 10% (bld10pct) rows from the cluster run to output/analysis/salt_cap/estimates.csv (keyed by strategy, outcome, sample, spec_id).
import csv, glob, pathlib
O = pathlib.Path("/Users/jr2728/PhD/projects/property_taxes/output/analysis/salt_cap")
key = lambda r: (r["strategy"], r["outcome"], r["sample"], r["spec_id"])
main = list(csv.DictReader(open(O / "estimates.csv")))
rows = {key(r): r for r in main if r["sample"] != "bld10pct"}
src = [O / "bld10pct/estimates_main_cluster.csv"] + sorted(glob.glob(str(O / "bld10pct/parts/estimates_*.csv")))
n = 0
for f in src:
    for r in csv.DictReader(open(f)):
        if r["sample"] == "bld10pct": rows[key(r)] = r; n += 1
with open(O / "estimates.csv", "w", newline="") as fh:
    w = csv.DictWriter(fh, fieldnames=["strategy", "outcome", "sample", "coef", "se", "N", "clusters", "spec_id"]); w.writeheader(); w.writerows(rows.values())
print("10% rows merged:", n, " total rows:", len(rows))
