"""
County-to-state income tax savings, 2017.

For the average taxpayer in each county, computes:
  (a) tax_savings_thousands : annual state income taxes saved by moving to a
      given state (positive = saves money)
  (b) rate_diff             : difference in effective state income tax rate
      (positive = destination is cheaper)

Data source: IRS Statistics of Income (SOI) county and state returns, 2017.
  - state_and_local_income_taxes_amount (A18425): state income taxes deducted
    on federal returns. Observed only for itemizers, so this understates total
    state taxes paid. Cross-state *differences* in effective rates remain
    informative since the bias applies consistently across states.
  - Income estimated via AGI stub midpoints (no total AGI in database).

Output: /app/data/county_to_state_tax_savings_2017.csv
"""

import duckdb
import pandas as pd

DB_PATH = "../../../data/database.db"
OUT_PATH = "../../../data/county_to_state_tax_savings_2017.csv"

# AGI stub midpoints in $000s (matches SOI amount units)
# County data has stubs 1, 3, 4, 5, 6, 8 (2→3 and 7→8 merged on import)
STUB_MIDPOINTS = {1: 0.0, 3: 25.0, 4: 62.5, 5: 87.5, 6: 150.0, 8: 300.0}

# ---- Load data ----
con = duckdb.connect(DB_PATH, read_only=True)
county = con.execute(
    "SELECT county, statefips, countyname, agi_stub, returns, "
    "       state_and_local_income_taxes_amount "
    "FROM returns_county_agi WHERE year = 2017"
).df()
state = con.execute(
    "SELECT state, agi_stub, returns, state_and_local_income_taxes_amount "
    "FROM returns_state WHERE year = 2017"
).df()
con.close()

print(f"County rows: {len(county):,}")
print(f"State rows:  {len(state):,}")

# ---- County-level stats ----
county["stub_midpoint"] = county["agi_stub"].map(STUB_MIDPOINTS)

county_stats = (
    county.groupby(["county", "statefips", "countyname"])
    .apply(
        lambda g: pd.Series({
            "avg_agi_pp":           (g["stub_midpoint"] * g["returns"]).sum() / g["returns"].sum(),
            "avg_state_inc_tax_pp": g["state_and_local_income_taxes_amount"].sum() / g["returns"].sum(),
        }),
        include_groups=False,
    )
    .reset_index()
)
county_stats["origin_effective_rate"] = (
    county_stats["avg_state_inc_tax_pp"] / county_stats["avg_agi_pp"]
)
county_stats = county_stats.rename(columns={
    "county":    "county_fips",
    "statefips": "origin_state",
    "countyname": "county_name",
})
print(f"Counties: {len(county_stats):,}")

# ---- State-level effective income tax rates ----
# Merge stubs 2→3 and 7+→8 to match county stub coding
state["agi_stub"] = state["agi_stub"].apply(
    lambda s: 3 if s == 2 else (8 if s >= 8 else s)
)
state_agg = (
    state.groupby(["state", "agi_stub"])[
        ["state_and_local_income_taxes_amount", "returns"]
    ]
    .sum()
    .reset_index()
)
state_agg["stub_midpoint"] = state_agg["agi_stub"].map(STUB_MIDPOINTS)

state_rates = (
    state_agg.groupby("state")
    .apply(
        lambda g: pd.Series({
            "dest_effective_rate": (
                g["state_and_local_income_taxes_amount"].sum()
                / (g["stub_midpoint"] * g["returns"]).sum()
            ),
            "dest_inc_tax_pp": (
                g["state_and_local_income_taxes_amount"].sum() / g["returns"].sum()
            ),
        }),
        include_groups=False,
    )
    .reset_index()
    .rename(columns={"state": "dest_state"})
)
print(f"States: {len(state_rates):,}")

# ---- Cross-join counties × states, exclude same-state moves ----
county_stats["_key"] = 1
state_rates["_key"] = 1
result = county_stats.merge(state_rates, on="_key").drop(columns="_key")
result = result[result["dest_state"] != result["origin_state"]].copy()

result["rate_diff"] = result["origin_effective_rate"] - result["dest_effective_rate"]
result["tax_savings_thousands"] = result["avg_agi_pp"] * result["rate_diff"]

result = result.rename(columns={
    "avg_agi_pp":           "avg_agi_pp_thousands",
    "avg_state_inc_tax_pp": "avg_state_inc_tax_pp_thousands",
})
result = result[
    [
        "county_fips", "county_name", "origin_state", "dest_state",
        "avg_agi_pp_thousands", "avg_state_inc_tax_pp_thousands",
        "origin_effective_rate", "dest_effective_rate",
        "rate_diff", "tax_savings_thousands",
    ]
].sort_values(["county_fips", "dest_state"])

print(f"Output rows: {len(result):,}")

# ---- Verification ----
print("\n--- Effective rates by destination state (sorted descending) ---")
print(
    state_rates.sort_values("dest_effective_rate", ascending=False)
    .to_string(index=False)
)

print("\n--- Top CA counties: savings moving to TX (state 48) ---")
ca_tx = result[(result["origin_state"] == 6) & (result["dest_state"] == 48)]
print(
    ca_tx[["county_fips", "county_name", "avg_agi_pp_thousands",
           "origin_effective_rate", "rate_diff", "tax_savings_thousands"]]
    .sort_values("tax_savings_thousands", ascending=False)
    .head(5)
    .to_string(index=False)
)

print("\n--- TX counties: savings moving to CA (state 6) — expect negative ---")
tx_ca = result[(result["origin_state"] == 48) & (result["dest_state"] == 6)]
print(
    tx_ca[["county_fips", "county_name", "avg_agi_pp_thousands",
           "origin_effective_rate", "rate_diff", "tax_savings_thousands"]]
    .head(5)
    .to_string(index=False)
)

# ---- Save ----
result.to_csv(OUT_PATH, index=False)
print(f"\nSaved to: {OUT_PATH}")
