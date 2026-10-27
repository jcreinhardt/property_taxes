# The 44 "odd" county-years: tax roll present, parcels not matching by address key

**Question.** In the 1% residential address sample, 740 county-years have >= 90% of the parcels present in tax years
y-1 and y+1 missing in year y. For 93% of them the full `tax` table has no rows at all for (fips, y). The remaining 44
county-years (`odd_county_years.csv`; MA/NH/ME/CT/VT 2013-2015 plus 5143/2019) *do* have a roll with situs addresses in
year y. Hypothesis to test: a different address-formatting vintage in year y breaks the key
`hash(left(situs_zip_code,5), upper(trim(situs_street_name)), upper(trim(situs_house_number)))`.

**Answer: no.** It is not a formatting problem and it cannot be repaired by any address normalisation. The rows present
in year y belong to a **different, essentially disjoint set of towns** (New England rolls are town-based). The parcels
of the towns in year y-1 simply have no row in year y: it is a *town-level delivery gap*, and the CLIPs are stable.

## Evidence

Deep dive (25017 Middlesex MA 2014, 33015 Rockingham NH 2014, 23005 Cumberland ME 2014, 9011 New London CT 2015;
`odd_cy_profile.sql`, `odd_cy_towns.sql`, outputs `odd_cy_profile.txt`, `odd_cy_towns.txt`):

* Field completeness and formatting are identical across y-1, y, y+1: 100% ZIP5 present, 100% numeric house numbers,
  street names never contain the suffix (`situs_mode`) token, `situs_city_state_zip_source = 'S'` throughout; the
  share of residential codes, unit numbers, directions etc. is the same in every year.
* **0 of the y-1 CLIPs (and 0 of the y-1 APNs) appear in year y** in all four counties, while 97-99.9% of them reappear in
  y+1 (25017: 97,991 of 98,090) and 99.98% of the year-y CLIPs also appear in y+1 (25017: 373,804 of 373,828). Year
  y+1 is the union of the two rolls.
* By town (`situs_city`): 25017 tax_year 2013 = Lowell, Waltham, Watertown, Melrose, Wilmington, Acton, Stoneham, Hudson,
  Bedford, ... (60 towns); tax_year 2014 = Cambridge, Newton, Natick, Framingham, Medford, Somerville, Arlington, ...
  (93 towns); only stray rows (< 30 per town, mis-citied) overlap. Same for 9011 (2014 = Ledyard, Gales Ferry, Lisbon,
  Bozrah; 2015 = Norwich, Groton, Waterford, New London, ...), 23005 and 33015. The rows > distinct CLIPs in year y are
  just rows with NULL CLIP (`max_rows_per_clip` equals the NULL-CLIP count).
* Where a town *is* substantially present in both y-1 and y, the raw key already matches 85-99.6% of parcels (e.g.
  9011/2014: 99.5%, 33007/2013: 99.6%, 33015/2013: 97.3%), so there is nothing to normalise (`odd_cy_recovery.txt`, last table).

All 44 county-years (`odd_cy_recovery.sql` on the extracted parquet `odd_cy_all.parquet`, output `odd_cy_recovery.csv`):
base = residential parcels (property_indicator_code 10/11/21/22) with a situs address in year y-1 (1,372,673 parcels).

| pooled over 44 county-years, share of y-1 residential parcels | y-1 -> y (odd year) | y-1 -> y+1 (control) |
|---|---|---|
| match by raw key | 2.7% | 88.4% |
| match by normalised key (see below) | 2.7% (identical to raw in every county-year) | 88.4% |
| match by city+street+house-number key (no ZIP) | 2.6% | 88.0% |
| match by CLIP | 2.6% | 85.4% |
| parcel's town has < 5% of its y+1 rows in that year | **95.7%** | 9.2% |

36 of the 44 county-years have < 5% raw-key matches into year y; the eight with 5-27% (9011/2014, 23003/2013,
23005/2013, 23029/2013, 33007/2013, 33009/2013, 33015/2013, 33017/2013) are exactly those where a few towns do have rolls
in both years (`t_both_substantial` in `odd_cy_recovery.txt`). In no county-year does the normalised key add a single match,
and CLIP never beats the address key.

Town x tax_year coverage, all years, for the 44 fips (`odd_cy_town_years.sql` -> `odd_cy_town_years.csv`;
`odd_cy_town_patterns_local.sql` -> `odd_cy_town_patterns_2011_2017.csv`; pattern string = covered in 2011..2017):

| state | dominant town patterns (towns with >= 500 rows) |
|---|---|
| MA (25) | `0001111` 262 towns (first roll 2014); `0010111` 69 towns (2013, then 2015: **2014 missing**). **No MA town has both 2013 and 2014.** |
| CT (9) | `0010111` 69 towns (2013, then 2015: **2014 missing**); a handful of towns have 2014 only |
| ME (23) | `0101111` 120 towns, `1101111` 18 (**2013 missing**); `0001111` 28 (first roll 2014) |
| NH (33) | `1101111` 104 towns (**2013 missing**); `0001111` 56 (first roll 2014) |
| VT (50) | `1101111` 61 towns (**2013 missing**) |
| AR 5143 | all towns 2011-2017 covered; 2019 has a single row -> plain county-wide gap in 2019 |

So the odd years are: 2013 in ME/NH/VT (only a few towns delivered that year), 2014 in MA/CT (towns delivered in 2013 are
absent in 2014 and vice versa; in MA the 2013 roll is probably the FY2014 roll of those towns), and 2015 for 9011 (the four
towns delivered in 2014 are missing in 2015). The parcels present in y-1 and y+1 are missing in y because *their town*
has no roll in y.

## Normalised key tested (does not help, but is harmless)

```sql
hash(left(situs_zip_code,5),
     regexp_replace(upper(trim(situs_street_name)), '\s+' || coalesce(upper(trim(situs_mode)),'§') || '$', ''),  -- drop trailing suffix token if equal to situs_mode
     regexp_extract(upper(trim(situs_house_number)), '^[0-9]+'))                                                -- numeric prefix of the house number
```
It yields exactly the same match rates as the raw key in all 44 county-years and in the control pairs (no false matches,
no new matches), because in these states the suffix is never folded into `situs_street_name` and house numbers are already
purely numeric. A city-based variant (`hash(upper(trim(situs_city)), street, house_no)`) is marginally worse (ZIP is cleaner than city).

## Implication / recommended repair

Address-key matching is fine; the gap must be handled as **missing data at the (fips, town, tax_year) level**:
1. Build a town-year coverage table (as in `odd_cy_town_years.sql`, generalised to all fips) and flag (fips, situs_city,
   tax_year) cells with < ~20% of the town's typical annual row count as *not delivered*.
2. For parcels present in y-1 and y+1 whose town is not delivered in y, carry the y-1 record forward (or treat year y as
   missing) instead of counting an exit and re-entry. Match across the gap by the raw address key (88.4% y-1 -> y+1
   pooled; 99%+ in MA) or by CLIP (85.4%; CLIPs are slightly less stable than addresses in ME/NH between 2012 and 2014, e.g.
   33015/2013: 49.8% by CLIP vs 90.3% by key; 23031/2013: 85.9% vs 97.7%).
3. For MA/CT 2013-2014 specifically, each town has exactly one roll in the two-year window; if a yearly panel is needed, the
   2013 roll of the "2013 towns" can be treated as their 2014 observation (same parcels appear in 2015).

## Caveats
* Tests use the full table restricted to the 44 fips for years y-1, y, y+1 (extraction `odd_cy_extract.sql`, 26 min, 8 cores)
  and all years for the town matrix (`odd_cy_town_years.sql`, 2 min). "Town" = `upper(trim(situs_city))`, which is the postal
  city, not the assessing jurisdiction (e.g. Gales Ferry / Mystic / Niantic are villages); the conclusion does not depend on this.
* The control pair y-1 -> y+1 is < 80% in 9 county-years (e.g. 33005/2013: 18.7%, 33017/2013: 50%, 9011/2015: 48.8%) because
  year y+1 is itself partially delivered for those counties (`town_absent_in_next` column), not because of key failures.
* Residential filter: property_indicator_code in (10, 11, 21, 22); NULL-CLIP rows are kept in the key-based rates and
  count as non-matches in the CLIP-based rates (they are 0.5-2% of rows).
* Nothing was written under /nfs/roberts/pi/; cluster outputs are in /home/jr2728/ptax_explore/hh_strat/out/odd_cy_*.
  No owner/person names were exported (situs fields only).
