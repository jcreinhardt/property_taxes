-- 01_build_panel.sql — Section 3 data build on the 1% building sample (Bouchet, DuckDB).
-- Run: cd ~/ptax_explore && srun -p day -c 8 --mem=96G -t 01:00:00 ~/bin/duckdb < salt/01_build_panel.sql
-- Inputs: ~/ptax_explore/samples/{tax_bld1pct, tax_bill_bld1pct_all, deeds_bld1pct, mortgage_bld1pct, da_bld1pct}.parquet
-- Outputs (~/ptax_explore/salt/out/): parcels.parquet (one row per 2016 parcel), panel.parquet (parcel x year 2010-2025),
--   mortgages.parquet (first-lien loan events), da_addr.parquet (Data Axle address-year composition), and diagnostic CSVs.
-- Names are hashes throughout; nothing identifying is written.
SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
SET threads=8; SET memory_limit='72GB';
.timer on
CREATE OR REPLACE MACRO uk(x) AS upper(trim(coalesce(x, '')));           -- unit string
CREATE OR REPLACE MACRO akey(b, u) AS hash(b, uk(u));                     -- address key = building key + unit

----------------------------------------------------------------------------------------------------------------------
-- 1. Tax roll: one row per parcel x tax year, 2010-2025, single family (10) and condo (11)
----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE tx AS
WITH t AS (
  SELECT clip, tax_year y, fips_code fips, situs_state st, left(situs_zip_code,5) zip5, property_indicator_code pic,
         situs_unit_number unit, bkey, akey(bkey, situs_unit_number) ak,
         owner_occupancy_code occ, coalesce(owner_1_corporate_indicator,false) OR coalesce(owner_2_corporate_indicator,false) corp,
         owner_relationship_type_code relcode, owner_etal_code etal, o1_ln, o2_ln, o1_full, o2_full,
         parcel_level_latitude lat, parcel_level_longitude lon, year_built, total_living_area_square_feet_all_buildings living_area,
         situs_delivery_point_validation_vacant_indicator dpv_vacant,
         row_number() OVER (PARTITION BY clip, tax_year ORDER BY last_assessor_update_date DESC NULLS LAST) rn
  FROM read_parquet('samples/tax_bld1pct.parquet')
  WHERE clip IS NOT NULL AND tax_year BETWEEN 2010 AND 2025 AND property_indicator_code IN (10, 11)
), b AS (
  SELECT clip, tax_year y, mail_here, mail_missing, homestead_exempt_indicator IS TRUE hs,
         senior_exempt_indicator IS TRUE senior_ex, disabled_exempt_indicator IS TRUE disabled_ex, veteran_exempt_indicator IS TRUE veteran_ex,
         total_tax_amount bill, net_tax_amount net_bill, assessed_total_value av, calculated_total_value mv, market_total_value mkt, appraised_total_value appr,
         assessed_year, coalesce(tax_area_code, tax_rate_area_code) tac, total_property_tax_rate_percent rate_pct,
         total_land_square_footage lot_sqft, total_number_of_acres acres, total_number_of_bedrooms_all_buildings bedrooms,
         total_number_of_bathrooms_all_buildings baths, total_number_of_stories stories,
         cbsa, municipality_code muni_code, municipality_name muni_name, tax_district_municipality_name tax_muni, township, school_district_name school_dist,
         row_number() OVER (PARTITION BY clip, tax_year ORDER BY total_tax_amount DESC NULLS LAST) rn
  FROM read_parquet('samples/tax_bill_bld1pct_all.parquet')
  WHERE clip IS NOT NULL AND tax_year BETWEEN 2010 AND 2025 AND property_indicator_code IN (10, 11)
)
SELECT t.* EXCLUDE (rn), b.* EXCLUDE (clip, y, rn)
FROM t LEFT JOIN b ON b.clip = t.clip AND b.y = t.y AND b.rn = 1
WHERE t.rn = 1;
SELECT 'tx rows' k, count(*) n, count(DISTINCT clip) parcels, sum((bill IS NULL)::INT) no_bill_join FROM tx;

----------------------------------------------------------------------------------------------------------------------
-- 2. Data Axle: address x file-year composition (all families at the address key)
----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE das AS
SELECT akey(bkey, unit_num) ak, record_year y,
       list_distinct(list_filter(flatten(list([ln_1, ln_2, ln_3, ln_4, ln_5])), lambda x: x IS NOT NULL)) surnames,
       list_distinct(list_filter(flatten(list([ln_1, ln_2, ln_3, ln_4, ln_5]) FILTER (WHERE primary_family_ind)), lambda x: x IS NOT NULL)) surnames_prim,
       count(*) n_fam, sum(primary_family_ind::INT) n_prim,
       sum((individual_id_1 IS NOT NULL)::INT + (individual_id_2 IS NOT NULL)::INT + (individual_id_3 IS NOT NULL)::INT
           + (individual_id_4 IS NOT NULL)::INT + (individual_id_5 IS NOT NULL)::INT) n_persons,
       max(CASE WHEN primary_family_ind THEN length_of_residence END) lor_prim,
       max(CASE WHEN primary_family_ind THEN age_1 END) age_prim,
       max(CASE WHEN primary_family_ind THEN family_id END) fam_prim,
       max(CASE WHEN primary_family_ind THEN owner_renter_status END) ownrent_prim,
       max(CASE WHEN primary_family_ind THEN marital_status END) marital_prim,
       max(CASE WHEN primary_family_ind THEN find END) find_prim,
       max(ge_als_census_tract_2010) tract, max(ge_als_census_bg_2010) bg
FROM read_parquet('samples/da_bld1pct.parquet')
WHERE bkey IS NOT NULL AND record_year BETWEEN 2010 AND 2023 AND record_year <> 2022
GROUP BY 1, 2;
COPY das TO 'salt/out/da_addr.parquet' (FORMAT parquet, COMPRESSION zstd);

-- crosswalk match rates by state x year (base universe: non-corporate SFR/condo on the roll)
COPY (
  SELECT tx.st, tx.y, tx.pic, count(*) n_parcels, avg((d.ak IS NOT NULL)::INT) match_rate
  FROM tx LEFT JOIN das d ON d.ak = tx.ak AND d.y = tx.y
  WHERE NOT corp AND tx.y BETWEEN 2012 AND 2023 GROUP BY ALL ORDER BY ALL
) TO 'salt/out/diag_match_rates.csv' (HEADER);

----------------------------------------------------------------------------------------------------------------------
-- 3. Deeds: arm's-length sales and latest pre-2017 deed (vesting, purchase)
----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE sales AS
SELECT clip, year(sale_derived_date) y, min(sale_derived_date) sale_date, arg_min(sale_amount, sale_derived_date) price,
       count(*) n_sales, bool_or(coalesce(new_construction_indicator,false)) newcon, bool_or(coalesce(investor_purchase_indicator,false)) investor_buy,
       bool_or(coalesce(buyer_1_corporate_indicator,false)) corp_buyer
FROM read_parquet('samples/deeds_bld1pct.parquet')
WHERE clip IS NOT NULL AND primary_category_code = 'A' AND NOT coalesce(interfamily_related_indicator,false) AND sale_amount > 0
  AND sale_derived_date IS NOT NULL AND year(sale_derived_date) BETWEEN 2000 AND 2026
GROUP BY 1, 2;
CREATE OR REPLACE TABLE deed16 AS            -- latest deed of any kind recorded by end-2016 (vesting / relationship of the 2016 owners)
SELECT clip, arg_max(buyer_ownership_rights_code, sale_derived_date) vest, arg_max(buyer_relationship_type_code, sale_derived_date) buyer_rel,
       arg_max(sale_derived_date, sale_derived_date) deed_date, arg_max(primary_category_code, sale_derived_date) deed_cat
FROM read_parquet('samples/deeds_bld1pct.parquet')
WHERE clip IS NOT NULL AND sale_derived_date IS NOT NULL AND sale_derived_date <= DATE '2016-12-31' GROUP BY 1;
CREATE OR REPLACE TABLE buy16 AS             -- latest arm's-length purchase by end-2016
SELECT clip, arg_max(sale_date, sale_date) buy_date, arg_max(price, sale_date) buy_price FROM sales WHERE sale_date <= DATE '2016-12-31' GROUP BY 1;
CREATE OR REPLACE TABLE price1416 AS SELECT clip, arg_max(price, sale_date) price1416, arg_max(y, sale_date) price1416_y FROM sales WHERE y BETWEEN 2014 AND 2016 GROUP BY 1;

----------------------------------------------------------------------------------------------------------------------
-- 4. Mortgages: first-lien loan events (amortisation and cash-out done in R)
----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE mort AS
SELECT clip, mortgage_date mdate, mortgage_recording_date rec_date, mortgage_amount amt, mortgage_type_code mtype, mortgage_purpose_code purpose,
       primary_category_code primc, mortgage_interest_rate irate, mortgage_term term, mortgage_term_code term_code,
       coalesce(refinance_loan_indicator,false) refi_ind, coalesce(equity_loan_indicator,false) equity_ind, coalesce(fixed_rate_indicator,false) fixed_ind,
       coalesce(variable_rate_loan_indicator,false) arm_ind, coalesce(construction_loan_indicator,false) constr_ind, coalesce(fha_loan_indicator,false) fha_ind,
       coalesce(veterans_administration_loan_indicator,false) va_ind, coalesce(second_home_rider_indicator,false) second_home_ind,
       mortgage_document_type_code doctype, mortgage_sequence_number seq, bo1_ln, bo2_ln
FROM read_parquet('samples/mortgage_bld1pct.parquet')
WHERE clip IS NOT NULL AND mortgage_date IS NOT NULL AND mortgage_amount > 0 AND year(mortgage_date) BETWEEN 1995 AND 2026
  AND coalesce(mortgage_type_code, '') <> 'J' AND NOT coalesce(equity_loan_indicator,false) AND coalesce(doctype,'') NOT IN ('EQ','RV','MO','AP','CD','RD');
COPY (SELECT m.* FROM mort m WHERE clip IN (SELECT clip FROM tx)) TO 'salt/out/mortgages.parquet' (FORMAT parquet, COMPRESSION zstd);

----------------------------------------------------------------------------------------------------------------------
-- 5. 2016 base row per parcel (fallback 2015), exposure, occupancy, co-owner class, value adjustment
----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE base AS
SELECT * FROM (
  SELECT tx.*, CASE WHEN y = 2016 THEN 0 ELSE 1 END fallback, row_number() OVER (PARTITION BY clip ORDER BY y DESC) rn
  FROM tx WHERE y IN (2015, 2016) AND bill > 0
) WHERE rn = 1;
-- county sales ratio: calculated value / arm's-length price, 2015-2017 sales, value from the roll of the sale year
CREATE OR REPLACE TABLE ratio_pairs AS
SELECT tx.fips, coalesce(tx.tac, '') tac, tx.mv / s.price rr
FROM sales s JOIN tx ON tx.clip = s.clip AND tx.y = s.y
WHERE s.y BETWEEN 2015 AND 2017 AND tx.mv > 1000 AND s.price > 10000 AND tx.mv / s.price BETWEEN 0.05 AND 5;
CREATE OR REPLACE TABLE ratio_tac AS SELECT fips, tac, count(*) n_sales, median(rr) ratio_c FROM ratio_pairs GROUP BY 1, 2 HAVING count(*) >= 30;
CREATE OR REPLACE TABLE ratio AS SELECT fips, count(*) n_sales, median(rr) ratio_c FROM ratio_pairs GROUP BY 1 HAVING count(*) >= 30;
CREATE OR REPLACE TABLE parcels AS
SELECT b.clip, b.y base_y, b.fallback, b.fips, b.st, b.zip5, b.pic, b.bkey, b.ak, b.unit, b.corp, b.occ, b.relcode, b.etal,
       b.o1_ln, b.o2_ln, b.o1_full, b.o2_full, (b.o2_full IS NOT NULL)::INT + 1 n_owners,
       b.lat, b.lon, b.year_built, b.living_area, b.lot_sqft, b.acres, b.bedrooms, b.baths, b.stories,
       b.cbsa, b.muni_code, b.muni_name, b.tax_muni, b.township, b.school_dist, b.tac, b.assessed_year, b.rate_pct,
       d.tract, d.bg,
       -- occupancy signals (2016 file for Data Axle; the base roll row for the other two)
       b.hs hs16, b.mail_here mh16, b.mail_missing mail_missing16, (d.ak IS NOT NULL) da16,
       coalesce(list_contains(d.surnames, b.o1_ln) OR list_contains(d.surnames, b.o2_ln), false) nm16,
       b.hs OR b.mail_here OR coalesce(list_contains(d.surnames, b.o1_ln) OR list_contains(d.surnames, b.o2_ln), false) oo16,
       -- exposure
       b.bill bill16, b.av av16, b.mv mv16, b.mkt mkt16, b.appr appr16,
       coalesce(rt.ratio_c, r.ratio_c) ratio_c, coalesce(rt.n_sales, r.n_sales) ratio_n, CASE WHEN rt.ratio_c IS NOT NULL THEN 'tac' WHEN r.ratio_c IS NOT NULL THEN 'county' END ratio_level,
       b.mv / CASE WHEN coalesce(rt.ratio_c, r.ratio_c) IS NULL OR coalesce(rt.ratio_c, r.ratio_c) BETWEEN 0.85 AND 1.15 THEN 1 ELSE coalesce(rt.ratio_c, r.ratio_c) END value16,
       p.price1416, p.price1416_y, bu.buy_date, bu.buy_price, de.vest, de.buyer_rel, de.deed_date,
       d.lor_prim lor16, d.age_prim age16, d.n_persons n_persons16, d.n_fam n_fam16, d.marital_prim marital16, d.find_prim find16, d.ownrent_prim ownrent16
FROM base b
LEFT JOIN das d ON d.ak = b.ak AND d.y = 2016
LEFT JOIN ratio r ON r.fips = b.fips
LEFT JOIN ratio_tac rt ON rt.fips = b.fips AND rt.tac = coalesce(b.tac, '')
LEFT JOIN price1416 p ON p.clip = b.clip
LEFT JOIN buy16 bu ON bu.clip = b.clip
LEFT JOIN deed16 de ON de.clip = b.clip;
ALTER TABLE parcels ADD COLUMN inv16 BOOLEAN; UPDATE parcels SET inv16 = NOT oo16 AND (corp OR (NOT mh16 AND NOT mail_missing16));
ALTER TABLE parcels ADD COLUMN rate16 DOUBLE; UPDATE parcels SET rate16 = bill16 / nullif(value16, 0);
ALTER TABLE parcels ADD COLUMN above16 BOOLEAN; UPDATE parcels SET above16 = bill16 > 10000;
ALTER TABLE parcels ADD COLUMN rate_ok BOOLEAN; UPDATE parcels SET rate_ok = rate16 BETWEEN 0.0005 AND 0.10;
ALTER TABLE parcels ADD COLUMN coowner VARCHAR; UPDATE parcels SET coowner =
  CASE WHEN n_owners < 2 THEN 'single'
       WHEN relcode IN ('HW','MA','MM','MW') OR vest IN ('TY','CP') THEN 'married'
       WHEN o1_ln = o2_ln THEN 'likely_married'
       WHEN relcode IN ('SI','SM','SW','UM','UW','UN') OR vest IN ('JT','TC','RS','UI') THEN 'unmarried'
       ELSE 'other' END;
COPY parcels TO 'salt/out/parcels.parquet' (FORMAT parquet, COMPRESSION zstd);
SELECT 'parcels' k, count(*) n, sum(oo16::INT) oo, sum(inv16::INT) inv, sum((NOT corp AND oo16)::INT) base_oo, sum(fallback) fallback15 FROM parcels;

-- diagnostics: occupancy signal agreement; co-owner shares; descriptives
COPY (SELECT hs16, mh16, nm16, da16, count(*) n FROM parcels WHERE NOT corp GROUP BY ALL ORDER BY ALL) TO 'salt/out/diag_occupancy_signals.csv' (HEADER);
COPY (SELECT coowner, count(*) n FROM parcels WHERE NOT corp AND oo16 GROUP BY 1 ORDER BY 2 DESC) TO 'salt/out/diag_coowner.csv' (HEADER);
COPY (SELECT st, count(*) n, sum(oo16::INT) oo, sum(inv16::INT) inv, avg(above16::INT) FILTER (WHERE oo16) above16_oo,
             median(bill16) FILTER (WHERE oo16) med_bill16, median(rate16) FILTER (WHERE oo16 AND rate_ok) med_rate16,
             median(value16) FILTER (WHERE oo16) med_value16, avg((ratio_c IS NULL)::INT) no_ratio, median(ratio_c) med_ratio
      FROM parcels WHERE NOT corp GROUP BY 1 ORDER BY 1) TO 'salt/out/diag_by_state.csv' (HEADER);

----------------------------------------------------------------------------------------------------------------------
-- 6. Panel: parcel x year with outcomes
----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE grid AS SELECT clip, ak, fips, tac, yr.y FROM parcels, (SELECT unnest(range(2010, 2026)) y) yr;
-- exit from Data Axle surnames
CREATE OR REPLACE TABLE ex AS
SELECT g.clip, g.y, d0.n_fam, d0.n_prim, d0.n_persons, d0.lor_prim, d0.age_prim, d0.fam_prim, len(d0.surnames) n_surnames,
       CASE WHEN d0.ak IS NULL THEN NULL
            WHEN d1.ak IS NULL AND d2.ak IS NULL THEN NULL
            WHEN list_has_any(d0.surnames, coalesce(d1.surnames, []) || coalesce(d2.surnames, [])) THEN 0 ELSE 1 END exit,
       CASE WHEN d0.ak IS NULL OR dm.ak IS NULL THEN NULL WHEN list_has_any(d0.surnames, dm.surnames) THEN 0 ELSE 1 END new_hh,   -- surname set disjoint from t-1
       -- labelled alternative (decision log 2026-10-16): primary-family surnames only, because stale secondary records keep sellers' names alive
       CASE WHEN d0.ak IS NULL OR len(d0.surnames_prim) = 0 THEN NULL
            WHEN d1.ak IS NULL AND d2.ak IS NULL THEN NULL
            WHEN list_has_any(d0.surnames_prim, coalesce(d1.surnames_prim, []) || coalesce(d2.surnames_prim, [])) THEN 0 ELSE 1 END exit_prim
FROM grid g
LEFT JOIN das d0 ON d0.ak = g.ak AND d0.y = g.y
LEFT JOIN das d1 ON d1.ak = g.ak AND d1.y = CASE WHEN g.y = 2021 THEN 2023 ELSE g.y + 1 END
LEFT JOIN das d2 ON d2.ak = g.ak AND d2.y = CASE WHEN g.y = 2020 THEN 2023 WHEN g.y = 2021 THEN NULL ELSE g.y + 2 END
LEFT JOIN das dm ON dm.ak = g.ak AND dm.y = CASE WHEN g.y = 2023 THEN 2021 ELSE g.y - 1 END;
-- roll-based outcomes: to_rental, appeal proxy, owner change
CREATE OR REPLACE TABLE cellchg AS
SELECT fips, coalesce(tac,'') tac, y, count(*) n_cell, avg((abs(av / lag_av - 1) > 0.03)::INT) share_chg
FROM (SELECT clip, fips, tac, y, av, lag(av) OVER (PARTITION BY clip ORDER BY y) lag_av, lag(y) OVER (PARTITION BY clip ORDER BY y) lag_y FROM tx)
WHERE lag_y = y - 1 AND av > 0 AND lag_av > 0 GROUP BY ALL;
CREATE OR REPLACE TABLE roll AS
SELECT r.clip, r.y, r.bill, r.av, r.mv, r.hs, r.mail_here, r.mail_missing, r.occ, r.assessed_year, r.tac,
       (r.lag_y = r.y - 1 AND r.o1_full = r.lag_o1) owner_same,
       (r.lag_y = r.y - 1 AND r.o1_full <> r.lag_o1) owner_change,
       CASE WHEN r.lag_y <> r.y - 1 OR r.o1_full <> r.lag_o1 OR r.o1_full IS NULL THEN NULL
            WHEN (r.lag_hs AND NOT r.hs) OR (r.lag_mh AND NOT r.mail_here AND NOT r.mail_missing) THEN 1 ELSE 0 END to_rental,
       CASE WHEN r.lag_y <> r.y - 1 OR r.av IS NULL OR r.lag_av IS NULL OR r.av <= 0 OR r.lag_av <= 0 OR c.share_chg IS NULL THEN NULL
            WHEN c.share_chg >= 0.20 THEN 0
            WHEN r.av / r.lag_av < 0.97 THEN 1 ELSE 0 END appeal_proxy,
       c.share_chg >= 0.20 reval_year, r.lag_y IS NOT NULL AND r.lag_y = r.y - 1 has_lag
FROM (SELECT tx.*, lag(y) OVER w lag_y, lag(o1_full) OVER w lag_o1, lag(hs) OVER w lag_hs, lag(mail_here) OVER w lag_mh, lag(av) OVER w lag_av
      FROM tx WINDOW w AS (PARTITION BY clip ORDER BY y)) r
LEFT JOIN cellchg c ON c.fips = r.fips AND c.tac = coalesce(r.tac,'') AND c.y = r.y;
CREATE OR REPLACE TABLE panel AS
SELECT g.clip, g.y,
       (ro.clip IS NOT NULL) in_roll, ro.bill, ro.av, ro.mv, ro.hs, ro.mail_here, ro.occ, ro.assessed_year, ro.owner_same, ro.owner_change, ro.to_rental, ro.appeal_proxy, ro.reval_year,
       (s.clip IS NOT NULL)::INT sale, CASE WHEN s.clip IS NOT NULL THEN ln(s.price) END log_price, s.price, s.newcon, s.investor_buy, s.corp_buyer,
       e.exit, e.exit_prim, e.new_hh, e.n_fam, e.n_prim, e.n_persons, e.n_surnames, e.lor_prim, e.age_prim, e.fam_prim
FROM grid g
LEFT JOIN roll ro ON ro.clip = g.clip AND ro.y = g.y
LEFT JOIN sales s ON s.clip = g.clip AND s.y = g.y
LEFT JOIN ex e ON e.clip = g.clip AND e.y = g.y;
COPY panel TO 'salt/out/panel.parquet' (FORMAT parquet, COMPRESSION zstd);
SELECT 'panel' k, count(*) n, sum(in_roll::INT) in_roll, sum(sale) sales, sum(exit) exits, count(exit) exit_obs, sum(to_rental) to_rental, sum(appeal_proxy) appeals FROM panel;

----------------------------------------------------------------------------------------------------------------------
-- 7. Exit validation (Section 3 step 3): exit given sale, false positives
----------------------------------------------------------------------------------------------------------------------
COPY (
  WITH p AS (SELECT p.*, pa.oo16, pa.corp, pa.st, pa.pic,
                    lag(exit) OVER (PARTITION BY p.clip ORDER BY y) exit_lag, lag(exit_prim) OVER (PARTITION BY p.clip ORDER BY y) exitp_lag,
                    sum(sale) OVER (PARTITION BY p.clip ORDER BY y ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) sales_near,
                    lead(owner_same, 1) OVER (PARTITION BY p.clip ORDER BY y) same1, lead(owner_same, 2) OVER (PARTITION BY p.clip ORDER BY y) same2
             FROM panel p JOIN parcels pa USING (clip) WHERE pa.oo16 AND NOT pa.corp)
  SELECT y, pic,
         count(*) FILTER (WHERE sale = 1 AND exit IS NOT NULL) n_sales,
         avg(exit) FILTER (WHERE sale = 1) exit_t_given_sale,
         avg(greatest(coalesce(exit_lag, 0), exit)) FILTER (WHERE sale = 1 AND exit IS NOT NULL) exit_tm1_or_t_given_sale,
         avg(exit) FILTER (WHERE sales_near = 0 AND owner_same AND same1 AND same2) exit_false_pos,
         count(*) FILTER (WHERE sales_near = 0 AND owner_same AND same1 AND same2 AND exit IS NOT NULL) n_stayers,
         avg(exit) exit_rate_all, avg((exit IS NULL)::INT) exit_missing,
         avg(exit_prim) FILTER (WHERE sale = 1) exitp_t_given_sale,
         avg(greatest(coalesce(exitp_lag, 0), exit_prim)) FILTER (WHERE sale = 1 AND exit_prim IS NOT NULL) exitp_tm1_or_t_given_sale,
         avg(exit_prim) FILTER (WHERE sales_near = 0 AND owner_same AND same1 AND same2) exitp_false_pos,
         avg(exit_prim) exitp_rate_all, avg((exit_prim IS NULL)::INT) exitp_missing
  FROM p WHERE y BETWEEN 2012 AND 2021 GROUP BY ALL ORDER BY ALL
) TO 'salt/out/diag_exit_validation.csv' (HEADER);
SELECT 'done' k;
