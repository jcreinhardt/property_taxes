-- Jurisdiction / tract panel 2012-2024 (Bouchet via run_sql.sbatch: corelogic.db ATTACHed READ_ONLY as cl): per year roll_tract, roll_area, sales (see k_panel_year.sql).
.timer on
CREATE TEMP TABLE sl AS
SELECT clip, year(sale_derived_date)::INT y, arg_min(sale_amount, sale_derived_date) price, bool_or(coalesce(new_construction_indicator, false)) newcon
FROM cl.ownertransfer
WHERE sale_derived_date BETWEEN DATE '2012-01-01' AND DATE '2024-12-31' AND primary_category_code = 'A'
  AND NOT coalesce(interfamily_related_indicator, false) AND sale_amount > 0 GROUP BY ALL;
-- Year 2019 of the jurisdiction/tract panel (SOM, read-only DB). Generated per year by k_panel.sbatch.
-- Roll: one row per clip (max bill / AV / MV over duplicate rows), all property types; residential = pic 10/11/21/22.
CREATE OR REPLACE TEMP TABLE r AS
SELECT clip, any_value(lpad(fips_code::VARCHAR, 5, '0')) fips, any_value(property_indicator_code) pic,
       any_value(left(census_id::VARCHAR, 6)) FILTER (WHERE length(census_id::VARCHAR) >= 6) tr6,
       any_value(coalesce(nullif(trim(tax_area_code), ''), nullif(trim(tax_rate_area_code), ''))) area,
       any_value(left(situs_zip_code, 5)) zip, max(total_tax_amount) bill, max(assessed_total_value) av, max(market_total_value) mv,
       max(total_property_tax_rate_percent) rate_pct, bool_or(coalesce(homestead_exempt_indicator, false)) hs
FROM cl.tax WHERE tax_year = 2019 GROUP BY clip;
COPY (SELECT fips, fips || tr6 tract, 2019 y, pic IN (10, 11, 21, 22) res, count(*) n, count(*) FILTER (WHERE bill > 0) n_bill,
             sum(bill) FILTER (WHERE bill > 0) bill, sum(av) FILTER (WHERE av > 0) av, sum(mv) FILTER (WHERE mv > 0) mv,
             median(bill / av) FILTER (WHERE bill > 0 AND av > 0) med_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r GROUP BY ALL) TO '/home/jr2728/ptax_explore/kpanel/roll_tract_2019.parquet' (FORMAT parquet);
COPY (SELECT fips, area, 2019 y, count(*) n, sum(bill) bill, sum(av) av, median(bill / av) med_nom, quantile_cont(bill / av, 0.25) q25_nom,
             quantile_cont(bill / av, 0.75) q75_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r WHERE pic IN (10, 11, 21, 22) AND bill > 0 AND av > 0 AND area IS NOT NULL GROUP BY ALL) TO '/home/jr2728/ptax_explore/kpanel/roll_area_2019.parquet' (FORMAT parquet);
-- Sales of the year joined to this year's roll
COPY (SELECT r.fips, r.fips || r.tr6 tract, r.zip, r.area, 2019 y, r.pic, r.hs, x.price, r.bill, r.av, r.mv, x.newcon, hash(r.clip) % 100 h
      FROM r JOIN sl x ON x.clip = r.clip AND x.y = 2019
      WHERE x.price > 10000 AND r.pic IN (10, 11, 21, 22)) TO '/home/jr2728/ptax_explore/kpanel/sales_2019.parquet' (FORMAT parquet);
-- Year 2020 of the jurisdiction/tract panel (SOM, read-only DB). Generated per year by k_panel.sbatch.
-- Roll: one row per clip (max bill / AV / MV over duplicate rows), all property types; residential = pic 10/11/21/22.
CREATE OR REPLACE TEMP TABLE r AS
SELECT clip, any_value(lpad(fips_code::VARCHAR, 5, '0')) fips, any_value(property_indicator_code) pic,
       any_value(left(census_id::VARCHAR, 6)) FILTER (WHERE length(census_id::VARCHAR) >= 6) tr6,
       any_value(coalesce(nullif(trim(tax_area_code), ''), nullif(trim(tax_rate_area_code), ''))) area,
       any_value(left(situs_zip_code, 5)) zip, max(total_tax_amount) bill, max(assessed_total_value) av, max(market_total_value) mv,
       max(total_property_tax_rate_percent) rate_pct, bool_or(coalesce(homestead_exempt_indicator, false)) hs
FROM cl.tax WHERE tax_year = 2020 GROUP BY clip;
COPY (SELECT fips, fips || tr6 tract, 2020 y, pic IN (10, 11, 21, 22) res, count(*) n, count(*) FILTER (WHERE bill > 0) n_bill,
             sum(bill) FILTER (WHERE bill > 0) bill, sum(av) FILTER (WHERE av > 0) av, sum(mv) FILTER (WHERE mv > 0) mv,
             median(bill / av) FILTER (WHERE bill > 0 AND av > 0) med_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r GROUP BY ALL) TO '/home/jr2728/ptax_explore/kpanel/roll_tract_2020.parquet' (FORMAT parquet);
COPY (SELECT fips, area, 2020 y, count(*) n, sum(bill) bill, sum(av) av, median(bill / av) med_nom, quantile_cont(bill / av, 0.25) q25_nom,
             quantile_cont(bill / av, 0.75) q75_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r WHERE pic IN (10, 11, 21, 22) AND bill > 0 AND av > 0 AND area IS NOT NULL GROUP BY ALL) TO '/home/jr2728/ptax_explore/kpanel/roll_area_2020.parquet' (FORMAT parquet);
-- Sales of the year joined to this year's roll
COPY (SELECT r.fips, r.fips || r.tr6 tract, r.zip, r.area, 2020 y, r.pic, r.hs, x.price, r.bill, r.av, r.mv, x.newcon, hash(r.clip) % 100 h
      FROM r JOIN sl x ON x.clip = r.clip AND x.y = 2020
      WHERE x.price > 10000 AND r.pic IN (10, 11, 21, 22)) TO '/home/jr2728/ptax_explore/kpanel/sales_2020.parquet' (FORMAT parquet);
-- Year 2021 of the jurisdiction/tract panel (SOM, read-only DB). Generated per year by k_panel.sbatch.
-- Roll: one row per clip (max bill / AV / MV over duplicate rows), all property types; residential = pic 10/11/21/22.
CREATE OR REPLACE TEMP TABLE r AS
SELECT clip, any_value(lpad(fips_code::VARCHAR, 5, '0')) fips, any_value(property_indicator_code) pic,
       any_value(left(census_id::VARCHAR, 6)) FILTER (WHERE length(census_id::VARCHAR) >= 6) tr6,
       any_value(coalesce(nullif(trim(tax_area_code), ''), nullif(trim(tax_rate_area_code), ''))) area,
       any_value(left(situs_zip_code, 5)) zip, max(total_tax_amount) bill, max(assessed_total_value) av, max(market_total_value) mv,
       max(total_property_tax_rate_percent) rate_pct, bool_or(coalesce(homestead_exempt_indicator, false)) hs
FROM cl.tax WHERE tax_year = 2021 GROUP BY clip;
COPY (SELECT fips, fips || tr6 tract, 2021 y, pic IN (10, 11, 21, 22) res, count(*) n, count(*) FILTER (WHERE bill > 0) n_bill,
             sum(bill) FILTER (WHERE bill > 0) bill, sum(av) FILTER (WHERE av > 0) av, sum(mv) FILTER (WHERE mv > 0) mv,
             median(bill / av) FILTER (WHERE bill > 0 AND av > 0) med_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r GROUP BY ALL) TO '/home/jr2728/ptax_explore/kpanel/roll_tract_2021.parquet' (FORMAT parquet);
COPY (SELECT fips, area, 2021 y, count(*) n, sum(bill) bill, sum(av) av, median(bill / av) med_nom, quantile_cont(bill / av, 0.25) q25_nom,
             quantile_cont(bill / av, 0.75) q75_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r WHERE pic IN (10, 11, 21, 22) AND bill > 0 AND av > 0 AND area IS NOT NULL GROUP BY ALL) TO '/home/jr2728/ptax_explore/kpanel/roll_area_2021.parquet' (FORMAT parquet);
-- Sales of the year joined to this year's roll
COPY (SELECT r.fips, r.fips || r.tr6 tract, r.zip, r.area, 2021 y, r.pic, r.hs, x.price, r.bill, r.av, r.mv, x.newcon, hash(r.clip) % 100 h
      FROM r JOIN sl x ON x.clip = r.clip AND x.y = 2021
      WHERE x.price > 10000 AND r.pic IN (10, 11, 21, 22)) TO '/home/jr2728/ptax_explore/kpanel/sales_2021.parquet' (FORMAT parquet);
-- Year 2022 of the jurisdiction/tract panel (SOM, read-only DB). Generated per year by k_panel.sbatch.
-- Roll: one row per clip (max bill / AV / MV over duplicate rows), all property types; residential = pic 10/11/21/22.
CREATE OR REPLACE TEMP TABLE r AS
SELECT clip, any_value(lpad(fips_code::VARCHAR, 5, '0')) fips, any_value(property_indicator_code) pic,
       any_value(left(census_id::VARCHAR, 6)) FILTER (WHERE length(census_id::VARCHAR) >= 6) tr6,
       any_value(coalesce(nullif(trim(tax_area_code), ''), nullif(trim(tax_rate_area_code), ''))) area,
       any_value(left(situs_zip_code, 5)) zip, max(total_tax_amount) bill, max(assessed_total_value) av, max(market_total_value) mv,
       max(total_property_tax_rate_percent) rate_pct, bool_or(coalesce(homestead_exempt_indicator, false)) hs
FROM cl.tax WHERE tax_year = 2022 GROUP BY clip;
COPY (SELECT fips, fips || tr6 tract, 2022 y, pic IN (10, 11, 21, 22) res, count(*) n, count(*) FILTER (WHERE bill > 0) n_bill,
             sum(bill) FILTER (WHERE bill > 0) bill, sum(av) FILTER (WHERE av > 0) av, sum(mv) FILTER (WHERE mv > 0) mv,
             median(bill / av) FILTER (WHERE bill > 0 AND av > 0) med_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r GROUP BY ALL) TO '/home/jr2728/ptax_explore/kpanel/roll_tract_2022.parquet' (FORMAT parquet);
COPY (SELECT fips, area, 2022 y, count(*) n, sum(bill) bill, sum(av) av, median(bill / av) med_nom, quantile_cont(bill / av, 0.25) q25_nom,
             quantile_cont(bill / av, 0.75) q75_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r WHERE pic IN (10, 11, 21, 22) AND bill > 0 AND av > 0 AND area IS NOT NULL GROUP BY ALL) TO '/home/jr2728/ptax_explore/kpanel/roll_area_2022.parquet' (FORMAT parquet);
-- Sales of the year joined to this year's roll
COPY (SELECT r.fips, r.fips || r.tr6 tract, r.zip, r.area, 2022 y, r.pic, r.hs, x.price, r.bill, r.av, r.mv, x.newcon, hash(r.clip) % 100 h
      FROM r JOIN sl x ON x.clip = r.clip AND x.y = 2022
      WHERE x.price > 10000 AND r.pic IN (10, 11, 21, 22)) TO '/home/jr2728/ptax_explore/kpanel/sales_2022.parquet' (FORMAT parquet);
-- Year 2023 of the jurisdiction/tract panel (SOM, read-only DB). Generated per year by k_panel.sbatch.
-- Roll: one row per clip (max bill / AV / MV over duplicate rows), all property types; residential = pic 10/11/21/22.
CREATE OR REPLACE TEMP TABLE r AS
SELECT clip, any_value(lpad(fips_code::VARCHAR, 5, '0')) fips, any_value(property_indicator_code) pic,
       any_value(left(census_id::VARCHAR, 6)) FILTER (WHERE length(census_id::VARCHAR) >= 6) tr6,
       any_value(coalesce(nullif(trim(tax_area_code), ''), nullif(trim(tax_rate_area_code), ''))) area,
       any_value(left(situs_zip_code, 5)) zip, max(total_tax_amount) bill, max(assessed_total_value) av, max(market_total_value) mv,
       max(total_property_tax_rate_percent) rate_pct, bool_or(coalesce(homestead_exempt_indicator, false)) hs
FROM cl.tax WHERE tax_year = 2023 GROUP BY clip;
COPY (SELECT fips, fips || tr6 tract, 2023 y, pic IN (10, 11, 21, 22) res, count(*) n, count(*) FILTER (WHERE bill > 0) n_bill,
             sum(bill) FILTER (WHERE bill > 0) bill, sum(av) FILTER (WHERE av > 0) av, sum(mv) FILTER (WHERE mv > 0) mv,
             median(bill / av) FILTER (WHERE bill > 0 AND av > 0) med_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r GROUP BY ALL) TO '/home/jr2728/ptax_explore/kpanel/roll_tract_2023.parquet' (FORMAT parquet);
COPY (SELECT fips, area, 2023 y, count(*) n, sum(bill) bill, sum(av) av, median(bill / av) med_nom, quantile_cont(bill / av, 0.25) q25_nom,
             quantile_cont(bill / av, 0.75) q75_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r WHERE pic IN (10, 11, 21, 22) AND bill > 0 AND av > 0 AND area IS NOT NULL GROUP BY ALL) TO '/home/jr2728/ptax_explore/kpanel/roll_area_2023.parquet' (FORMAT parquet);
-- Sales of the year joined to this year's roll
COPY (SELECT r.fips, r.fips || r.tr6 tract, r.zip, r.area, 2023 y, r.pic, r.hs, x.price, r.bill, r.av, r.mv, x.newcon, hash(r.clip) % 100 h
      FROM r JOIN sl x ON x.clip = r.clip AND x.y = 2023
      WHERE x.price > 10000 AND r.pic IN (10, 11, 21, 22)) TO '/home/jr2728/ptax_explore/kpanel/sales_2023.parquet' (FORMAT parquet);
-- Year 2024 of the jurisdiction/tract panel (SOM, read-only DB). Generated per year by k_panel.sbatch.
-- Roll: one row per clip (max bill / AV / MV over duplicate rows), all property types; residential = pic 10/11/21/22.
CREATE OR REPLACE TEMP TABLE r AS
SELECT clip, any_value(lpad(fips_code::VARCHAR, 5, '0')) fips, any_value(property_indicator_code) pic,
       any_value(left(census_id::VARCHAR, 6)) FILTER (WHERE length(census_id::VARCHAR) >= 6) tr6,
       any_value(coalesce(nullif(trim(tax_area_code), ''), nullif(trim(tax_rate_area_code), ''))) area,
       any_value(left(situs_zip_code, 5)) zip, max(total_tax_amount) bill, max(assessed_total_value) av, max(market_total_value) mv,
       max(total_property_tax_rate_percent) rate_pct, bool_or(coalesce(homestead_exempt_indicator, false)) hs
FROM cl.tax WHERE tax_year = 2024 GROUP BY clip;
COPY (SELECT fips, fips || tr6 tract, 2024 y, pic IN (10, 11, 21, 22) res, count(*) n, count(*) FILTER (WHERE bill > 0) n_bill,
             sum(bill) FILTER (WHERE bill > 0) bill, sum(av) FILTER (WHERE av > 0) av, sum(mv) FILTER (WHERE mv > 0) mv,
             median(bill / av) FILTER (WHERE bill > 0 AND av > 0) med_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r GROUP BY ALL) TO '/home/jr2728/ptax_explore/kpanel/roll_tract_2024.parquet' (FORMAT parquet);
COPY (SELECT fips, area, 2024 y, count(*) n, sum(bill) bill, sum(av) av, median(bill / av) med_nom, quantile_cont(bill / av, 0.25) q25_nom,
             quantile_cont(bill / av, 0.75) q75_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r WHERE pic IN (10, 11, 21, 22) AND bill > 0 AND av > 0 AND area IS NOT NULL GROUP BY ALL) TO '/home/jr2728/ptax_explore/kpanel/roll_area_2024.parquet' (FORMAT parquet);
-- Sales of the year joined to this year's roll
COPY (SELECT r.fips, r.fips || r.tr6 tract, r.zip, r.area, 2024 y, r.pic, r.hs, x.price, r.bill, r.av, r.mv, x.newcon, hash(r.clip) % 100 h
      FROM r JOIN sl x ON x.clip = r.clip AND x.y = 2024
      WHERE x.price > 10000 AND r.pic IN (10, 11, 21, 22)) TO '/home/jr2728/ptax_explore/kpanel/sales_2024.parquet' (FORMAT parquet);
