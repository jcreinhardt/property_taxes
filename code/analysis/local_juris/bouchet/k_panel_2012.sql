-- Jurisdiction / tract panel 2012-2024 (Bouchet via run_sql.sbatch: corelogic.db ATTACHed READ_ONLY as cl): per year roll_tract, roll_area, sales (see k_panel_year.sql).
.timer on
CREATE TEMP TABLE sl AS
SELECT clip, year(sale_derived_date)::INT y, arg_min(sale_amount, sale_derived_date) price, bool_or(coalesce(new_construction_indicator, false)) newcon
FROM cl.ownertransfer
WHERE sale_derived_date BETWEEN DATE '2012-01-01' AND DATE '2024-12-31' AND primary_category_code = 'A'
  AND NOT coalesce(interfamily_related_indicator, false) AND sale_amount > 0 GROUP BY ALL;
-- Year 2012 of the jurisdiction/tract panel (SOM, read-only DB). Generated per year by k_panel.sbatch.
-- Roll: one row per clip (max bill / AV / MV over duplicate rows), all property types; residential = pic 10/11/21/22.
CREATE OR REPLACE TEMP TABLE r AS
SELECT clip, any_value(lpad(fips_code::VARCHAR, 5, '0')) fips, any_value(property_indicator_code) pic,
       any_value(left(census_id::VARCHAR, 6)) FILTER (WHERE length(census_id::VARCHAR) >= 6) tr6,
       any_value(coalesce(nullif(trim(tax_area_code), ''), nullif(trim(tax_rate_area_code), ''))) area,
       any_value(left(situs_zip_code, 5)) zip, max(total_tax_amount) bill, max(assessed_total_value) av, max(market_total_value) mv,
       max(total_property_tax_rate_percent) rate_pct, bool_or(coalesce(homestead_exempt_indicator, false)) hs
FROM cl.tax WHERE tax_year = 2012 GROUP BY clip;
COPY (SELECT fips, fips || tr6 tract, 2012 y, pic IN (10, 11, 21, 22) res, count(*) n, count(*) FILTER (WHERE bill > 0) n_bill,
             sum(bill) FILTER (WHERE bill > 0) bill, sum(av) FILTER (WHERE av > 0) av, sum(mv) FILTER (WHERE mv > 0) mv,
             median(bill / av) FILTER (WHERE bill > 0 AND av > 0) med_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r GROUP BY ALL) TO '/home/jr2728/ptax_explore/kpanel/roll_tract_2012.parquet' (FORMAT parquet);
COPY (SELECT fips, area, 2012 y, count(*) n, sum(bill) bill, sum(av) av, median(bill / av) med_nom, quantile_cont(bill / av, 0.25) q25_nom,
             quantile_cont(bill / av, 0.75) q75_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r WHERE pic IN (10, 11, 21, 22) AND bill > 0 AND av > 0 AND area IS NOT NULL GROUP BY ALL) TO '/home/jr2728/ptax_explore/kpanel/roll_area_2012.parquet' (FORMAT parquet);
-- Sales of the year joined to this year's roll
COPY (SELECT r.fips, r.fips || r.tr6 tract, r.zip, r.area, 2012 y, r.pic, r.hs, x.price, r.bill, r.av, r.mv, x.newcon, hash(r.clip) % 100 h
      FROM r JOIN sl x ON x.clip = r.clip AND x.y = 2012
      WHERE x.price > 10000 AND r.pic IN (10, 11, 21, 22)) TO '/home/jr2728/ptax_explore/kpanel/sales_2012.parquet' (FORMAT parquet);
-- Year 2013 of the jurisdiction/tract panel (SOM, read-only DB). Generated per year by k_panel.sbatch.
-- Roll: one row per clip (max bill / AV / MV over duplicate rows), all property types; residential = pic 10/11/21/22.
CREATE OR REPLACE TEMP TABLE r AS
SELECT clip, any_value(lpad(fips_code::VARCHAR, 5, '0')) fips, any_value(property_indicator_code) pic,
       any_value(left(census_id::VARCHAR, 6)) FILTER (WHERE length(census_id::VARCHAR) >= 6) tr6,
       any_value(coalesce(nullif(trim(tax_area_code), ''), nullif(trim(tax_rate_area_code), ''))) area,
       any_value(left(situs_zip_code, 5)) zip, max(total_tax_amount) bill, max(assessed_total_value) av, max(market_total_value) mv,
       max(total_property_tax_rate_percent) rate_pct, bool_or(coalesce(homestead_exempt_indicator, false)) hs
FROM cl.tax WHERE tax_year = 2013 GROUP BY clip;
COPY (SELECT fips, fips || tr6 tract, 2013 y, pic IN (10, 11, 21, 22) res, count(*) n, count(*) FILTER (WHERE bill > 0) n_bill,
             sum(bill) FILTER (WHERE bill > 0) bill, sum(av) FILTER (WHERE av > 0) av, sum(mv) FILTER (WHERE mv > 0) mv,
             median(bill / av) FILTER (WHERE bill > 0 AND av > 0) med_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r GROUP BY ALL) TO '/home/jr2728/ptax_explore/kpanel/roll_tract_2013.parquet' (FORMAT parquet);
COPY (SELECT fips, area, 2013 y, count(*) n, sum(bill) bill, sum(av) av, median(bill / av) med_nom, quantile_cont(bill / av, 0.25) q25_nom,
             quantile_cont(bill / av, 0.75) q75_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r WHERE pic IN (10, 11, 21, 22) AND bill > 0 AND av > 0 AND area IS NOT NULL GROUP BY ALL) TO '/home/jr2728/ptax_explore/kpanel/roll_area_2013.parquet' (FORMAT parquet);
-- Sales of the year joined to this year's roll
COPY (SELECT r.fips, r.fips || r.tr6 tract, r.zip, r.area, 2013 y, r.pic, r.hs, x.price, r.bill, r.av, r.mv, x.newcon, hash(r.clip) % 100 h
      FROM r JOIN sl x ON x.clip = r.clip AND x.y = 2013
      WHERE x.price > 10000 AND r.pic IN (10, 11, 21, 22)) TO '/home/jr2728/ptax_explore/kpanel/sales_2013.parquet' (FORMAT parquet);
-- Year 2014 of the jurisdiction/tract panel (SOM, read-only DB). Generated per year by k_panel.sbatch.
-- Roll: one row per clip (max bill / AV / MV over duplicate rows), all property types; residential = pic 10/11/21/22.
CREATE OR REPLACE TEMP TABLE r AS
SELECT clip, any_value(lpad(fips_code::VARCHAR, 5, '0')) fips, any_value(property_indicator_code) pic,
       any_value(left(census_id::VARCHAR, 6)) FILTER (WHERE length(census_id::VARCHAR) >= 6) tr6,
       any_value(coalesce(nullif(trim(tax_area_code), ''), nullif(trim(tax_rate_area_code), ''))) area,
       any_value(left(situs_zip_code, 5)) zip, max(total_tax_amount) bill, max(assessed_total_value) av, max(market_total_value) mv,
       max(total_property_tax_rate_percent) rate_pct, bool_or(coalesce(homestead_exempt_indicator, false)) hs
FROM cl.tax WHERE tax_year = 2014 GROUP BY clip;
COPY (SELECT fips, fips || tr6 tract, 2014 y, pic IN (10, 11, 21, 22) res, count(*) n, count(*) FILTER (WHERE bill > 0) n_bill,
             sum(bill) FILTER (WHERE bill > 0) bill, sum(av) FILTER (WHERE av > 0) av, sum(mv) FILTER (WHERE mv > 0) mv,
             median(bill / av) FILTER (WHERE bill > 0 AND av > 0) med_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r GROUP BY ALL) TO '/home/jr2728/ptax_explore/kpanel/roll_tract_2014.parquet' (FORMAT parquet);
COPY (SELECT fips, area, 2014 y, count(*) n, sum(bill) bill, sum(av) av, median(bill / av) med_nom, quantile_cont(bill / av, 0.25) q25_nom,
             quantile_cont(bill / av, 0.75) q75_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r WHERE pic IN (10, 11, 21, 22) AND bill > 0 AND av > 0 AND area IS NOT NULL GROUP BY ALL) TO '/home/jr2728/ptax_explore/kpanel/roll_area_2014.parquet' (FORMAT parquet);
-- Sales of the year joined to this year's roll
COPY (SELECT r.fips, r.fips || r.tr6 tract, r.zip, r.area, 2014 y, r.pic, r.hs, x.price, r.bill, r.av, r.mv, x.newcon, hash(r.clip) % 100 h
      FROM r JOIN sl x ON x.clip = r.clip AND x.y = 2014
      WHERE x.price > 10000 AND r.pic IN (10, 11, 21, 22)) TO '/home/jr2728/ptax_explore/kpanel/sales_2014.parquet' (FORMAT parquet);
-- Year 2015 of the jurisdiction/tract panel (SOM, read-only DB). Generated per year by k_panel.sbatch.
-- Roll: one row per clip (max bill / AV / MV over duplicate rows), all property types; residential = pic 10/11/21/22.
CREATE OR REPLACE TEMP TABLE r AS
SELECT clip, any_value(lpad(fips_code::VARCHAR, 5, '0')) fips, any_value(property_indicator_code) pic,
       any_value(left(census_id::VARCHAR, 6)) FILTER (WHERE length(census_id::VARCHAR) >= 6) tr6,
       any_value(coalesce(nullif(trim(tax_area_code), ''), nullif(trim(tax_rate_area_code), ''))) area,
       any_value(left(situs_zip_code, 5)) zip, max(total_tax_amount) bill, max(assessed_total_value) av, max(market_total_value) mv,
       max(total_property_tax_rate_percent) rate_pct, bool_or(coalesce(homestead_exempt_indicator, false)) hs
FROM cl.tax WHERE tax_year = 2015 GROUP BY clip;
COPY (SELECT fips, fips || tr6 tract, 2015 y, pic IN (10, 11, 21, 22) res, count(*) n, count(*) FILTER (WHERE bill > 0) n_bill,
             sum(bill) FILTER (WHERE bill > 0) bill, sum(av) FILTER (WHERE av > 0) av, sum(mv) FILTER (WHERE mv > 0) mv,
             median(bill / av) FILTER (WHERE bill > 0 AND av > 0) med_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r GROUP BY ALL) TO '/home/jr2728/ptax_explore/kpanel/roll_tract_2015.parquet' (FORMAT parquet);
COPY (SELECT fips, area, 2015 y, count(*) n, sum(bill) bill, sum(av) av, median(bill / av) med_nom, quantile_cont(bill / av, 0.25) q25_nom,
             quantile_cont(bill / av, 0.75) q75_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r WHERE pic IN (10, 11, 21, 22) AND bill > 0 AND av > 0 AND area IS NOT NULL GROUP BY ALL) TO '/home/jr2728/ptax_explore/kpanel/roll_area_2015.parquet' (FORMAT parquet);
-- Sales of the year joined to this year's roll
COPY (SELECT r.fips, r.fips || r.tr6 tract, r.zip, r.area, 2015 y, r.pic, r.hs, x.price, r.bill, r.av, r.mv, x.newcon, hash(r.clip) % 100 h
      FROM r JOIN sl x ON x.clip = r.clip AND x.y = 2015
      WHERE x.price > 10000 AND r.pic IN (10, 11, 21, 22)) TO '/home/jr2728/ptax_explore/kpanel/sales_2015.parquet' (FORMAT parquet);
-- Year 2016 of the jurisdiction/tract panel (SOM, read-only DB). Generated per year by k_panel.sbatch.
-- Roll: one row per clip (max bill / AV / MV over duplicate rows), all property types; residential = pic 10/11/21/22.
CREATE OR REPLACE TEMP TABLE r AS
SELECT clip, any_value(lpad(fips_code::VARCHAR, 5, '0')) fips, any_value(property_indicator_code) pic,
       any_value(left(census_id::VARCHAR, 6)) FILTER (WHERE length(census_id::VARCHAR) >= 6) tr6,
       any_value(coalesce(nullif(trim(tax_area_code), ''), nullif(trim(tax_rate_area_code), ''))) area,
       any_value(left(situs_zip_code, 5)) zip, max(total_tax_amount) bill, max(assessed_total_value) av, max(market_total_value) mv,
       max(total_property_tax_rate_percent) rate_pct, bool_or(coalesce(homestead_exempt_indicator, false)) hs
FROM cl.tax WHERE tax_year = 2016 GROUP BY clip;
COPY (SELECT fips, fips || tr6 tract, 2016 y, pic IN (10, 11, 21, 22) res, count(*) n, count(*) FILTER (WHERE bill > 0) n_bill,
             sum(bill) FILTER (WHERE bill > 0) bill, sum(av) FILTER (WHERE av > 0) av, sum(mv) FILTER (WHERE mv > 0) mv,
             median(bill / av) FILTER (WHERE bill > 0 AND av > 0) med_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r GROUP BY ALL) TO '/home/jr2728/ptax_explore/kpanel/roll_tract_2016.parquet' (FORMAT parquet);
COPY (SELECT fips, area, 2016 y, count(*) n, sum(bill) bill, sum(av) av, median(bill / av) med_nom, quantile_cont(bill / av, 0.25) q25_nom,
             quantile_cont(bill / av, 0.75) q75_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r WHERE pic IN (10, 11, 21, 22) AND bill > 0 AND av > 0 AND area IS NOT NULL GROUP BY ALL) TO '/home/jr2728/ptax_explore/kpanel/roll_area_2016.parquet' (FORMAT parquet);
-- Sales of the year joined to this year's roll
COPY (SELECT r.fips, r.fips || r.tr6 tract, r.zip, r.area, 2016 y, r.pic, r.hs, x.price, r.bill, r.av, r.mv, x.newcon, hash(r.clip) % 100 h
      FROM r JOIN sl x ON x.clip = r.clip AND x.y = 2016
      WHERE x.price > 10000 AND r.pic IN (10, 11, 21, 22)) TO '/home/jr2728/ptax_explore/kpanel/sales_2016.parquet' (FORMAT parquet);
-- Year 2017 of the jurisdiction/tract panel (SOM, read-only DB). Generated per year by k_panel.sbatch.
-- Roll: one row per clip (max bill / AV / MV over duplicate rows), all property types; residential = pic 10/11/21/22.
CREATE OR REPLACE TEMP TABLE r AS
SELECT clip, any_value(lpad(fips_code::VARCHAR, 5, '0')) fips, any_value(property_indicator_code) pic,
       any_value(left(census_id::VARCHAR, 6)) FILTER (WHERE length(census_id::VARCHAR) >= 6) tr6,
       any_value(coalesce(nullif(trim(tax_area_code), ''), nullif(trim(tax_rate_area_code), ''))) area,
       any_value(left(situs_zip_code, 5)) zip, max(total_tax_amount) bill, max(assessed_total_value) av, max(market_total_value) mv,
       max(total_property_tax_rate_percent) rate_pct, bool_or(coalesce(homestead_exempt_indicator, false)) hs
FROM cl.tax WHERE tax_year = 2017 GROUP BY clip;
COPY (SELECT fips, fips || tr6 tract, 2017 y, pic IN (10, 11, 21, 22) res, count(*) n, count(*) FILTER (WHERE bill > 0) n_bill,
             sum(bill) FILTER (WHERE bill > 0) bill, sum(av) FILTER (WHERE av > 0) av, sum(mv) FILTER (WHERE mv > 0) mv,
             median(bill / av) FILTER (WHERE bill > 0 AND av > 0) med_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r GROUP BY ALL) TO '/home/jr2728/ptax_explore/kpanel/roll_tract_2017.parquet' (FORMAT parquet);
COPY (SELECT fips, area, 2017 y, count(*) n, sum(bill) bill, sum(av) av, median(bill / av) med_nom, quantile_cont(bill / av, 0.25) q25_nom,
             quantile_cont(bill / av, 0.75) q75_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r WHERE pic IN (10, 11, 21, 22) AND bill > 0 AND av > 0 AND area IS NOT NULL GROUP BY ALL) TO '/home/jr2728/ptax_explore/kpanel/roll_area_2017.parquet' (FORMAT parquet);
-- Sales of the year joined to this year's roll
COPY (SELECT r.fips, r.fips || r.tr6 tract, r.zip, r.area, 2017 y, r.pic, r.hs, x.price, r.bill, r.av, r.mv, x.newcon, hash(r.clip) % 100 h
      FROM r JOIN sl x ON x.clip = r.clip AND x.y = 2017
      WHERE x.price > 10000 AND r.pic IN (10, 11, 21, 22)) TO '/home/jr2728/ptax_explore/kpanel/sales_2017.parquet' (FORMAT parquet);
-- Year 2018 of the jurisdiction/tract panel (SOM, read-only DB). Generated per year by k_panel.sbatch.
-- Roll: one row per clip (max bill / AV / MV over duplicate rows), all property types; residential = pic 10/11/21/22.
CREATE OR REPLACE TEMP TABLE r AS
SELECT clip, any_value(lpad(fips_code::VARCHAR, 5, '0')) fips, any_value(property_indicator_code) pic,
       any_value(left(census_id::VARCHAR, 6)) FILTER (WHERE length(census_id::VARCHAR) >= 6) tr6,
       any_value(coalesce(nullif(trim(tax_area_code), ''), nullif(trim(tax_rate_area_code), ''))) area,
       any_value(left(situs_zip_code, 5)) zip, max(total_tax_amount) bill, max(assessed_total_value) av, max(market_total_value) mv,
       max(total_property_tax_rate_percent) rate_pct, bool_or(coalesce(homestead_exempt_indicator, false)) hs
FROM cl.tax WHERE tax_year = 2018 GROUP BY clip;
COPY (SELECT fips, fips || tr6 tract, 2018 y, pic IN (10, 11, 21, 22) res, count(*) n, count(*) FILTER (WHERE bill > 0) n_bill,
             sum(bill) FILTER (WHERE bill > 0) bill, sum(av) FILTER (WHERE av > 0) av, sum(mv) FILTER (WHERE mv > 0) mv,
             median(bill / av) FILTER (WHERE bill > 0 AND av > 0) med_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r GROUP BY ALL) TO '/home/jr2728/ptax_explore/kpanel/roll_tract_2018.parquet' (FORMAT parquet);
COPY (SELECT fips, area, 2018 y, count(*) n, sum(bill) bill, sum(av) av, median(bill / av) med_nom, quantile_cont(bill / av, 0.25) q25_nom,
             quantile_cont(bill / av, 0.75) q75_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r WHERE pic IN (10, 11, 21, 22) AND bill > 0 AND av > 0 AND area IS NOT NULL GROUP BY ALL) TO '/home/jr2728/ptax_explore/kpanel/roll_area_2018.parquet' (FORMAT parquet);
-- Sales of the year joined to this year's roll
COPY (SELECT r.fips, r.fips || r.tr6 tract, r.zip, r.area, 2018 y, r.pic, r.hs, x.price, r.bill, r.av, r.mv, x.newcon, hash(r.clip) % 100 h
      FROM r JOIN sl x ON x.clip = r.clip AND x.y = 2018
      WHERE x.price > 10000 AND r.pic IN (10, 11, 21, 22)) TO '/home/jr2728/ptax_explore/kpanel/sales_2018.parquet' (FORMAT parquet);
