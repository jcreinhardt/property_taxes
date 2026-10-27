-- Year __Y__ of the jurisdiction/tract panel (SOM, read-only DB). Generated per year by k_panel.sbatch.
-- Roll: one row per clip (max bill / AV / MV over duplicate rows), all property types; residential = pic 10/11/21/22.
CREATE OR REPLACE TEMP TABLE r AS
SELECT clip, any_value(lpad(fips_code::VARCHAR, 5, '0')) fips, any_value(property_indicator_code) pic,
       any_value(left(census_id::VARCHAR, 6)) FILTER (WHERE length(census_id::VARCHAR) >= 6) tr6,
       any_value(coalesce(nullif(trim(tax_area_code), ''), nullif(trim(tax_rate_area_code), ''))) area,
       any_value(left(situs_zip_code, 5)) zip, max(total_tax_amount) bill, max(assessed_total_value) av, max(market_total_value) mv,
       max(total_property_tax_rate_percent) rate_pct, bool_or(coalesce(homestead_exempt_indicator, false)) hs
FROM cl.tax WHERE tax_year = __Y__ GROUP BY clip;
COPY (SELECT fips, fips || tr6 tract, __Y__ y, pic IN (10, 11, 21, 22) res, count(*) n, count(*) FILTER (WHERE bill > 0) n_bill,
             sum(bill) FILTER (WHERE bill > 0) bill, sum(av) FILTER (WHERE av > 0) av, sum(mv) FILTER (WHERE mv > 0) mv,
             median(bill / av) FILTER (WHERE bill > 0 AND av > 0) med_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r GROUP BY ALL) TO '__DIR__/roll_tract___Y__.parquet' (FORMAT parquet);
COPY (SELECT fips, area, __Y__ y, count(*) n, sum(bill) bill, sum(av) av, median(bill / av) med_nom, quantile_cont(bill / av, 0.25) q25_nom,
             quantile_cont(bill / av, 0.75) q75_nom, median(rate_pct) med_rate_pct, avg(hs::INT) sh_hs
      FROM r WHERE pic IN (10, 11, 21, 22) AND bill > 0 AND av > 0 AND area IS NOT NULL GROUP BY ALL) TO '__DIR__/roll_area___Y__.parquet' (FORMAT parquet);
-- Sales of the year joined to this year's roll
COPY (SELECT r.fips, r.fips || r.tr6 tract, r.zip, r.area, __Y__ y, r.pic, r.hs, x.price, r.bill, r.av, r.mv, x.newcon, hash(r.clip) % 100 h
      FROM r JOIN sl x ON x.clip = r.clip AND x.y = __Y__
      WHERE x.price > 10000 AND r.pic IN (10, 11, 21, 22)) TO '__DIR__/sales___Y__.parquet' (FORMAT parquet);
