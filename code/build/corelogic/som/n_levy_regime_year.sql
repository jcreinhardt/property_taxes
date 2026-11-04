-- Year __Y__ of the levy-regime panel (SOM, read-only DB). Generated per year by n_levy_regime.sbatch.
-- Residential parcels (pic 10/11/21/22) with bill > 0 and AV > 0; tax area = tax_area_code / tax_rate_area_code, else situs city.
CREATE OR REPLACE TEMP TABLE cur AS
SELECT clip, any_value(lpad(fips_code::VARCHAR, 5, '0')) fips,
       any_value(coalesce(nullif(trim(tax_area_code), ''), nullif(trim(tax_rate_area_code), ''), 'city:' || upper(trim(situs_city)))) area,
       max(total_tax_amount) bill, max(assessed_total_value) av
FROM cl.tax WHERE tax_year = __Y__ __FILTER__ AND property_indicator_code IN (10, 11, 21, 22)
GROUP BY clip HAVING max(total_tax_amount) > 0 AND max(assessed_total_value) > 0;
COPY (
  WITH a AS (SELECT fips, area, count(*) n, sum(bill) bill, sum(av) av FROM cur WHERE area IS NOT NULL GROUP BY ALL),
  s AS (SELECT c.fips, c.area, count(*) n_st, sum(p.bill) bill0, sum(c.bill) bill1, sum(p.av) av0, sum(c.av) av1,
               median(ln(c.av / p.av)) med_dlav, median(ln(c.bill / p.bill)) med_dlbill,
               avg((abs(ln(c.av / p.av)) < 0.001)::INT) sh_av_same, avg((abs(ln((c.bill / c.av) / (p.bill / p.av))) < 0.0005)::INT) sh_rate_same
        FROM cur c JOIN prev p ON p.clip = c.clip AND p.fips = c.fips AND p.area = c.area
        WHERE c.area IS NOT NULL GROUP BY ALL)
  SELECT a.*, __Y__ y, s.* EXCLUDE (fips, area) FROM a LEFT JOIN s USING (fips, area)
) TO '__DIR__/levy_area___Y__.parquet' (FORMAT parquet);
DROP TABLE IF EXISTS prev; ALTER TABLE cur RENAME TO prev;
