-- Sales with parcel IDs for repeat-sales tests (Bouchet via run_sql.sbatch; corelogic.db READ_ONLY as cl).
-- Residential parcels (pic 10/11/21/22) of the 2018 roll with their 2010 tract (fips || left(census_id, 6)); arm's-length
-- sales 2012-2024 (first sale per parcel-year, price > 10k, not interfamily; new-construction flag kept). Only parcels with
-- >= 2 such sales are kept. Output ~/ptax_explore/kpanel/repeat_sales.parquet (clip, tract, y, price, newcon).
CREATE TEMP TABLE r AS
SELECT clip, any_value(lpad(fips_code::VARCHAR, 5, '0') || left(census_id::VARCHAR, 6)) tract
FROM cl.tax WHERE tax_year = 2018 AND property_indicator_code IN (10, 11, 21, 22) AND length(census_id::VARCHAR) >= 6 GROUP BY clip;
CREATE TEMP TABLE sl AS
SELECT clip, year(sale_derived_date)::INT y, arg_min(sale_amount, sale_derived_date) price, bool_or(coalesce(new_construction_indicator, false)) newcon
FROM cl.ownertransfer
WHERE sale_derived_date BETWEEN DATE '2012-01-01' AND DATE '2024-12-31' AND primary_category_code = 'A'
  AND NOT coalesce(interfamily_related_indicator, false) AND sale_amount > 10000 GROUP BY ALL;
COPY (SELECT s.clip, r.tract, s.y, s.price, s.newcon FROM sl s JOIN r USING (clip)
      WHERE s.clip IN (SELECT clip FROM sl GROUP BY clip HAVING count(*) >= 2)) TO '/home/jr2728/ptax_explore/kpanel/repeat_sales.parquet' (FORMAT parquet);
SELECT count(*) sales, count(DISTINCT clip) parcels FROM '/home/jr2728/ptax_explore/kpanel/repeat_sales.parquet';
