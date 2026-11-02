-- Year __Y__: residential roll parcels by county x tract with year built = __Y__, __Y__ - 1, >= __Y__ - 4 (new construction
-- on the roll), plus parcels absent from the __Y__ - 1 roll (roll entries). Generated per year by l_yearbuilt.sbatch (SOM, read-only DB).
CREATE OR REPLACE TEMP TABLE r AS
SELECT clip, any_value(lpad(fips_code::VARCHAR, 5, '0')) fips, any_value(left(census_id::VARCHAR, 6)) FILTER (WHERE length(census_id::VARCHAR) >= 6) tr6,
       max(year_built) yb
FROM cl.tax WHERE tax_year = __Y__ AND property_indicator_code IN (10, 11, 21, 22) GROUP BY clip;
CREATE OR REPLACE TEMP TABLE prev AS SELECT DISTINCT clip FROM cl.tax WHERE tax_year = __Y__ - 1 AND property_indicator_code IN (10, 11, 21, 22);
COPY (SELECT fips, fips || tr6 tract, __Y__ y, count(*) n, count(*) FILTER (WHERE yb IS NOT NULL) n_yb,
             count(*) FILTER (WHERE yb = __Y__) built_y, count(*) FILTER (WHERE yb = __Y__ - 1) built_ym1, count(*) FILTER (WHERE yb >= __Y__ - 4) built_5y,
             count(*) FILTER (WHERE clip NOT IN (SELECT clip FROM prev)) entered
      FROM r GROUP BY ALL) TO '__DIR__/roll_built___Y__.parquet' (FORMAT parquet);
