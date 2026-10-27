SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Michigan: calculated_improvement_value is never populated, so no structure filter; principal-residence exemption = 18 school mills.
CREATE OR REPLACE TABLE b AS
  SELECT tax_year y, fips_code, owner_occupancy_code occ, coalesce(homestead_exempt_indicator,false) hs, mail_here,
         total_tax_amount / calculated_total_value r_mv
  FROM read_parquet('/home/jr2728/ptax_explore/samples/tax_bill_bld1pct.parquet')
  WHERE property_indicator_code = 10 AND situs_state = 'MI' AND total_tax_amount > 0 AND calculated_total_value > 0
    AND total_tax_amount / calculated_total_value BETWEEN 0.0005 AND 0.1;
CREATE OR REPLACE TABLE c AS SELECT fips_code, y, count(*) n, quantile_cont(r_mv, 0.9) p90 FROM b GROUP BY ALL HAVING count(*) >= 20;
CREATE OR REPLACE TABLE r AS SELECT b.*, r_mv / c.p90 rel FROM b JOIN c USING (fips_code, y);
SELECT least(floor(rel * 20) / 20, 1.2) bin, count(*) n FROM r WHERE y = 2019 GROUP BY 1 ORDER BY 1;
SELECT y, count(*) n, avg((rel < 0.8)::INT) disc, avg((occ='O')::INT) code_o, avg((rel<0.8)::INT) FILTER (WHERE occ='O') p_disc_o,
       avg((rel<0.8)::INT) FILTER (WHERE occ<>'O' OR occ IS NULL) p_disc_noto, avg((occ='O')::INT) FILTER (WHERE rel<0.8) p_o_disc,
       avg((occ='O')::INT) FILTER (WHERE rel>=0.8) p_o_nodisc, avg(hs::INT) hs
FROM r GROUP BY 1 ORDER BY 1;
