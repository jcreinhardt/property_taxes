SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Ratio-discount states (owner-occupants pay a structurally lower share of value): SC, WV, AL, MS, IN. Bill discount = effective rate
-- (tax / calculated value) below 80% of the county x tax area x year 90th percentile. Cross code O x discount with recent sales
-- (deeds sample) and mailing relation, SFR 2013-2020.
CREATE OR REPLACE TABLE b AS
  SELECT bkey, tax_year y, situs_state st, fips_code, coalesce(tax_area_code, tax_rate_area_code, '') tac, owner_occupancy_code occ,
         coalesce(homestead_exempt_indicator,false) hs, mail_here, corp, total_tax_amount / calculated_total_value r_mv, calculated_total_value mv
  FROM read_parquet('/home/jr2728/ptax_explore/samples/tax_bill_bld1pct.parquet')
  WHERE property_indicator_code = 10 AND coalesce(calculated_improvement_value,0) > 0 AND situs_state IN ('SC','WV','AL','MS','IN')
    AND total_tax_amount > 0 AND assessed_total_value > 0 AND calculated_total_value > 0 AND total_tax_amount / calculated_total_value BETWEEN 0.0005 AND 0.1;
CREATE OR REPLACE TABLE c AS SELECT fips_code, tac, y, count(*) n, quantile_cont(r_mv, 0.9) p90 FROM b GROUP BY ALL HAVING count(*) >= 20;
CREATE OR REPLACE TABLE r AS SELECT b.*, r_mv / c.p90 < 0.8 disc FROM b JOIN c USING (fips_code, tac, y) WHERE y BETWEEN 2013 AND 2020;
CREATE OR REPLACE TABLE sales AS
  SELECT bkey, year(sale_derived_date) sy FROM read_parquet('/home/jr2728/ptax_explore/samples/deeds_bld1pct.parquet')
  WHERE primary_category_code = 'A' AND NOT coalesce(interfamily_related_indicator,false) AND sale_amount > 0 AND sale_derived_date IS NOT NULL;
CREATE OR REPLACE TABLE rs AS
  SELECT r.*, EXISTS (SELECT 1 FROM sales s WHERE s.bkey = r.bkey AND s.sy IN (r.y - 1, r.y)) sold_recent,
              EXISTS (SELECT 1 FROM sales s WHERE s.bkey = r.bkey AND s.sy BETWEEN r.y - 5 AND r.y - 2) sold_2_5,
              EXISTS (SELECT 1 FROM sales s WHERE s.bkey = r.bkey) ever_sold
  FROM r;
-- 1. the four cells by state, with recent-sale shares and value
COPY (SELECT st, occ = 'O' code_o, disc, count(*) n, avg(sold_recent::INT) sold_recent, avg(sold_2_5::INT) sold_2_5, avg(ever_sold::INT) ever_sold,
             avg(hs::INT) hs, avg(mail_here::INT) mail_here, avg(corp::INT) corp, median(mv) med_value
      FROM rs GROUP BY ALL ORDER BY ALL) TO '/home/jr2728/ptax_explore/hh_strat/out/oo_refine_cells.csv' (HEADER);
-- 2. accuracy by state excluding recently sold (exemption not yet applied for), pooled 2013-2020
COPY (SELECT st, count(*) n, avg(disc::INT) disc, avg((occ='O')::INT) code_o,
             avg(disc::INT) FILTER (WHERE occ = 'O') p_disc_o, avg(disc::INT) FILTER (WHERE occ <> 'O' OR occ IS NULL) p_disc_noto,
             avg((occ='O')::INT) FILTER (WHERE disc) p_o_disc, avg((occ='O')::INT) FILTER (WHERE NOT disc) p_o_nodisc,
             avg((occ='O')::INT) FILTER (WHERE NOT disc AND NOT sold_recent) p_o_nodisc_nosale,
             avg((occ='O')::INT) FILTER (WHERE NOT disc AND NOT ever_sold) p_o_nodisc_neversold,
             avg(disc::INT) FILTER (WHERE occ = 'O' AND NOT sold_recent) p_disc_o_nosale,
             avg(mail_here::INT) FILTER (WHERE disc) p_mail_disc, avg(mail_here::INT) FILTER (WHERE NOT disc) p_mail_nodisc
      FROM rs GROUP BY 1 ORDER BY 1) TO '/home/jr2728/ptax_explore/hh_strat/out/oo_refine_acc.csv' (HEADER);
-- 3. persistence: is a parcel's discount status stable over years? (share of parcels whose disc flips between consecutive years)
COPY (SELECT a.st, avg((a.disc <> b.disc)::INT) disc_flip, avg(((a.occ='O') <> (b.occ='O'))::INT) code_flip, count(*) n
      FROM rs a JOIN rs b ON a.bkey = b.bkey AND b.y = a.y + 1 AND a.st = b.st GROUP BY 1 ORDER BY 1)
  TO '/home/jr2728/ptax_explore/hh_strat/out/oo_refine_flip.csv' (HEADER);
