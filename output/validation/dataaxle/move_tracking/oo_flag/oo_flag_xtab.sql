SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Owner-occupancy code vs homestead exemption flag vs mailing address = situs, single-family (pic 10), tax years 2012-2021.
CREATE OR REPLACE TABLE t AS
  SELECT tax_year, situs_state st, owner_occupancy_code occ, coalesce(homestead_exempt_indicator::VARCHAR,'') hs,
         coalesce(owner_1_corporate_indicator,false) corp,
         left(coalesce(mailing_zip_code,''),5) = left(situs_zip_code,5) AND upper(trim(coalesce(mailing_house_number,''))) = upper(trim(situs_house_number))
           AND upper(trim(coalesce(mailing_street_name,''))) = upper(trim(situs_street_name)) mail_here,
         coalesce(trim(mailing_zip_code),'') = '' mail_missing
  FROM read_parquet('/home/jr2728/ptax_explore/samples/tax_bld1pct.parquet')
  WHERE property_indicator_code = 10 AND tax_year BETWEEN 2012 AND 2021 AND coalesce(calculated_improvement_value,0) > 0;
-- 1. value frequencies
COPY (SELECT occ, hs, count(*) n FROM t GROUP BY ALL ORDER BY n DESC) TO '/home/jr2728/ptax_explore/hh_strat/out/oo_values.csv' (HEADER);
-- 2. occ vs mail_here, by year
COPY (SELECT tax_year, count(*) n, avg((occ='O')::INT) occ_o, avg(mail_here::INT) mail_here, avg(mail_missing::INT) mail_missing,
             avg((occ='O' AND mail_here)::INT) o_and_mail, avg((occ='O' AND NOT mail_here AND NOT mail_missing)::INT) o_but_mail_elsewhere,
             avg((occ='O' AND mail_missing)::INT) o_mail_missing, avg(((occ<>'O' OR occ IS NULL) AND mail_here)::INT) mail_here_not_o,
             avg((hs='Y')::INT) homestead
      FROM t GROUP BY 1 ORDER BY 1) TO '/home/jr2728/ptax_explore/hh_strat/out/oo_by_year.csv' (HEADER);
-- 3. by state, 2019: occ O share, homestead share, agreement
COPY (SELECT st, count(*) n, avg((occ='O')::INT) occ_o, avg(mail_here::INT) mail_here, avg((hs='Y')::INT) homestead,
             avg((occ='O' AND hs='Y')::INT) o_and_hs, avg((occ='O' AND hs<>'Y')::INT) o_not_hs, avg(((occ<>'O' OR occ IS NULL) AND hs='Y')::INT) hs_not_o,
             avg((hs='Y' AND mail_here)::INT) hs_and_mail, avg((hs='Y' AND NOT mail_here)::INT) hs_not_mail, avg((hs<>'Y' AND mail_here)::INT) mail_not_hs,
             avg(corp::INT) corp
      FROM t WHERE tax_year = 2019 GROUP BY 1 HAVING count(*) >= 2000 ORDER BY homestead DESC) TO '/home/jr2728/ptax_explore/hh_strat/out/oo_by_state_2019.csv' (HEADER);
