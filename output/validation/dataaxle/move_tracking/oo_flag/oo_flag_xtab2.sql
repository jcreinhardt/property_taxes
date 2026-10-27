SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- What is the owner_occupancy_code made of? SFR (pic 10, with structure), 2012-2021.
CREATE OR REPLACE TABLE t AS
  SELECT tax_year, situs_state st, owner_occupancy_code occ, coalesce(homestead_exempt_indicator,false) hs, coalesce(owner_1_corporate_indicator,false) corp,
         left(coalesce(mailing_zip_code,''),5) = left(situs_zip_code,5) same_zip,
         upper(trim(coalesce(mailing_city,''))) = upper(trim(coalesce(situs_city,''))) same_city,
         upper(trim(coalesce(mailing_house_number,''))) = upper(trim(situs_house_number)) same_hn,
         upper(trim(coalesce(mailing_street_name,''))) = upper(trim(situs_street_name)) same_street,
         coalesce(mailing_state,'') = coalesce(situs_state,'') same_state,
         regexp_matches(upper(coalesce(mailing_street_address,'')), 'P\.? ?O\.? ?BOX|^BOX ') pobox
  FROM read_parquet('/home/jr2728/ptax_explore/samples/tax_bld1pct.parquet')
  WHERE property_indicator_code = 10 AND tax_year BETWEEN 2012 AND 2021 AND coalesce(calculated_improvement_value,0) > 0;
-- A. composition of code O vs others by mailing-address relation
COPY (SELECT occ, CASE WHEN same_zip AND same_hn AND same_street THEN '1_exact' WHEN same_hn AND same_street THEN '2_hn_street_other_zip'
                      WHEN same_zip AND same_hn THEN '3_zip_hn_other_street' WHEN pobox AND same_zip THEN '4_pobox_same_zip' WHEN pobox THEN '5_pobox_other_zip'
                      WHEN same_zip THEN '6_same_zip_other_addr' WHEN same_city THEN '7_same_city' WHEN same_state THEN '8_same_state' ELSE '9_other_state' END rel,
             count(*) n, avg(hs::INT) homestead, avg(corp::INT) corp
      FROM t GROUP BY ALL ORDER BY occ, rel) TO '/home/jr2728/ptax_explore/hh_strat/out/oo_composition.csv' (HEADER);
-- B. homestead flag by state (2019): populated where? agreement with code and mailing
COPY (SELECT st, count(*) n, avg(hs::INT) homestead, avg((occ='O')::INT) occ_o, avg((same_zip AND same_hn AND same_street)::INT) mail_here,
             avg((hs AND occ='O')::INT) hs_and_o, avg((hs AND occ<>'O')::INT) hs_not_o, avg((NOT hs AND occ='O')::INT) o_not_hs,
             avg((hs AND NOT (same_zip AND same_hn AND same_street))::INT) hs_mail_elsewhere
      FROM t WHERE tax_year = 2019 GROUP BY 1 HAVING count(*) >= 2000 ORDER BY homestead DESC) TO '/home/jr2728/ptax_explore/hh_strat/out/oo_homestead_by_state_2019.csv' (HEADER);
