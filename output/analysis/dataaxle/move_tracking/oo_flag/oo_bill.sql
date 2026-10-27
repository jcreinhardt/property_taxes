SET preserve_insertion_order=false; USE memory; SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Owner-occupancy inferred from the tax bill. SFR (pic 10) 2012-2021, 1% building sample.
-- Within each tax cell (county x tax area x year) the "full rate" is the 90th percentile of the effective rate; a parcel whose
-- effective rate is well below it is receiving an owner-occupant discount (homestead / principal-residence exemption, lower
-- assessment ratio or class rate). Compared with the CoreLogic owner_occupancy_code, the homestead flag and the mailing-address rule.
CREATE OR REPLACE TABLE b AS
  SELECT tax_year y, situs_state st, fips_code, coalesce(tax_area_code, tax_rate_area_code, '') tac, owner_occupancy_code occ,
         coalesce(homestead_exempt_indicator,false) hs, mail_here, corp,
         total_tax_amount / assessed_total_value r_av, total_tax_amount / calculated_total_value r_mv,
         coalesce(total_tax_exemption_amount,0) > 0 OR coalesce(calculated_total_tax_exemption_amount,0) > 0 exempt_amt
  FROM read_parquet('/home/jr2728/ptax_explore/samples/tax_bill_bld1pct.parquet')
  WHERE property_indicator_code = 10 AND coalesce(calculated_improvement_value,0) > 0
    AND total_tax_amount > 0 AND assessed_total_value > 0 AND calculated_total_value > 0
    AND total_tax_amount / calculated_total_value BETWEEN 0.0005 AND 0.1;
CREATE OR REPLACE TABLE c AS
  SELECT fips_code, tac, y, count(*) n, quantile_cont(r_av, 0.9) p90_av, quantile_cont(r_mv, 0.9) p90_mv, median(r_av) med_av, median(r_mv) med_mv
  FROM b GROUP BY ALL HAVING count(*) >= 20;
CREATE OR REPLACE TABLE r AS
  SELECT b.*, r_av / c.p90_av rel_av, r_mv / c.p90_mv rel_mv, c.n cell_n FROM b JOIN c USING (fips_code, tac, y);
-- 1. distribution of relative rates by state, 2019 (both measures)
COPY (SELECT st, meas, least(greatest(floor(rel * 20) / 20, 0), 1.2) bin, count(*) n
      FROM (SELECT st, 'av' meas, rel_av rel FROM r WHERE y = 2019 UNION ALL SELECT st, 'mv', rel_mv FROM r WHERE y = 2019)
      GROUP BY ALL ORDER BY ALL) TO '/home/jr2728/ptax_explore/hh_strat/out/oo_bill_hist_2019.csv' (HEADER);
-- 2. by state and year: discount share and agreement with code / flag / mailing rule, at thresholds 0.9 and 0.8 on both measures
CREATE OR REPLACE MACRO agree(disc, flag) AS struct_pack(
    disc := avg(disc::INT), flag := avg(flag::INT),
    p_disc_given_flag := avg(disc::INT) FILTER (WHERE flag), p_disc_given_noflag := avg(disc::INT) FILTER (WHERE NOT flag),
    p_flag_given_disc := avg(flag::INT) FILTER (WHERE disc), p_flag_given_nodisc := avg(flag::INT) FILTER (WHERE NOT disc));
COPY (SELECT st, y, count(*) n, avg(exempt_amt::INT) exempt_amt, avg(hs::INT) homestead, avg((occ='O')::INT) occ_o, avg(mail_here::INT) mail_here,
             agree(rel_av < 0.9, occ = 'O') av90_occ, agree(rel_av < 0.8, occ = 'O') av80_occ, agree(rel_mv < 0.9, occ = 'O') mv90_occ, agree(rel_mv < 0.8, occ = 'O') mv80_occ,
             agree(rel_av < 0.9, hs) av90_hs, agree(rel_mv < 0.9, hs) mv90_hs, agree(rel_av < 0.9, mail_here) av90_mail, agree(rel_mv < 0.9, mail_here) mv90_mail,
             agree(exempt_amt, occ = 'O') amt_occ, agree(exempt_amt, hs) amt_hs
      FROM r GROUP BY ALL HAVING count(*) >= 1000 ORDER BY st, y) TO '/home/jr2728/ptax_explore/hh_strat/out/oo_bill_by_state_year.csv' (HEADER);
-- 3. exemption amount fields: populated where?
COPY (SELECT situs_state st, tax_year y, count(*) n, avg((coalesce(total_tax_exemption_amount,0) > 0)::INT) tot_ex, avg((coalesce(calculated_total_tax_exemption_amount,0) > 0)::INT) calc_ex,
             avg((hs)::INT) hs
      FROM read_parquet('/home/jr2728/ptax_explore/samples/tax_bill_bld1pct.parquet') t
      WHERE property_indicator_code = 10 AND tax_year IN (2015, 2019) GROUP BY ALL HAVING count(*) >= 1000 ORDER BY st, y)
  TO '/home/jr2728/ptax_explore/hh_strat/out/oo_exempt_fields.csv' (HEADER);
