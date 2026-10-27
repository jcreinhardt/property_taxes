SET preserve_insertion_order=false;
COPY (
  SELECT record_year, lpad(zip::VARCHAR,5,'0') zip, upper(trim(house_num)) h, upper(trim(street_name)) s, street_suffix sfx, address_type,
         count(*) fam_records, count(*) FILTER (WHERE primary_family_ind) primaries, count(DISTINCT unit_num) units_listed
  FROM cl.dataaxle_annual
  WHERE zip IN (10023, 10024, 10025, 10026, 10027) AND upper(street_name) LIKE '%AMST%'
    AND upper(trim(house_num)) IN ('640','654','700','702','734','774','794','842','920','933','934','936','941','944','955','961','975','992','995',
                                   '676','760','882','892','976','1020','1084','1129','681','701','751','968','1028','932','938')
  GROUP BY ALL ORDER BY h, record_year
) TO '/home/jr2728/ptax_explore/hh_strat/out/amst_years.csv' (HEADER);
