SET preserve_insertion_order=false;
COPY (
WITH da AS (SELECT nullif(upper(trim(unit_num)),'') u FROM cl.dataaxle_annual WHERE record_year=2019),
     tx AS (SELECT nullif(upper(trim(situs_unit_number)),'') u FROM cl.tax WHERE tax_year=2019),
     c AS (SELECT 'dataaxle_family_records' src, u, count(*) n FROM da GROUP BY u
           UNION ALL SELECT 'tax_parcel_records', u, count(*) FROM tx GROUP BY u)
SELECT src,
       CASE WHEN u IS NULL THEN '(NULL/blank)' WHEN u IN ('1','2','3','4','5','6','A','B','C','D','101') THEN u ELSE '(other non-null)' END unit,
       sum(n) n
FROM c GROUP BY ALL ORDER BY src, unit) TO '/home/jr2728/ptax_explore/hh_strat/out/units_2019_union.csv' (HEADER);
