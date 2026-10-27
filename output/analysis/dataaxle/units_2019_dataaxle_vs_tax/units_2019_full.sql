SET preserve_insertion_order=false;
COPY (
WITH da AS (SELECT nullif(upper(trim(unit_num)),'') u FROM cl.dataaxle_annual  WHERE record_year=2019),
     tx AS (SELECT nullif(upper(trim(situs_unit_number)),'') u FROM cl.tax  WHERE tax_year=2019),
     c AS (SELECT 'dataaxle_family_records' src, u, count(*) n FROM da GROUP BY u
           UNION ALL SELECT 'tax_parcel_records', u, count(*) FROM tx GROUP BY u),
     r AS (SELECT *, row_number() OVER (PARTITION BY src ORDER BY n DESC, u) rk FROM c WHERE u IS NOT NULL)
SELECT src, u AS unit, n, rk FROM r WHERE rk<=10
UNION ALL SELECT src, '(other non-null)', sum(n), 11 FROM r WHERE rk>10 GROUP BY src
UNION ALL SELECT src, '(NULL/blank)', n, 12 FROM c WHERE u IS NULL
UNION ALL SELECT src, '(distinct non-null values)', count(*), 13 FROM r GROUP BY src
ORDER BY src, rk) TO '/home/jr2728/ptax_explore/hh_strat/out/units_2019_full.csv' (HEADER);
