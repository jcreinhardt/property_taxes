SET preserve_insertion_order=false;
USE memory;
CREATE TABLE t AS
  SELECT fips_code fips, any_value(situs_state) OVER (PARTITION BY fips_code) st,
         coalesce(nullif(upper(trim(municipality_name)),''), nullif(upper(trim(municipality_code)),''), nullif(upper(trim(town_code)),''), nullif(upper(trim(jurisdiction_county_code)),'')) muni,
         taxroll_certification_date cert, last_assessor_update_date upd, taxroll_edition_number ed, situs_delivery_point_validation_code dpv
  FROM cl.tax WHERE tax_year=2021;
-- county modal dates
CREATE TABLE cm AS SELECT fips, mode(cert) cert_mode, mode(upd) upd_mode FROM t GROUP BY fips;
-- municipality-level: modal dates and within-muni purity
CREATE TABLE mm AS
  SELECT fips, muni, count(*) n, mode(cert) cert_m, mode(upd) upd_m
  FROM t WHERE muni IS NOT NULL GROUP BY fips, muni;
COPY (
  WITH mu AS (
    SELECT mm.fips, count(*) FILTER (WHERE mm.n >= 50) n_munis_50,
           count(*) FILTER (WHERE mm.n >= 50 AND mm.upd_m <> cm.upd_mode) n_munis_50_own_upd,
           count(*) FILTER (WHERE mm.n >= 50 AND mm.cert_m <> cm.cert_mode) n_munis_50_own_cert
    FROM mm JOIN cm USING (fips) GROUP BY mm.fips),
  base AS (
    SELECT t.fips, any_value(t.st) state, count(*) n_rows, count(DISTINCT t.muni) n_munis,
           count(DISTINCT t.cert) n_cert_dates, count(DISTINCT t.upd) n_update_dates, count(DISTINCT t.ed) n_editions,
           avg((t.cert = cm.cert_mode)::INT) share_rows_modal_cert, avg((t.upd = cm.upd_mode)::INT) share_rows_modal_upd,
           min(t.cert) cert_min, max(t.cert) cert_max, min(t.upd) upd_min, max(t.upd) upd_max,
           avg((t.dpv = 'Y')::INT) dpv_y, avg((t.dpv IS NULL)::INT) dpv_null
    FROM t JOIN cm USING (fips) GROUP BY t.fips)
  SELECT * FROM base LEFT JOIN mu USING (fips) ORDER BY fips
) TO '/home/jr2728/ptax_explore/hh_strat/out/juris_2021_county.csv' (HEADER);
