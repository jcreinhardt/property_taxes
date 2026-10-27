SET preserve_insertion_order=false;
USE memory;
SET temp_directory='/nfs/roberts/scratch/pi_csl53/jr2728/duckdb_tmp';
-- Mover rates on a gap-filled family panel. Each family_id is treated as present every year from its first to its last
-- observed year (2006-2023; the truncated 2022 file is ignored, so 2022 is always filled). Missing years carry the last
-- observed address forward: a gap followed by the same address = staying; a gap followed by another address = the family
-- stays at the old address until the year the new address is first observed (move dated to the year before that).
-- loose  = building (ZIP5 + street + house number; NULL if not a street address) or ZIP differs
-- strict = loose AND LOR at the new address <= years since last observed at the old address (1 without a gap)
CREATE OR REPLACE MACRO bk(t, z, s, h) AS CASE WHEN t = 'S' AND coalesce(trim(h),'') <> '' AND coalesce(trim(s),'') <> '' AND z IS NOT NULL
                                               THEN hash(lpad(z::VARCHAR,5,'0'), upper(trim(s)), upper(trim(h))) END;
CREATE OR REPLACE TABLE o AS
  SELECT family_id fid, record_year::SMALLINT y, primary_family_ind prim, bk(address_type, zip, street_name, house_num) b, zip z, state st,
         length_of_residence lor
  FROM cl.dataaxle_annual WHERE record_year <> 2022;
CREATE OR REPLACE TABLE o2 AS
  SELECT fid, y, prim, b, z, st,
         lead(y) OVER w ny, lead(b) OVER w nb, lead(z) OVER w nz, lead(st) OVER w nst, lead(lor) OVER w nlor
  FROM o WINDOW w AS (PARTITION BY fid ORDER BY y);
DROP TABLE o;
-- one row per observed record with a successor: covers transitions t = y .. ny-1; only the last one (t = ny-1) can be a move
CREATE OR REPLACE TABLE tr AS
  SELECT y, ny, prim, (ny - y) gap,
         (b IS DISTINCT FROM nb OR z IS DISTINCT FROM nz) mv, st IS DISTINCT FROM nst ist, coalesce(nlor <= ny - y, false) rs
  FROM o2 WHERE ny IS NOT NULL;
DROP TABLE o2;
COPY (
  WITH x AS (SELECT unnest(range(y, ny)) t, ny, prim, gap, mv, ist, rs FROM tr)
  SELECT t, t + 1 t1, prim, count(*) n,
         count(*) FILTER (WHERE t + 1 < ny) n_filled_next,
         count(*) FILTER (WHERE t + 1 = ny AND mv) loose,
         count(*) FILTER (WHERE t + 1 = ny AND mv AND rs) strict_moves,
         count(*) FILTER (WHERE t + 1 = ny AND ist) interstate_loose,
         count(*) FILTER (WHERE t + 1 = ny AND ist AND rs) interstate_strict,
         count(*) FILTER (WHERE t + 1 = ny AND mv AND rs AND gap > 1) strict_after_gap,
         count(*) FILTER (WHERE t + 1 = ny AND mv AND gap > 1) loose_after_gap
  FROM x GROUP BY ALL ORDER BY ALL
) TO '/home/jr2728/ptax_explore/hh_strat/out/moves_filled.csv' (HEADER);
-- gap lengths, for reference
COPY (SELECT gap, mv, rs, prim, count(*) n FROM tr WHERE gap > 1 GROUP BY ALL ORDER BY ALL)
  TO '/home/jr2728/ptax_explore/hh_strat/out/moves_filled_gaps.csv' (HEADER);
