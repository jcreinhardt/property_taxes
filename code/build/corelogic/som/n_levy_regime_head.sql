-- Levy-regime panel 2012-2024 (SOM, read-only DB): per tax area x year, all residential parcels and the same parcels in t-1 and t.
SET temp_directory='/home/jr2728/ptax_explore/tmp'; SET preserve_insertion_order=false;
ATTACH '/gpfs/project/corelogic_database/corelogic_public.db' AS cl (READ_ONLY);
USE memory;
.timer on
CREATE TEMP TABLE prev AS SELECT ''::VARCHAR clip, ''::VARCHAR fips, ''::VARCHAR area, 0::DOUBLE bill, 0::DOUBLE av WHERE false;
