-- Extract relevant variables and insert into new database

-- Attach new database
attach '../../data/corelogic.db' as cl 
attach '/gpfs/gibbs/pi/lapoint/corelogic/database/corelogic.db' as cl_raw (readonly)

-- Tax --
create table tax as 
select clip, tax_year, any_value(fips_code), min(total_tax_amount), min(calculated_total_value)
from cl_raw.tax 
group by clip, tax_year;
