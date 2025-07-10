-- Extract relevant variables and insert into new database

-- Attach old database
attach '/gpfs/gibbs/pi/lapoint/corelogic/database/corelogic.db' as cl_raw (readonly);

-- Set up
select current_setting('threads') as threads;
select current_setting('memory_limit') as memory_limit;
set memory_limit = '800GB';
set temp_directory = '../../temp/';
.large_number_rendering footer

-- Add Tax 
create table tax as 
select 
    clip, 
    tax_year, 
    any_value(fips_code) as fips_code, 
    min(total_tax_amount) as total_tax_amount,
    min(calculated_total_value) as calculated_total_value
from cl_raw.tax 
where 
    tax_year between 2013 and 2024 and
    clip is not null and
    fips_code is not null and 
    total_tax_amount is not null and 
    calculated_total_value is not null
group by clip, tax_year;

-- Add Ownertransfer
create table ownertransfer as 
select 
    owner_transfer_composite_transaction_id, 
    clip, 
    fips_code, 
    sale_derived_date, 
    sale_amount,
    primary_category_code
from cl_raw.ownertransfer
where 
    year(sale_derived_date) between 2013 and 2023
    and clip is not null
    and fips_code is not null
    and sale_derived_date is not null
    and sale_amount is not null
order by fips_code, sale_derived_date;  

