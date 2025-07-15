-- Extract relevant variables and insert into new database

-- Attach old database
attach '/gpfs/gibbs/pi/lapoint/corelogic/database/corelogic.db' as cl_raw (readonly);

-- Set up
select current_setting('threads') as threads;
select current_setting('memory_limit') as memory_limit;
set temp_directory = '../../temp/';
.large_number_rendering footer

-- Add Tax 
create table tax as 
select 
    clip, 
    tax_year, 
    any_value(fips_code) as fips_code, 
    any_value(total_tax_amount) as total_tax_amount,
    any_value(calculated_total_value) as calculated_total_value,
    any_value(owner_1_full_name) as owner_1_full_name,
from cl_raw.tax 
where 
    tax_year between 2013 and 2024 and
    clip is not null and
    fips_code is not null and 
    total_tax_amount is not null and 
    calculated_total_value is not null
group by clip, tax_year
having 
    count(distinct fips_code) = 1 and 
    count(distinct total_tax_amount) = 1 and
    count(distinct calculated_total_value) = 1 and
    count(distinct owner_1_full_name) = 1
order by tax_year, fips_code, calculated_total_value;

select 
    (count(1) from cl_raw.tax) as n_raw,
    (count(distinct clip) from cl_raw.tax) as n_clips_raw,
    (count(1) from tax) as n,
    (count(distinct clip) from tax) as n_clips;

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

