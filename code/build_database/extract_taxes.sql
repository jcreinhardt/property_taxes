-- Extract relevant variables and insert into new database

-- Attach old database
attach '/gpfs/gibbs/pi/lapoint/corelogic/database/corelogic.db' as cl_raw (readonly);

-- Set up
select current_setting('threads') as threads;
select current_setting('memory_limit') as memory_limit;
set temp_directory = '../../temp/';
set memory_limit='3200GB';
set threads=30;
select current_setting('threads') as threads;
select current_setting('memory_limit') as memory_limit;


-- Add Tax 
create table if not exists tax as 
select 
    clip, 
    tax_year, 
    any_value(fips_code) as fips_code, 
    any_value(total_tax_amount) as total_tax_amount,
    any_value(calculated_total_value) as calculated_total_value,
    any_value(owner_1_full_name) as owner_1_full_name,
    any_value(census_id) as census_id
from cl_raw.tax 
where 
    tax_year = 2024 and
    clip is not null and
    fips_code is not null and 
    total_tax_amount is not null and 
    calculated_total_value is not null
group by clip, tax_year
having 
    count(distinct fips_code) = 1 and 
    count(distinct total_tax_amount) = 1 and
    count(distinct calculated_total_value) = 1 and
    count(distinct owner_1_full_name) = 1 and
    count(distinct census_id) = 1
order by tax_year, fips_code, calculated_total_value;

insert into tax 
select 
    clip, 
    tax_year, 
    any_value(fips_code) as fips_code, 
    any_value(total_tax_amount) as total_tax_amount,
    any_value(calculated_total_value) as calculated_total_value,
    any_value(owner_1_full_name) as owner_1_full_name,
    any_value(census_id) as census_id
from cl_raw.tax 
where 
    tax_year = 2023 and
    clip is not null and
    fips_code is not null and 
    total_tax_amount is not null and        
    calculated_total_value is not null 
group by clip, tax_year
having 
    count(distinct fips_code) = 1 and 
    count(distinct total_tax_amount) = 1 and
    count(distinct calculated_total_value) = 1 and
    count(distinct owner_1_full_name) = 1 and
    count(distinct census_id) = 1
order by tax_year, fips_code, calculated_total_value;

insert into tax 
select 
    clip, 
    tax_year, 
    any_value(fips_code) as fips_code, 
    any_value(total_tax_amount) as total_tax_amount,
    any_value(calculated_total_value) as calculated_total_value,
    any_value(owner_1_full_name) as owner_1_full_name,
    any_value(census_id) as census_id
from cl_raw.tax 
where 
    tax_year = 2022 and
    clip is not null and
    fips_code is not null and 
    total_tax_amount is not null and        
    calculated_total_value is not null 
group by clip, tax_year
having 
    count(distinct fips_code) = 1 and 
    count(distinct total_tax_amount) = 1 and
    count(distinct calculated_total_value) = 1 and
    count(distinct owner_1_full_name) = 1 and
    count(distinct census_id) = 1
order by tax_year, fips_code, calculated_total_value;

insert into tax 
select 
    clip, 
    tax_year, 
    any_value(fips_code) as fips_code, 
    any_value(total_tax_amount) as total_tax_amount,
    any_value(calculated_total_value) as calculated_total_value,
    any_value(owner_1_full_name) as owner_1_full_name,
    any_value(census_id) as census_id
from cl_raw.tax 
where 
    tax_year = 2021 and
    clip is not null and
    fips_code is not null and 
    total_tax_amount is not null and        
    calculated_total_value is not null 
group by clip, tax_year
having 
    count(distinct fips_code) = 1 and 
    count(distinct total_tax_amount) = 1 and
    count(distinct calculated_total_value) = 1 and
    count(distinct owner_1_full_name) = 1 and
    count(distinct census_id) = 1
order by tax_year, fips_code, calculated_total_value;

insert into tax 
select 
    clip, 
    tax_year, 
    any_value(fips_code) as fips_code, 
    any_value(total_tax_amount) as total_tax_amount,
    any_value(calculated_total_value) as calculated_total_value,
    any_value(owner_1_full_name) as owner_1_full_name,
    any_value(census_id) as census_id
from cl_raw.tax 
where 
    tax_year = 2020 and
    clip is not null and
    fips_code is not null and 
    total_tax_amount is not null and        
    calculated_total_value is not null 
group by clip, tax_year
having 
    count(distinct fips_code) = 1 and 
    count(distinct total_tax_amount) = 1 and
    count(distinct calculated_total_value) = 1 and
    count(distinct owner_1_full_name) = 1 and
    count(distinct census_id) = 1
order by tax_year, fips_code, calculated_total_value;

insert into tax 
select 
    clip, 
    tax_year, 
    any_value(fips_code) as fips_code, 
    any_value(total_tax_amount) as total_tax_amount,
    any_value(calculated_total_value) as calculated_total_value,
    any_value(owner_1_full_name) as owner_1_full_name,
    any_value(census_id) as census_id
from cl_raw.tax 
where 
    tax_year = 2019 and
    clip is not null and
    fips_code is not null and 
    total_tax_amount is not null and        
    calculated_total_value is not null 
group by clip, tax_year
having 
    count(distinct fips_code) = 1 and 
    count(distinct total_tax_amount) = 1 and
    count(distinct calculated_total_value) = 1 and
    count(distinct owner_1_full_name) = 1 and
    count(distinct census_id) = 1
order by tax_year, fips_code, calculated_total_value;

insert into tax 
select 
    clip, 
    tax_year, 
    any_value(fips_code) as fips_code, 
    any_value(total_tax_amount) as total_tax_amount,
    any_value(calculated_total_value) as calculated_total_value,
    any_value(owner_1_full_name) as owner_1_full_name,
    any_value(census_id) as census_id
from cl_raw.tax 
where 
    tax_year = 2018 and
    clip is not null and
    fips_code is not null and 
    total_tax_amount is not null and        
    calculated_total_value is not null 
group by clip, tax_year
having 
    count(distinct fips_code) = 1 and 
    count(distinct total_tax_amount) = 1 and
    count(distinct calculated_total_value) = 1 and
    count(distinct owner_1_full_name) = 1 and
    count(distinct census_id) = 1
order by tax_year, fips_code, calculated_total_value;

insert into tax 
select 
    clip, 
    tax_year, 
    any_value(fips_code) as fips_code, 
    any_value(total_tax_amount) as total_tax_amount,
    any_value(calculated_total_value) as calculated_total_value,
    any_value(owner_1_full_name) as owner_1_full_name,
    any_value(census_id) as census_id
from cl_raw.tax 
where 
    tax_year = 2017 and
    clip is not null and
    fips_code is not null and 
    total_tax_amount is not null and        
    calculated_total_value is not null 
group by clip, tax_year
having 
    count(distinct fips_code) = 1 and 
    count(distinct total_tax_amount) = 1 and
    count(distinct calculated_total_value) = 1 and
    count(distinct owner_1_full_name) = 1 and
    count(distinct census_id) = 1
order by tax_year, fips_code, calculated_total_value;

insert into tax 
select 
    clip, 
    tax_year, 
    any_value(fips_code) as fips_code, 
    any_value(total_tax_amount) as total_tax_amount,
    any_value(calculated_total_value) as calculated_total_value,
    any_value(owner_1_full_name) as owner_1_full_name,
    any_value(census_id) as census_id
from cl_raw.tax 
where 
    tax_year = 2016 and
    clip is not null and
    fips_code is not null and 
    total_tax_amount is not null and        
    calculated_total_value is not null 
group by clip, tax_year
having 
    count(distinct fips_code) = 1 and 
    count(distinct total_tax_amount) = 1 and
    count(distinct calculated_total_value) = 1 and
    count(distinct owner_1_full_name) = 1 and
    count(distinct census_id) = 1
order by tax_year, fips_code, calculated_total_value;

insert into tax 
select 
    clip, 
    tax_year, 
    any_value(fips_code) as fips_code, 
    any_value(total_tax_amount) as total_tax_amount,
    any_value(calculated_total_value) as calculated_total_value,
    any_value(owner_1_full_name) as owner_1_full_name,
    any_value(census_id) as census_id
from cl_raw.tax 
where 
    tax_year = 2015 and
    clip is not null and
    fips_code is not null and 
    total_tax_amount is not null and        
    calculated_total_value is not null 
group by clip, tax_year
having 
    count(distinct fips_code) = 1 and 
    count(distinct total_tax_amount) = 1 and
    count(distinct calculated_total_value) = 1 and
    count(distinct owner_1_full_name) = 1 and
    count(distinct census_id) = 1
order by tax_year, fips_code, calculated_total_value;

insert into tax 
select 
    clip, 
    tax_year, 
    any_value(fips_code) as fips_code, 
    any_value(total_tax_amount) as total_tax_amount,
    any_value(calculated_total_value) as calculated_total_value,
    any_value(owner_1_full_name) as owner_1_full_name,
    any_value(census_id) as census_id
from cl_raw.tax 
where 
    tax_year = 2014 and
    clip is not null and
    fips_code is not null and 
    total_tax_amount is not null and        
    calculated_total_value is not null 
group by clip, tax_year
having 
    count(distinct fips_code) = 1 and 
    count(distinct total_tax_amount) = 1 and
    count(distinct calculated_total_value) = 1 and
    count(distinct owner_1_full_name) = 1 and
    count(distinct census_id) = 1
order by tax_year, fips_code, calculated_total_value;

insert into tax 
select 
    clip, 
    tax_year, 
    any_value(fips_code) as fips_code, 
    any_value(total_tax_amount) as total_tax_amount,
    any_value(calculated_total_value) as calculated_total_value,
    any_value(owner_1_full_name) as owner_1_full_name,
    any_value(census_id) as census_id
from cl_raw.tax 
where 
    tax_year = 2013 and
    clip is not null and
    fips_code is not null and 
    total_tax_amount is not null and        
    calculated_total_value is not null 
group by clip, tax_year
having 
    count(distinct fips_code) = 1 and 
    count(distinct total_tax_amount) = 1 and
    count(distinct calculated_total_value) = 1 and
    count(distinct owner_1_full_name) = 1 and
    count(distinct census_id) = 1
order by tax_year, fips_code, calculated_total_value;


-- Add Ownertransfer
create table if not exists ownertransfer as 
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

