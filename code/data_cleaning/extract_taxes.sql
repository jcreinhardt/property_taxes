-- Extract relevant variables and insert into new database

-- Attach old database
attach '/gpfs/gibbs/pi/lapoint/corelogic/database/corelogic.db' as cl_raw (readonly);

-- Set up
select current_setting('threads') as threads;
select current_setting('memory_limit') as memory_limit;
set memory_limit = '20GB';
set temp_directory = '../../temp/';
.large_number_rendering footer
.maxrow 60


-- Add Tax  (manually change years if rerunning code)
create table tax as 
select clip, tax_year, any_value(fips_code), min(total_tax_amount), min(calculated_total_value)
from cl_raw.tax 
where 
    tax_year = 2024 and 
    clip is not null and
    fips_code is not null and 
    total_tax_amount is not null and 
    calculated_total_value is not null
group by clip, tax_year;

insert into tax
select clip, tax_year, any_value(fips_code), min(total_tax_amount), min(calculated_total_value)
from cl_raw.tax 
where 
    tax_year = 2020 and 
    clip is not null and
    fips_code is not null and 
    total_tax_amount is not null and 
    calculated_total_value is not null
group by clip, tax_year;

-- Clean up
alter table tax rename 'any_value(fips_code)' to fips_code;
alter table tax rename 'min(total_tax_amount)' to total_tax_amount;
alter table tax rename 'min(calculated_total_value)' to calculated_total_value;

-- Add Ownertransfer
create table ownertransfer as 
select clip, fips_code, sale_derived_date, sale_amount
from cl_raw.ownertransfer
where 
    year(sale_derived_date) between 2013 and 2023
    and clip is not null
    and fips_code is not null
    and sale_derived_date is not null
    and sale_amount is not null
order by fips_code, sale_derived_date;

-- TAX: Check for consistency
select count(1) from tax;
select count(distinct clip) from tax;
select count(distinct fips_code) from tax;
select 
    count(clip) / count(1) as clip_share,
    count(fips_code) / count(1) as fips_code_share,
    count(tax_year) / count(1) as tax_year_share,
    count(total_tax_amount) / count(1) as total_tax_amount_share,
    count(calculated_total_value) / count(1) as calculated_total_value_share
from tax;
select 
    fips_code // 1000 as state,
    count(distinct clip) as n,
    round(mean(total_tax_amount), 1) as avg_taxes,
    round(mean(calculated_total_value), 1) as avg_value,
    round(stddev(total_tax_amount), 1) as sd_taxes,
    round(stddev(calculated_total_value), 1) as sd_value
from tax tablesample 10%
group by state
order by state; -- Some values are clearly nonsensical

alter table tax add column windsorized boolean;
with county_q99 as (
    select 
        fips_code,
        quantile(total_tax_amount, .99) as q99_tax,
        quantile(calculated_total_value, .99) as q99_value
    from tax
    group by fips_code 
)
update tax set windsorized = case when
    total_tax_amount < 0 or
    total_tax_amount > (select q99_tax from county_q99 where county_q99.fips_code = tax.fips_code)
then false else true end; -- windsorize at 99th percentile per county

-- OWNERTRANSFER: Check for consistency
select count(1) from ownertransfer;
select count(distinct clip) from ownertransfer;
select count(distinct fips_code) from ownertransfer;
select
    count(clip) / count(1) as clip_share,
    count(fips_code) / count(1) as fips_code_share,
    count(sale_derived_date) / count(1) as sale_derived_date_share,
    count(sale_amount) / count(1) as sale_amount_share
from ownertransfer; --NAs as expected

with counts as (
    select count(1) as n from ownertransfer group by clip, sale_derived_date
) select n, count(1) from counts group by n order by n; 

select 
    fips_code // 1000 as state,
    count(distinct clip) as n,
    round(mean(sale_amount), 1) as avg_sale_amount,
    round(stddev(sale_amount), 1) as sd_sale_amount
from ownertransfer tablesample 10%
group by state
order by state; -- Numbers sometimes make no sense

alter table ownertransfer add column windsorized boolean;
with county_q99 as (
    select 
        fips_code,
        quantile(sale_amount, .99) as q99_sale
    from ownertransfer
    group by fips_code 
)
update ownertransfer set windsorized = case when
    sale_amount < 0 or
    sale_amount > (select q99_sale from county_q99 where county_q99.fips_code = ownertransfer.fips_code)
then false else true end; -- windsorize at 99th percentile per county

with counts as (
    select count(1) as n 
    from tax
    group by clip
)
select n, count(1) from counts group by n order by n;

