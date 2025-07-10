
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

with counts as (
    select count(1) as n from tax group by clip 
) select n, count(1) from counts group by n order by n; 
with leads as (
    select 
        tax_year,
        lead(tax_year) over (partition by clip order by tax_year) as next_observed_year
    from tax
)
select count(1) from leads 
where tax_year + 1 != next_observed_year and 
next_observed_year is not null; -- Check for gaps in tax years

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
with duplicates as (
    select clip, sale_derived_date, count(1) as n
    from ownertransfer
    group by clip, sale_derived_date
    having count(1) > 1
)
update ownertransfer set windsorized = false
where 
    (clip, sale_derived_date) in (select clip, sale_derived_date from duplicates)
; -- Remove observations where dates do not pin down transactions

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

