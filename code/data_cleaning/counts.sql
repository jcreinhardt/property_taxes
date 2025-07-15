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

with counts as (
    select count(1) as n from tax group by clip 
) select n, count(1) from counts group by n order by n; --mostly full coverage

with leads as (
    select 
        tax_year,
        lead(tax_year) over (partition by clip order by tax_year) as next_observed_year
    from tax
) 
select count(1) from leads 
where tax_year + 1 != next_observed_year and 
next_observed_year is not null; -- About 31 million gaps

with leads as (
    select 
        clip,
        fips_code // 1000 as state,
        tax_year,
        lead(tax_year) over (partition by clip order by tax_year) as next_observed_year
    from tax
), gap_properties as (
    select 
        any_value(state) as state,
        bool_or(tax_year + 1 != next_observed_year and next_observed_year is not null) as gap
    from leads
    group by clip
)
select 
    state, 
    round(100 * count(case when gap then 1 end) / count(1), 3) as share
from gap_properties
group by state
order by state; -- Some states feature quite a lot of gaps

with leads as (
    select 
        tax_year,
        lead(tax_year) over (partition by clip order by tax_year) as next_observed_year
    from tax
) 
select 
    next_observed_year, 
    count(1) as n,
from leads
where tax_year + 1 != next_observed_year and next_observed_year is not null
group by next_observed_year
order by next_observed_year; -- early on + 2020 is missing

with leads as (
    select 
        tax_year,
        lead(tax_year) over (partition by clip order by tax_year) as next_observed_year
    from tax
) 
select 
    tax_year, 
    count(1) as n,
from leads
where tax_year + 1 != next_observed_year and next_observed_year is not null
group by tax_year
order by tax_year;

with duplicates as (
    select 
        clip, 
        tax_year, 
        min(total_tax_amount) as min_tax,
        max(total_tax_amount) as max_tax,
        min(calculated_total_value) as min_value,
        max(calculated_total_value) as max_value
    from cl_raw.tax
    where tax_year = 2013
    group by clip, tax_year
    having count(1) > 1
)
select 
    count(case when min_tax != max_tax then 1 end) as n_different_taxes,
    count(case when min_value != max_value then 1 end) as n_different_values,
    count(1) as n_duplicates
from duplicates;

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

with counts as (
    select count(1) as n 
    from tax
    group by clip
)
select n, count(1) from counts group by n order by n;


