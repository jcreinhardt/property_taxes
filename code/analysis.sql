-- Set up
select current_setting('threads') as threads;
select current_setting('memory_limit') as memory_limit;
set memory_limit = '20GB';
set temp_directory = '../../temp/';

-- Assessed values vs. transaction values
select count(1) from ownertransfer;
select 
    count(1) as n 
from ownertransfer o
join tax t on o.clip = t.clip and year(o.sale_derived_date) = t.tax_year;

with assessment_changes as (
    select 
        o.sale_amount,
        t0.calculated_total_value as pre_transaction_value,
        t1.calculated_total_value as current_transaction_value,
        t2.calculated_total_value as post_transaction_value
    from ownertransfer o tablesample 10%
    join tax t0 on o.clip = t0.clip and year(o.sale_derived_date) = t0.tax_year + 1
    join tax t1 on o.clip = t1.clip and year(o.sale_derived_date) = t1.tax_year + 0
    join tax t2 on o.clip = t2.clip and year(o.sale_derived_date) = t2.tax_year - 1
)
select
    mean(pre_transaction_value / sale_amount) as pre_ratio,
    mean(current_transaction_value / sale_amount) as current_ratio,
    mean(post_transaction_value / sale_amount) as post_ratio
from assessment_changes;

-- Reassessment frequency
with changes as(
    select 
        fips_code // 1000 as state,
        tax_year,
        calculated_total_value,
        lead(calculated_total_value) over (partition by clip order by tax_year) as next_value
    from tax
)
select 
        state,
        count(1) as n,
        count(case when calculated_total_value != next_value then 1 end) as n_reassessed,
from changes
where next_value is not null
group by state
order by state;
