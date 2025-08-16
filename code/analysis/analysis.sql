-- Set up
select current_setting('threads') as threads;
select current_setting('memory_limit') as memory_limit;
set temp_directory = '../../temp/';




--------------------------------------------
-- Assessed values vs. transaction values --
--------------------------------------------

-- find properties with exactly one transaction in 4-year intervals
with transaction_years as (
  select 
    clip,
    year(sale_derived_date) as sale_year,
    sale_amount
  from ownertransfer
  where 
    primary_category_code = 'A' and
    windsorized = true and
    sale_year between 2015 and 2022
),

four_year_intervals as (
  select 
    clip,
    sale_year,
    sale_amount,
    sale_year - 2 as interval_start,
    sale_year + 1 as interval_end
  from transaction_years
),

-- count transactions per property in each 4-year interval
single_transaction_properties as (
  select 
    t1.clip,
    t1.sale_year,
    t1.sale_amount,
    t1.interval_start,
    t1.interval_end
  from four_year_intervals t1
  where (
    select count(*)
    from transaction_years t2
    where t2.clip = t1.clip
    and t2.sale_year between t1.interval_start and t1.interval_end
  ) = 1
),

-- get assessment ratios for all four years
assessment_ratios as (
  select 
    stp.clip,
    stp.sale_year,
    stp.sale_amount,
    t.tax_year,
    t.calculated_total_value / stp.sale_amount as assessment_to_sale_ratio
  from single_transaction_properties stp
  join tax t on stp.clip = t.clip
  where 
    t.tax_year between stp.interval_start and stp.interval_end and
    t.windsorized = true
),

-- aggregate ratios by sale year and relative position
year_aggregation as (
  select 
    sale_year,
    case 
        when tax_year = sale_year - 2 then 't_minus_2'
        when tax_year = sale_year - 1 then 't_minus_1'
        when tax_year = sale_year then 't0'
        when tax_year = sale_year + 1 then 't1'
    end as year_relative_to_sale,
    median(assessment_to_sale_ratio) as med_ratio
  from assessment_ratios
  group by sale_year, tax_year
)
select 
  sale_year,
  round(mean(case when year_relative_to_sale = 't_minus_2' then med_ratio end), 4) as med_t_minus_2,
  round(mean(case when year_relative_to_sale = 't_minus_1' then med_ratio end), 4) as med_t_minus_1,
  round(mean(case when year_relative_to_sale = 't0' then med_ratio end), 4) as med_t0,
  round(mean(case when year_relative_to_sale = 't1' then med_ratio end), 4) as med_t1,
from year_aggregation
group by sale_year
order by sale_year;


-- Repeat but collapse to the state level
with transaction_years as (
  select 
    clip,
    fips_code // 1000 as state,
    year(sale_derived_date) as sale_year,
    sale_amount
  from ownertransfer
  where 
    primary_category_code = 'A' and
    windsorized = true and
    sale_year between 2015 and 2022
),

four_year_intervals as (
  select 
    clip,
    state,
    sale_year,
    sale_amount,
    sale_year - 2 as interval_start,
    sale_year + 1 as interval_end
  from transaction_years
),

single_transaction_properties as (
  select 
    t1.clip,
    t1.state,
    t1.sale_year,
    t1.sale_amount,
    t1.interval_start,
    t1.interval_end
  from four_year_intervals t1
  where (
    select count(*)
    from transaction_years t2
    where t2.clip = t1.clip
    and t2.sale_year between t1.interval_start and t1.interval_end
  ) = 1
),

assessment_ratios as (
  select 
    stp.clip,
    stp.state,
    stp.sale_year,
    stp.sale_amount,
    t.tax_year,
    t.calculated_total_value / stp.sale_amount as assessment_to_sale_ratio
  from single_transaction_properties stp
  join tax t on stp.clip = t.clip
  where 
    t.tax_year between stp.interval_start and stp.interval_end and
    t.windsorized = true
),

state_aggregation as (
  select
    sale_year,
    state,
    case 
        when tax_year = sale_year - 2 then 't_minus_2'
        when tax_year = sale_year - 1 then 't_minus_1'
        when tax_year = sale_year then 't0'
        when tax_year = sale_year + 1 then 't1'
    end as year_relative_to_sale,
    median(assessment_to_sale_ratio) as med_ratio
  from assessment_ratios
  group by state, sale_year, tax_year
)
select 
  state,
  round(mean(case when year_relative_to_sale = 't_minus_2' then med_ratio end), 4) as med_t_minus_2,
  round(mean(case when year_relative_to_sale = 't_minus_1' then med_ratio end), 4) as med_t_minus_1,
  round(mean(case when year_relative_to_sale = 't0' then med_ratio end), 4) as med_t0,
  round(mean(case when year_relative_to_sale = 't1' then med_ratio end), 4) as med_t1,
from state_aggregation
group by state
order by state;








four_year_intervals as (
  select 
    clip,
    state,
    sale_year,
    sale_amount,
    sale_year - 2 as interval_start,
    sale_year + 1 as interval_end
  from transaction_years
),

-- count transactions per property in each 4-year interval
single_transaction_properties as (
  select 
    t1.clip,
    t1.state,
    t1.sale_year,
    t1.sale_amount,
    t1.interval_start,
    t1.interval_end
  from four_year_intervals t1
  where (
    select count(*)
    from transaction_years t2
    where t2.clip = t1.clip
    and t2.sale_year between t1.interval_start and t1.interval_end
  ) = 1
),

-- get assessment ratios for all four years
assessment_ratios as (
  select 
    stp.clip,
    stp.state,
    stp.sale_year,
    stp.sale_amount,
    t.tax_year,
    t.calculated_total_value / stp.sale_amount as assessment_to_sale_ratio
  from single_transaction_properties stp
  join tax t on stp.clip = t.clip
  where 
    t.tax_year between stp.interval_start and stp.interval_end and
    t.windsorized = true
),

-- aggregate ratios by sale year and relative position
year_aggregation as (
  select 
    state,
    case 
        when tax_year = sale_year - 2 then 't_minus_2'
        when tax_year = sale_year - 1 then 't_minus_1'
        when tax_year = sale_year then 't0'
        when tax_year = sale_year + 1 then 't1'
    end as year_relative_to_sale,
    median(assessment_to_sale_ratio) as med_ratio
  from assessment_ratios
  group by sale_year, tax_year
)

select 
  sale_year,
  round(median(case when year_relative_to_sale = 't_minus_2' then med_ratio end), 4) as med_t_minus_2,
  round(median(case when year_relative_to_sale = 't_minus_1' then med_ratio end), 4) as med_t_minus_1,
  round(median(case when year_relative_to_sale = 't0' then med_ratio end), 4) as med_t0,
  round(median(case when year_relative_to_sale = 't1' then med_ratio end), 4) as med_t1,
  count(distinct case when year_relative_to_sale = 'year_of_sale' then 1 end) as transactions
from year_aggregation
group by sale_year
order by sale_year;


with assessment_changes as (
    select 
        o.sale_amount,
        t0.calculated_total_value as pre_transaction_value,
        t1.calculated_total_value as current_transaction_value,
        t2.calculated_total_value as post_transaction_value
    from (
        select 
            clip,
            sale_derived_date,
            sale_amount,
            lead(year(sale_derived_date)) over (partition by clip order by sale_derived_date) as next_sale,
            lag(year(sale_derived_date)) over (partition by clip order by sale_derived_date) as prev_sale,
            windsorized,
            primary_category_code
        from ownertransfer 
        where 
            windsorized = true and
            primary_category_code = 'A' and
            year(sale_derived_date) between 2014 and 2022
    ) o
    join tax t0 on o.clip = t0.clip and year(o.sale_derived_date) = t0.tax_year + 1
    join tax t1 on o.clip = t1.clip and year(o.sale_derived_date) = t1.tax_year + 0
    join tax t2 on o.clip = t2.clip and year(o.sale_derived_date) = t2.tax_year - 1
    where 
        (year(o.sale_derived_date) != next_sale or next_sale is null) and
        (year(o.sale_derived_date) != prev_sale or prev_sale is null) and
        t0.windsorized = true and 
        t1.windsorized = true and 
        t2.windsorized = true
)
select
    count(1) as n,
    round(median(pre_transaction_value / sale_amount), 3) as pre_ratio,
    round(median(current_transaction_value / sale_amount), 3) as current_ratio,
    round(median(post_transaction_value / sale_amount), 3) as post_ratio,
from assessment_changes; --overall lots of reassessments

with assessment_changes as (
    select 
        o.sale_amount,
        t_2.calculated_total_value as minus_2,
        t_1.calculated_total_value as minus_1,
        t0.calculated_total_value as current,
        t1.calculated_total_value as plus_1,
        t2.calculated_total_value as plus_2
    from (
        select 
            clip,
            sale_derived_date,
            sale_amount,
            lead(year(sale_derived_date), 2) over (partition by clip order by sale_derived_date) as sale_plus_2,
            lead(year(sale_derived_date)) over (partition by clip order by sale_derived_date) as sale_plus_1,
            lag(year(sale_derived_date)) over (partition by clip order by sale_derived_date) as sale_minus_1,
            lag(year(sale_derived_date), 2) over (partition by clip order by sale_derived_date) as sale_minus_2,
            windsorized,
            primary_category_code
        from ownertransfer 
        where 
            windsorized = true and
            primary_category_code = 'A' and
            year(sale_derived_date) between 2015 and 2021
    ) o
    join tax t_2 on o.clip = t_2.clip and year(o.sale_derived_date) = t_2.tax_year + 2
    join tax t_1 on o.clip = t_1.clip and year(o.sale_derived_date) = t_1.tax_year + 1
    join tax t0 on o.clip = t0.clip and year(o.sale_derived_date) = t0.tax_year + 0
    join tax t1 on o.clip = t1.clip and year(o.sale_derived_date) = t1.tax_year - 1
    join tax t2 on o.clip = t2.clip and year(o.sale_derived_date) = t2.tax_year - 2
    where 
        (year(o.sale_derived_date) != sale_plus_2 or sale_plus_2 is null) and
        (year(o.sale_derived_date) != sale_plus_1 or sale_plus_1 is null) and
        (year(o.sale_derived_date) != sale_minus_1 or sale_minus_1 is null) and
        (year(o.sale_derived_date) != sale_minus_2 or sale_minus_2 is null) and
        t0.windsorized = true and
        t1.windsorized = true and
        t2.windsorized = true
)
select
    count(1) as n,
    round(median(pre_transaction_value / sale_amount), 3) as pre_ratio,
    round(median(current_transaction_value / sale_amount), 3) as current_ratio,
    round(median(post_transaction_value / sale_amount), 3) as post_ratio,
from assessment_changes; --overall lots of reassessments

with assessment_changes as (
    select 
        o.fips_code // 1000 as state,
        o.sale_amount,
        t0.calculated_total_value as pre_transaction_value,
        t1.calculated_total_value as current_transaction_value,
        t2.calculated_total_value as post_transaction_value
    from (
        select 
            clip,
            fips_code,
            sale_derived_date,
            sale_amount,
            lead(year(sale_derived_date)) over (partition by clip order by sale_derived_date) as next_sale,
            lag(year(sale_derived_date)) over (partition by clip order by sale_derived_date) as prev_sale,
            windsorized,
            primary_category_code
        from ownertransfer 
        where 
            windsorized = true and
            primary_category_code = 'A' and
            year(sale_derived_date) between 2014 and 2022
    ) o
    join tax t0 on o.clip = t0.clip and year(o.sale_derived_date) = t0.tax_year + 1
    join tax t1 on o.clip = t1.clip and year(o.sale_derived_date) = t1.tax_year + 0
    join tax t2 on o.clip = t2.clip and year(o.sale_derived_date) = t2.tax_year - 1
    where 
        (year(o.sale_derived_date) != next_sale or next_sale is null) and
        (year(o.sale_derived_date) != prev_sale or prev_sale is null) and
        t0.windsorized = true and 
        t1.windsorized = true and 
        t2.windsorized = true
)
select
    state,
    count(1) as n,
    round(median(pre_transaction_value / sale_amount), 3) as pre_ratio,
    round(median(current_transaction_value / sale_amount), 3) as current_ratio,
    round(median(post_transaction_value / sale_amount), 3) as post_ratio,
from assessment_changes
group by state
order by state; -- quite some heterogeneity; some reassessments look 'binding'

with assessment_changes as (
    select 
        year(o.sale_derived_date) as year,
        o.sale_amount,
        t0.calculated_total_value as pre_transaction_value,
        t2.calculated_total_value as post_transaction_value
    from (
        select 
            clip,
            sale_derived_date,
            sale_amount,
            lead(year(sale_derived_date)) over (partition by clip order by sale_derived_date) as next_sale,
            lag(year(sale_derived_date)) over (partition by clip order by sale_derived_date) as prev_sale,
            windsorized,
            primary_category_code
        from ownertransfer 
        where 
            windsorized = true and
            primary_category_code = 'A' and
            year(sale_derived_date) between 2014 and 2022
    ) o
    join tax t0 on o.clip = t0.clip and year(o.sale_derived_date) = t0.tax_year + 1
    join tax t2 on o.clip = t2.clip and year(o.sale_derived_date) = t2.tax_year - 1
    where 
        (year(o.sale_derived_date) != next_sale or next_sale is null) and
        (year(o.sale_derived_date) != prev_sale or prev_sale is null) and
        t0.windsorized = true and 
        t2.windsorized = true
)
select
    year,
    count(1) as n,
    round(median(post_transaction_value / sale_amount) - median(pre_transaction_value / sale_amount), 3) as change,
from assessment_changes
group by year
order by year; -- Increasing a lot over time!

.maxrows 500
with assessment_changes as (
    select 
        year(o.sale_derived_date) as year,
        o.fips_code // 1000 as state,
        o.sale_amount,
        t0.calculated_total_value as pre_transaction_value,
        t2.calculated_total_value as post_transaction_value
    from (
        select 
            clip,
            fips_code,
            sale_derived_date,
            sale_amount,
            lead(year(sale_derived_date)) over (partition by clip order by sale_derived_date) as next_sale,
            lag(year(sale_derived_date)) over (partition by clip order by sale_derived_date) as prev_sale,
            windsorized,
            primary_category_code
        from ownertransfer 
        where 
            windsorized = true and
            primary_category_code = 'A' and
            year(sale_derived_date) between 2014 and 2022
    ) o
    join tax t0 on o.clip = t0.clip and year(o.sale_derived_date) = t0.tax_year + 1
    join tax t2 on o.clip = t2.clip and year(o.sale_derived_date) = t2.tax_year - 1
    where 
        (year(o.sale_derived_date) != next_sale or next_sale is null) and
        (year(o.sale_derived_date) != prev_sale or prev_sale is null) and
        t0.windsorized = true and 
        t2.windsorized = true
),
aggregates as (
    select
        year,
        state,
        count(1) as n,
        round(median(post_transaction_value / sale_amount) - median(pre_transaction_value / sale_amount), 3) as change
    from assessment_changes
    where state < 57
    group by state, year
)
select distinct
    state,
    round(
        first(change order by year desc) - last(change order by year desc) 
        , 4) as delta
from aggregates
group by state
order by state; -- across most states

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




----------------------------
-- Regressivity by tenure --
----------------------------

with sales AS (
  SELECT 
    clip,
    sale_amount,
    sale_derived_date,
    year(sale_derived_date) - LAG(year(sale_derived_date)) OVER (PARTITION BY clip ORDER BY sale_derived_date) AS tenure
  FROM ownertransfer
  WHERE windsorized = true AND primary_category_code = 'A' AND month(sale_derived_date) != 12
),

tax_after_sale AS (
  SELECT 
    s.clip,
    s.sale_amount,
    s.sale_derived_date,
    s.tenure,
    t.tax_year,
    (t.total_tax_amount / s.sale_amount) * 100 as effective_tax_rate
  FROM sales s
  JOIN tax t ON s.clip = t.clip
  WHERE t.tax_year = year(s.sale_derived_date) - 1
    AND t.windsorized = true
)

SELECT 
  tenure,
  COUNT(*) as count,
  ROUND(MEDIAN(effective_tax_rate), 3) as median,
  ROUND(QUANTILE(effective_tax_rate, .9), 3) as q90,
  ROUND(QUANTILE(effective_tax_rate, .1), 3) as q10,
  ROUND(STDDEV(effective_tax_rate), 3) as sigma
FROM tax_after_sale
GROUP BY tenure
ORDER BY tenure;


----------------------------------
-- Taxes paid vs. capital gains --
----------------------------------

-- Aggregate US --

WITH sequential_sales AS (
  -- Get sequential sales for each property
  SELECT 
    clip,
    sale_amount,
    sale_derived_date,
    -- Get the next sale for the same property
    LEAD(sale_amount) OVER (PARTITION BY clip ORDER BY sale_derived_date) as next_sale_amount,
    LEAD(sale_derived_date) OVER (PARTITION BY clip ORDER BY sale_derived_date) as next_sale_date
  FROM ownertransfer
  WHERE windsorized = true and primary_category_code = 'A' 
),

sales_with_appreciation AS (
  -- Calculate price appreciation between sequential sales
  SELECT 
    clip,
    sale_derived_date as first_sale_date,
    next_sale_date as second_sale_date,
    -- Calculate price appreciation
    (next_sale_amount - sale_amount) as price_appreciation,
    -- Calculate years between sales
    year(next_sale_date) - year(sale_derived_date) as tenure
  FROM sequential_sales
  WHERE next_sale_amount IS NOT NULL -- Only properties with at least 2 sales
),

tax_payments_between_sales AS (
  -- Sum all tax payments between the two sales
  SELECT 
    swa.clip,
    swa.first_sale_date,
    swa.second_sale_date,
    swa.price_appreciation,
    swa.tenure,
    -- Sum all tax payments in the years between sales (exclusive of sale years)
    SUM(t.total_tax_amount) as total_tax_paid_between_sales
  FROM sales_with_appreciation swa
  JOIN tax t ON swa.clip = t.clip
  WHERE t.tax_year > year(swa.first_sale_date)
    AND t.tax_year <= year(swa.second_sale_date)
  GROUP BY 
    swa.clip,
    swa.first_sale_date,
    swa.second_sale_date,
    swa.price_appreciation,
    swa.tenure
),

final_calculations AS (
  -- Calculate the ratio of taxes to price appreciation
  SELECT 
    *,
    -- Ratio of total taxes paid to price appreciation
    (total_tax_paid_between_sales / price_appreciation) * 100 as tax_to_appreciation_ratio_pct
  FROM tax_payments_between_sales
)

SELECT 
  COUNT(*) as property_count,
  ROUND(MEDIAN(price_appreciation), 2) as med_appreciation,
  ROUND(MEDIAN(total_tax_paid_between_sales), 2) as med_tax,
  ROUND(MEDIAN(tax_to_appreciation_ratio_pct), 2) as med_gains_tax,
  ROUND(QUANTILE(tax_to_appreciation_ratio_pct, .90), 2) as q90_tax,
  ROUND(QUANTILE(tax_to_appreciation_ratio_pct, .1), 2) as q10_tax
FROM final_calculations;


-- Split by state --

WITH sequential_sales AS (
  -- Get sequential sales for each property
  SELECT 
    clip,
    fips_code // 1000 as state,
    sale_amount,
    sale_derived_date,
    -- Get the next sale for the same property
    LEAD(sale_amount) OVER (PARTITION BY clip ORDER BY sale_derived_date) as next_sale_amount,
    LEAD(sale_derived_date) OVER (PARTITION BY clip ORDER BY sale_derived_date) as next_sale_date
  FROM ownertransfer
  WHERE windsorized = true and primary_category_code = 'A' 
),

sales_with_appreciation AS (
  -- Calculate price appreciation between sequential sales
  SELECT 
    clip,
    state,
    sale_derived_date as first_sale_date,
    next_sale_date as second_sale_date,
    -- Calculate price appreciation
    (next_sale_amount - sale_amount) as price_appreciation,
    -- Calculate years between sales
    year(next_sale_date) - year(sale_derived_date) as tenure
  FROM sequential_sales
  WHERE next_sale_amount IS NOT NULL -- Only properties with at least 2 sales
),

tax_payments_between_sales AS (
  -- Sum all tax payments between the two sales
  SELECT 
    swa.clip,
    swa.state,
    swa.first_sale_date,
    swa.second_sale_date,
    swa.price_appreciation,
    swa.tenure,
    -- Sum all tax payments in the years between sales (exclusive of sale years)
    SUM(t.total_tax_amount) as total_tax_paid_between_sales
  FROM sales_with_appreciation swa
  JOIN tax t ON swa.clip = t.clip
  WHERE t.tax_year > year(swa.first_sale_date)
    AND t.tax_year <= year(swa.second_sale_date)
  GROUP BY 
    swa.clip,
    swa.state,
    swa.first_sale_date,
    swa.second_sale_date,
    swa.price_appreciation,
    swa.tenure
),

final_calculations AS (
  -- Calculate the ratio of taxes to price appreciation
  SELECT 
    *,
    -- Ratio of total taxes paid to price appreciation
    (total_tax_paid_between_sales / price_appreciation) * 100 as tax_to_appreciation_ratio_pct
  FROM tax_payments_between_sales
)

SELECT 
  state,
  COUNT(*) as property_count,
  ROUND(MEDIAN(price_appreciation), 2) as med_appreciation,
  ROUND(MEDIAN(total_tax_paid_between_sales), 2) as med_tax,
  ROUND(MEDIAN(tax_to_appreciation_ratio_pct), 2) as med_gains_tax,
  ROUND(QUANTILE(tax_to_appreciation_ratio_pct, .90), 2) as q90_tax,
  ROUND(QUANTILE(tax_to_appreciation_ratio_pct, .1), 2) as q10_tax
FROM final_calculations
GROUP BY state
ORDER BY state;

-- Split by transaction year --

WITH sequential_sales AS (
  -- Get sequential sales for each property
  SELECT 
    clip,
    sale_amount,
    sale_derived_date,
    -- Get the next sale for the same property
    LEAD(sale_amount) OVER (PARTITION BY clip ORDER BY sale_derived_date) as next_sale_amount,
    LEAD(sale_derived_date) OVER (PARTITION BY clip ORDER BY sale_derived_date) as next_sale_date
  FROM ownertransfer
  WHERE windsorized = true and primary_category_code = 'A' 
),

sales_with_appreciation AS (
  -- Calculate price appreciation between sequential sales
  SELECT 
    clip,
    sale_derived_date as first_sale_date,
    next_sale_date as second_sale_date,
    -- Calculate price appreciation
    (next_sale_amount - sale_amount) as price_appreciation,
    -- Calculate years between sales
    year(next_sale_date) - year(sale_derived_date) as tenure
  FROM sequential_sales
  WHERE next_sale_amount IS NOT NULL -- Only properties with at least 2 sales
),

tax_payments_between_sales AS (
  -- Sum all tax payments between the two sales
  SELECT 
    swa.clip,
    swa.first_sale_date,
    swa.second_sale_date,
    swa.price_appreciation,
    swa.tenure,
    -- Sum all tax payments in the years between sales (exclusive of sale years)
    SUM(t.total_tax_amount) as total_tax_paid_between_sales
  FROM sales_with_appreciation swa
  JOIN tax t ON swa.clip = t.clip
  WHERE t.tax_year > year(swa.first_sale_date)
    AND t.tax_year <= year(swa.second_sale_date)
  GROUP BY 
    swa.clip,
    swa.first_sale_date,
    swa.second_sale_date,
    swa.price_appreciation,
    swa.tenure
),

final_calculations AS (
  -- Calculate the ratio of taxes to price appreciation
  SELECT 
    *,
    -- Ratio of total taxes paid to price appreciation
    (total_tax_paid_between_sales / price_appreciation) * 100 as tax_to_appreciation_ratio_pct
  FROM tax_payments_between_sales
)

SELECT 
  year(second_sale_date) as year,
  COUNT(*) as property_count,
  ROUND(MEDIAN(price_appreciation), 2) as med_appreciation,
  ROUND(MEDIAN(total_tax_paid_between_sales), 2) as med_tax,
  ROUND(MEDIAN(tax_to_appreciation_ratio_pct), 2) as med_gains_tax,
  ROUND(QUANTILE(tax_to_appreciation_ratio_pct, .90), 2) as q90_tax,
  ROUND(QUANTILE(tax_to_appreciation_ratio_pct, .1), 2) as q10_tax
FROM final_calculations
GROUP BY year
ORDER BY year;



-------------------------------
-- Regressivity by valuation --
-------------------------------

with quantiles as (
  select 
    quantile(sale_amount, .1) as q10,
    quantile(sale_amount, .2) as q20,
    quantile(sale_amount, .3) as q30,
    quantile(sale_amount, .4) as q40,
    quantile(sale_amount, .5) as q50,
    quantile(sale_amount, .6) as q60,
    quantile(sale_amount, .7) as q70,
    quantile(sale_amount, .8) as q80,
    quantile(sale_amount, .9) as q90
  from ownertransfer
  where 
    primary_category_code = 'A' and
    windsorized = true and 
    sale_amount > 25000
), 
  tax_after_sale AS (
  SELECT 
    o.clip,
    o.sale_amount,
    o.sale_derived_date,
    case when 
      o.sale_amount <= (select q.q10 from quantiles q) then 'q0'
      when o.sale_amount <= (select q.q20 from quantiles q) then 'q10'
      when o.sale_amount <= (select q.q30 from quantiles q) then 'q20'
      when o.sale_amount <= (select q.q40 from quantiles q) then 'q30'
      when o.sale_amount <= (select q.q50 from quantiles q) then 'q40'
      when o.sale_amount <= (select q.q60 from quantiles q) then 'q50'
      when o.sale_amount <= (select q.q70 from quantiles q) then 'q60'
      when o.sale_amount <= (select q.q80 from quantiles q) then 'q70'
      when o.sale_amount <= (select q.q90 from quantiles q) then 'q80'
      else 'q90'
    end as qtile,
    t.tax_year,
    (t.total_tax_amount / o.sale_amount) * 100 as effective_tax_rate
  FROM ownertransfer o
  JOIN tax t ON o.clip = t.clip
  WHERE t.tax_year = year(o.sale_derived_date) - 1
    AND t.windsorized = true
    AND o.primary_category_code = 'A'
    AND o.windsorized = true
    AND o.sale_amount > 25000
)

SELECT 
  qtile,
  COUNT(*) as count,
  ROUND(MEDIAN(effective_tax_rate), 3) as median,
  ROUND(QUANTILE(effective_tax_rate, .9), 3) as q90,
  ROUND(QUANTILE(effective_tax_rate, .1), 3) as q10,
  ROUND(STDDEV(effective_tax_rate), 3) as sigma
FROM tax_after_sale
GROUP BY qtile
ORDER BY qtile;




--------------------------------------------------------------------
--- Did high tax homes get transacted more intensely after 2017? ---
--------------------------------------------------------------------

with sale_years as (
  select clip, year(sale_derived_date) as sale_year, sale_amount
  from ownertransfer 
), 
tax_quantiles as (
  select 
    quantile(total_tax_amount, .1) as q10,
    quantile(total_tax_amount, .2) as q20,
    quantile(total_tax_amount, .3) as q30,
    quantile(total_tax_amount, .4) as q40,
    quantile(total_tax_amount, .5) as q50,
    quantile(total_tax_amount, .6) as q60,
    quantile(total_tax_amount, .7) as q70,
    quantile(total_tax_amount, .8) as q80,
    quantile(total_tax_amount, .9) as q90,
    quantile(total_tax_amount, .95) as q95,
    quantile(total_tax_amount, .99) as q99,
    quantile(total_tax_amount, .995) as q995
  from tax
)
select 
  case when t.total_tax_amount <= (select q.q10 from tax_quantiles q) then 'q0'
       when t.total_tax_amount <= (select q.q20 from tax_quantiles q) then 'q10'
       when t.total_tax_amount <= (select q.q30 from tax_quantiles q) then 'q20'
       when t.total_tax_amount <= (select q.q40 from tax_quantiles q) then 'q30'
       when t.total_tax_amount <= (select q.q50 from tax_quantiles q) then 'q40'
       when t.total_tax_amount <= (select q.q60 from tax_quantiles q) then 'q50'
       when t.total_tax_amount <= (select q.q70 from tax_quantiles q) then 'q60'
       when t.total_tax_amount <= (select q.q80 from tax_quantiles q) then 'q70'
       when t.total_tax_amount <= (select q.q90 from tax_quantiles q) then 'q80'
       when t.total_tax_amount <= (select q.q95 from tax_quantiles q) then 'q90'
       when t.total_tax_amount <= (select q.q99 from tax_quantiles q) then 'q95'
       when t.total_tax_amount <= (select q.q995 from tax_quantiles q) then 'q99'
       else 'q100' end as tax_quantile,
  sum(case when s.sale_year is not null then 1 end) / count(1) as share_sold
from tax t
left join sale_years s on t.clip = s.clip and s.sale_year = t.tax_year
where tax_year = 2016 and t.fips_code // 1000 = 6 -- California
group by tax_quantile
order by tax_quantile;