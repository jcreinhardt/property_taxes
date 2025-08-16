-- Setup
select current_setting('threads') as threads;
select current_setting('memory_limit') as memory_limit;
set temp_directory = '../../temp/';

---------------------------------
--- Do a little data cleaning ---
---------------------------------

alter table tax add column sample boolean;
update tax set sample = false;
alter table ownertransfer add column sample boolean;
update ownertransfer set sample = false;

-- 1: Restrict to a balanced panel
with counts as (select clip, count(distinct tax_year) as n from tax group by clip)
update tax set sample = true
where clip in (select clip from counts where n = 12);

-- 2: Focus on arms length transactions
update ownertransfer set sample = true where primary_category_code = 'A';

-- 3: Windsorize sale amounts and assessed values
update ownertransfer set sample = false where sale_amount not between 25000 and 5000000;
update tax set sample = false where calculated_total_value not between 25000 and 5000000;

-- 4: Focus on properties that don't sell successively
with sale_years as (
    select 
        owner_transfer_composite_transaction_id as id,
        clip, 
        year(sale_derived_date) as sale_year,
        lead(year(sale_derived_date)) over (partition by clip order by sale_derived_date) as next_sale_year
    from ownertransfer
)
update ownertransfer set sample = false 
where owner_transfer_composite_transaction_id in (select id from sale_years where next_sale_year - sale_year <= 1);


-----------------------------------------------------------------------
--- Construct hypothetical assessed values based on tract-level HPI ---
-----------------------------------------------------------------------

-- 1: Assess format of census tract
select length(tract) as n, count(1) from ffha_hpi group by n;
select length(census_id) as n, count(1) from tax group by n;

-- 2: Unify corelogic tract formatting
select census_id[-1] as final_digit, count(1) from tax group by final_digit; -- There are indeed 17 blocks that were split lol
select fips_code, census_id from tax where census_id[-1] in ('A', 'B'); 
alter table tax add column tract ubigint;
update tax set tract = concat(fips_code, census_id[1:6]);
update tax set tract = NULL where tract < 100000;
update tax set tract = NULL where length(census_id) = 5;
select length(cast(tract as varchar)) as n, count(1) from tax group by n;

-- 3: Unify FFHA tract formatting
alter table ffha_hpi alter column tract type ubigint;

-- 4: Check for join share
with tax_tracts as (
    select distinct tract from tax where tract is not null and sample = true
),
ffha_tracts as (
    select distinct tract from ffha_hpi where tract is not null 
),
match_stats as (
    select 
        count(distinct t.tract) as n_tax_tracts,
        count(distinct f.tract) as n_ffha_tracts,
        count(distinct case when f.tract is not null then t.tract end) as n_matched_tracts
    from tax_tracts t
    full outer join ffha_tracts f on t.tract = f.tract
)
select 
    n_matched_tracts as matches,
    n_tax_tracts - n_matched_tracts as unmatched_tax_tracts,
    n_ffha_tracts - n_matched_tracts as unmatched_ffha_tracts
from match_stats;


------------------------------------------------------------------
--- Calculate current value as last sale price adjusted by HPI ---
-----------------------------------------------------------------

create or replace table property_panel as from (

    -- Only keep most recent transaction within a year
    with sale_years as (
        select 
            clip, 
            year(sale_derived_date) as sale_year,
            last(sale_amount order by sale_derived_date) as sale_amount
        from ownertransfer
        where sample = true
        group by clip, year(sale_derived_date)
    ),
    -- Add most recent sale price and year
    property_panel as (
        select
            t.clip,
            t.tax_year,
            any_value(t.tract) as tract,
            any_value(t.calculated_total_value) as assessed_val,
            last(s.sale_year order by s.sale_year) as sale_year,
            last(s.sale_amount order by s.sale_year) as sale_amount
        from tax t
        left join sale_years s on t.clip = s.clip and t.tax_year >= s.sale_year
        where t.sample = true and fips_code // 1000 > 40
        group by t.clip, t.tax_year
        order by t.clip, t.tax_year
    )
    -- Merge tract-level HPI
    select 
        p.*,
        h_current.hpi,
        h_sale.hpi as hpi_at_sale,
        round(p.sale_amount * (h_current.hpi / h_sale.hpi)) as pred_value
    from property_panel p
    left join ffha_hpi h_current on p.tract = h_current.tract and p.tax_year = h_current.year
    left join ffha_hpi h_sale on p.tract = h_sale.tract and p.sale_year = h_sale.year
    order by p.clip, p.tax_year
);


-----------------------------------------------------------------------
--- Compare regressivity of assessed values vs. hypothetical values ---
-----------------------------------------------------------------------

with 
sale_years as (
    select 
        clip, 
        year(sale_derived_date) as sale_year,
        last(sale_amount order by sale_derived_date) as sale_amount
    from ownertransfer
    where sample = true and fips_code // 1000 > 40
    group by clip, year(sale_derived_date)
),
isolated_sales as (
    select
        s1.clip,
        s1.sale_year,
        s1.sale_amount
    from sale_years s1
    left join sale_years s2 
        on s1.clip = s2.clip 
        and s2.sale_year = s1.sale_year - 1
    where s2.clip is null
),
quantiles as (
    select
        quantile(sale_amount, 0.1) as q10,
        quantile(sale_amount, 0.2) as q20,
        quantile(sale_amount, 0.3) as q30,
        quantile(sale_amount, 0.4) as q40,
        quantile(sale_amount, 0.5) as q50,
        quantile(sale_amount, 0.6) as q60,
        quantile(sale_amount, 0.7) as q70,
        quantile(sale_amount, 0.8) as q80,
        quantile(sale_amount, 0.9) as q90
    from isolated_sales 
),
tax_after_sale AS (
  SELECT 
    i.clip,
    i.sale_amount,
    case 
        when i.sale_amount <= q.q10 then 'q0'
        when i.sale_amount <= q.q20 then 'q10'
        when i.sale_amount <= q.q30 then 'q20'
        when i.sale_amount <= q.q40 then 'q30'
        when i.sale_amount <= q.q50 then 'q40'
        when i.sale_amount <= q.q60 then 'q50'
        when i.sale_amount <= q.q70 then 'q60'
        when i.sale_amount <= q.q80 then 'q70'
        when i.sale_amount <= q.q90 then 'q80'
        else 'q90'
    end as qtile,
    p.tax_year,
    (p.assessed_val / i.sale_amount) * 100 as assess_rate,
    (p.pred_value / i.sale_amount) * 100 as new_rate
  FROM isolated_sales i
  JOIN property_panel p ON i.clip = p.clip and p.tax_year = i.sale_year - 1
  cross join quantiles q
)
SELECT 
    qtile,
    COUNT(*) as count,
    round(mean(sale_amount)) as avg,
    ROUND(MEDIAN(assess_rate), 1) as median,
    ROUND(QUANTILE(assess_rate, .9), 1) as q90,
    ROUND(QUANTILE(assess_rate, .1), 1) as q10,
    ROUND(STDDEV(assess_rate), 1) as sigma,
    ROUND(MEDIAN(new_rate), 1) as new_median,
    ROUND(QUANTILE(new_rate, .9), 1) as new_q90,
    ROUND(QUANTILE(new_rate, .1), 1) as new_q10,
    ROUND(STDDEV(new_rate), 1) as new_sigma
FROM tax_after_sale
GROUP BY qtile
ORDER BY qtile;









