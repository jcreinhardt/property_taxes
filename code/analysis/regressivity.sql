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
alter table ffha_hpi add column state utinyint;
update ffha_hpi set state = cast(tract as ubigint) // 1000000000;


-----------------------------------------------------------------------
--- Compare regressivity of assessed values vs. hypothetical values ---
-----------------------------------------------------------------------

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