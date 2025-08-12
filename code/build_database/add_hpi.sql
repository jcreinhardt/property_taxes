-- Set up
select current_setting('threads') as threads;
select current_setting('memory_limit') as memory_limit;
set temp_directory = '../../temp/';

-- Add HPI data 
create table ffha_hpi as from read_csv_auto('../../data/house_price_indices/ffha_hpi.csv');

-- First pass
describe ffha_hpi;
summarize ffha_hpi;

select count(1) from ffha_hpi;
select count(distinct tract) from ffha_hpi;
select state_abbr, count(1) as n from ffha_hpi group by state_abbr order by n desc;
select year, count(1) as n from ffha_hpi group by year order by year;

-- Looks quite clean
select year, count(1) as n, sum(case when annual_change is null then 1 else 0 end) as n_missing from ffha_hpi group by year order by year;