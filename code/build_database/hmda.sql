-- Create database with 2013-2024 HDMA series

-- Attach old database
.open '../../data/hmda.db'

-- Set up
select current_setting('threads') as threads;
select current_setting('memory_limit') as memory_limit;
set temp_directory = '../../temp/';

-- Load data
create table if not exists hmda_2024 as from '../../data/hmda/2024_public_lar_csv.csv';
create table if not exists hmda_2023 as from '../../data/hmda/2023_public_lar_csv.csv';
create table if not exists hmda_2022 
    as from read_csv(
        '../../data/hmda/2022_public_lar_csv.csv',
        types={'total_units': 'VARCHAR'}
    );
create table if not exists hmda_2021 as from '../../data/hmda/2021_public_lar.csv';
create table if not exists hmda_2020 
    as from read_csv(
        '../../data/hmda/2020_lar_csv.csv',
        types={'loan_term': 'VARCHAR', 'total_units': 'VARCHAR'}
    );
create table if not exists hmda_2019 as from '../../data/hmda/2019_public_lar_csv.csv';
create table if not exists hmda_2018 
    as from read_csv(
        '../../data/hmda/2018_public_lar_csv.csv',
        types={'loan_term': 'VARCHAR', 'total_units': 'VARCHAR'}
    );
create table if not exists hmda_2017 as from '../../data/hmda/hmda_2017_nationwide_all-records_labels.csv';
create table if not exists hmda_2016 as from '../../data/hmda/hmda_2016_nationwide_all-records_labels.csv';
create table if not exists hmda_2015 as from '../../data/hmda/hmda_2015_nationwide_all-records_labels.csv';
create table if not exists hmda_2014 as from '../../data/hmda/hmda_2014_nationwide_all-records_labels.csv';
create table if not exists hmda_2013 as from '../../data/hmda/hmda_2013_nationwide_all-records_labels.csv';

-- Combine into panel
create table if not exists hmda as (
    select 
        activity_year as year,
        derived_msa_md as msa,
        state_code as state,
        county_code as county,
        census_tract,
        derived_dwelling_category as property_type,
        derived_ethnicity as ethnicity,
        derived_race as race,
        derived_sex as sex,
        action_taken,
        purchaser_type,
        preapproval,
        loan_type,
        loan_purpose,
        lien_status,
        loan_amount,
        rate_spread,
        hoepa_status as hoepa,
        income,
        occupancy_type as occupancy,
        tract_population,
        tract_minority_population_percent,
        ffiec_msa_md_median_family_income,
        tract_to_msa_income_percentage,
        tract_owner_occupied_units,
        tract_one_to_four_family_homes
    from hmda_2024 
);

insert into hmda (
    select 
        activity_year as year,
        derived_msa_md as msa,
        state_code as state,
        county_code as county,
        census_tract,
        derived_dwelling_category as property_type,
        derived_ethnicity as ethnicity,
        derived_race as race,
        derived_sex as sex,
        action_taken,
        purchaser_type,
        preapproval,
        loan_type,
        loan_purpose,
        lien_status,
        loan_amount,
        rate_spread,
        hoepa_status as hoepa,
        income,
        occupancy_type as occupancy,
        tract_population,
        tract_minority_population_percent,
        ffiec_msa_md_median_family_income,
        tract_to_msa_income_percentage,
        tract_owner_occupied_units,
        tract_one_to_four_family_homes
    from hmda_2023
);

insert into hmda (
    select 
        activity_year as year,
        derived_msa_md as msa,
        state_code as state,
        county_code as county,
        census_tract,
        derived_dwelling_category as property_type,
        derived_ethnicity as ethnicity,
        derived_race as race,
        derived_sex as sex,
        action_taken,
        purchaser_type,
        preapproval,
        loan_type,
        loan_purpose,
        lien_status,
        loan_amount,
        rate_spread,
        hoepa_status as hoepa,
        income,
        occupancy_type as occupancy,
        tract_population,
        tract_minority_population_percent,
        ffiec_msa_md_median_family_income,
        tract_to_msa_income_percentage,
        tract_owner_occupied_units,
        tract_one_to_four_family_homes
    from hmda_2022
);  


insert into hmda (
    select 
        activity_year as year,
        derived_msa_md as msa,
        state_code as state,
        county_code as county,
        census_tract,
        derived_dwelling_category as property_type,
        derived_ethnicity as ethnicity,
        derived_race as race,
        derived_sex as sex,
        action_taken,
        purchaser_type,
        preapproval,
        loan_type,
        loan_purpose,
        lien_status,
        loan_amount,
        rate_spread,
        hoepa_status as hoepa,
        income,
        occupancy_type as occupancy,
        tract_population,
        tract_minority_population_percent,
        ffiec_msa_md_median_family_income,
        tract_to_msa_income_percentage,
        tract_owner_occupied_units,
        tract_one_to_four_family_homes
    from hmda_2021
);  


insert into hmda (
    select 
        activity_year as year,
        derived_msa_md as msa,
        state_code as state,
        county_code as county,
        census_tract,
        derived_dwelling_category as property_type,
        derived_ethnicity as ethnicity,
        derived_race as race,
        derived_sex as sex,
        action_taken,
        purchaser_type,
        preapproval,
        loan_type,
        loan_purpose,
        lien_status,
        loan_amount,
        rate_spread,
        hoepa_status as hoepa,
        income,
        occupancy_type as occupancy,
        tract_population,
        tract_minority_population_percent,
        ffiec_msa_md_median_family_income,
        tract_to_msa_income_percentage,
        tract_owner_occupied_units,
        tract_one_to_four_family_homes
    from hmda_2020
);  


insert into hmda (
    select 
        activity_year as year,
        derived_msa_md as msa,
        state_code as state,
        county_code as county,
        census_tract,
        derived_dwelling_category as property_type,
        derived_ethnicity as ethnicity,
        derived_race as race,
        derived_sex as sex,
        action_taken,
        purchaser_type,
        preapproval,
        loan_type,
        loan_purpose,
        lien_status,
        loan_amount,
        rate_spread,
        hoepa_status as hoepa,
        income,
        occupancy_type as occupancy,
        tract_population,
        tract_minority_population_percent,
        ffiec_msa_md_median_family_income,
        tract_to_msa_income_percentage,
        tract_owner_occupied_units,
        tract_one_to_four_family_homes
    from hmda_2019
);  


insert into hmda (
    select 
        activity_year as year,
        derived_msa_md as msa,
        state_code as state,
        county_code as county,
        census_tract,
        derived_dwelling_category as property_type,
        derived_ethnicity as ethnicity,
        derived_race as race,
        derived_sex as sex,
        action_taken,
        purchaser_type,
        preapproval,
        loan_type,
        loan_purpose,
        lien_status,
        loan_amount,
        rate_spread,
        hoepa_status as hoepa,
        income,
        occupancy_type as occupancy,
        tract_population,
        tract_minority_population_percent,
        ffiec_msa_md_median_family_income,
        tract_to_msa_income_percentage,
        tract_owner_occupied_units,
        tract_one_to_four_family_homes
    from hmda_2018
);  

insert into hmda (
    select 
        as_of_year as year,
        msamd as msa,
        state_code as state,
        county_code as county,
        census_tract_number as census_tract,
        property_type as property_type,
        applicant_ethnicity as ethnicity,
        applicant_race_1 as race,
        applicant_sex as sex,
        action_taken,
        purchaser_type,
        preapproval,
        loan_type,
        loan_purpose,
        lien_status,
        loan_amount_000s as loan_amount,
        rate_spread,
        hoepa_status as hoepa,
        applicant_income_000s as income,
        owner_occupancy as occupancy,
        population as tract_population,
        minority_population as tract_minority_population_percent,
        hud_median_family_income as ffiec_msa_md_median_family_income,
        tract_to_msamd_income as tract_to_msa_income_percentage,
        number_of_owner_occupied_units as tract_owner_occupied_units,
        number_of_1_to_4_family_units as tract_one_to_four_family_homes
    from hmda_2017
);

insert into hmda (
    select 
        as_of_year as year,
        msamd as msa,
        state_code as state,
        county_code as county,
        census_tract_number as census_tract,
        property_type as property_type,
        applicant_ethnicity as ethnicity,
        applicant_race_1 as race,
        applicant_sex as sex,
        action_taken,
        purchaser_type,
        preapproval,
        loan_type,
        loan_purpose,
        lien_status,
        loan_amount_000s as loan_amount,
        rate_spread,
        hoepa_status as hoepa,
        applicant_income_000s as income,
        owner_occupancy as occupancy,
        population as tract_population,
        minority_population as tract_minority_population_percent,
        hud_median_family_income as ffiec_msa_md_median_family_income,
        tract_to_msamd_income as tract_to_msa_income_percentage,
        number_of_owner_occupied_units as tract_owner_occupied_units,
        number_of_1_to_4_family_units as tract_one_to_four_family_homes
    from hmda_2016
);

insert into hmda (
    select 
        as_of_year as year,
        msamd as msa,
        state_code as state,
        county_code as county,
        census_tract_number as census_tract,
        property_type as property_type,
        applicant_ethnicity as ethnicity,
        applicant_race_1 as race,
        applicant_sex as sex,
        action_taken,
        purchaser_type,
        preapproval,
        loan_type,
        loan_purpose,
        lien_status,
        loan_amount_000s as loan_amount,
        rate_spread,
        hoepa_status as hoepa,
        applicant_income_000s as income,
        owner_occupancy as occupancy,
        population as tract_population,
        minority_population as tract_minority_population_percent,
        hud_median_family_income as ffiec_msa_md_median_family_income,
        tract_to_msamd_income as tract_to_msa_income_percentage,
        number_of_owner_occupied_units as tract_owner_occupied_units,
        number_of_1_to_4_family_units as tract_one_to_four_family_homes
    from hmda_2015
);

insert into hmda (
    select 
        as_of_year as year,
        msamd as msa,
        state_code as state,
        county_code as county,
        census_tract_number as census_tract,
        property_type as property_type,
        applicant_ethnicity as ethnicity,
        applicant_race_1 as race,
        applicant_sex as sex,
        action_taken,
        purchaser_type,
        preapproval,
        loan_type,
        loan_purpose,
        lien_status,
        loan_amount_000s as loan_amount,
        rate_spread,
        hoepa_status as hoepa,
        applicant_income_000s as income,
        owner_occupancy as occupancy,
        population as tract_population,
        minority_population as tract_minority_population_percent,
        hud_median_family_income as ffiec_msa_md_median_family_income,
        tract_to_msamd_income as tract_to_msa_income_percentage,
        number_of_owner_occupied_units as tract_owner_occupied_units,
        number_of_1_to_4_family_units as tract_one_to_four_family_homes
    from hmda_2014
);

insert into hmda (
    select 
        as_of_year as year,
        msamd as msa,
        state_code as state,
        county_code as county,
        census_tract_number as census_tract,
        property_type as property_type,
        applicant_ethnicity as ethnicity,
        applicant_race_1 as race,
        applicant_sex as sex,
        action_taken,
        purchaser_type,
        preapproval,
        loan_type,
        loan_purpose,
        lien_status,
        loan_amount_000s as loan_amount,
        rate_spread,
        hoepa_status as hoepa,
        applicant_income_000s as income,
        owner_occupancy as occupancy,
        population as tract_population,
        minority_population as tract_minority_population_percent,
        hud_median_family_income as ffiec_msa_md_median_family_income,
        tract_to_msamd_income as tract_to_msa_income_percentage,
        number_of_owner_occupied_units as tract_owner_occupied_units,
        number_of_1_to_4_family_units as tract_one_to_four_family_homes
    from hmda_2013
);

-- Check for consistent variable definition across vintages
select year, count(1) as n from hmda group by year order by year;

select 
    msa, year, count(1) as n 
from hmda 
group by msa, year having n < 1000
order by msa, year; -- 2017 contains typos

select state, count(1) as n
from hmda group by state order by state; -- reporting change
update hmda set state = CASE 
    WHEN state = 'AL' THEN '01'
    WHEN state = 'AK' THEN '02'
    WHEN state = 'AZ' THEN '04'
    WHEN state = 'AR' THEN '05'
    WHEN state = 'CA' THEN '06'
    WHEN state = 'CO' THEN '08'
    WHEN state = 'CT' THEN '09'
    WHEN state = 'DE' THEN '10'
    WHEN state = 'FL' THEN '12'
    WHEN state = 'GA' THEN '13'
    WHEN state = 'HI' THEN '15'
    WHEN state = 'ID' THEN '16'
    WHEN state = 'IL' THEN '17'
    WHEN state = 'IN' THEN '18'
    WHEN state = 'IA' THEN '19'
    WHEN state = 'KS' THEN '20'
    WHEN state = 'KY' THEN '21'
    WHEN state = 'LA' THEN '22'
    WHEN state = 'ME' THEN '23'
    WHEN state = 'MD' THEN '24'
    WHEN state = 'MA' THEN '25'
    WHEN state = 'MI' THEN '26'
    WHEN state = 'MN' THEN '27'
    WHEN state = 'MS' THEN '28'
    WHEN state = 'MO' THEN '29'
    WHEN state = 'MT' THEN '30'
    WHEN state = 'NE' THEN '31'
    WHEN state = 'NV' THEN '32'
    WHEN state = 'NH' THEN '33'
    WHEN state = 'NJ' THEN '34'
    WHEN state = 'NM' THEN '35'
    WHEN state = 'NY' THEN '36'
    WHEN state = 'NC' THEN '37'
    WHEN state = 'ND' THEN '38'
    WHEN state = 'OH' THEN '39'
    WHEN state = 'OK' THEN '40'
    WHEN state = 'OR' THEN '41'
    WHEN state = 'PA' THEN '42'
    WHEN state = 'RI' THEN '44'
    WHEN state = 'SC' THEN '45'
    WHEN state = 'SD' THEN '46'
    WHEN state = 'TN' THEN '47'
    WHEN state = 'TX' THEN '48'
    WHEN state = 'UT' THEN '49'
    WHEN state = 'VT' THEN '50'
    WHEN state = 'VA' THEN '51'
    WHEN state = 'WA' THEN '53'
    WHEN state = 'WV' THEN '54'
    WHEN state = 'WI' THEN '55'
    WHEN state = 'WY' THEN '56'
    WHEN state = 'DC' THEN '11'
    WHEN state = 'AS' THEN '60'
    WHEN state = 'FM' THEN '64'
    WHEN state = 'GU' THEN '66'
    WHEN state = 'MH' THEN '68'
    WHEN state = 'MP' THEN '69'
    WHEN state = 'PR' THEN '72'
    WHEN state = 'PW' THEN '70'
    WHEN state = 'VI' THEN '78'
    WHEN state = 'NA' THEN NULL
    ELSE state  
END;
alter table hmda alter column state type INT2;

select count(distinct county) as counties from hmda; -- roughly correct
update hmda set county = NULL where county = 'NA';
update hmda set county = NULL where county = 'na';
update hmda set county = NULL where county = 'N/AN/';
update hmda set county = cast(state as integer) * 1000 + cast(county as integer) where year < 2018;
alter table hmda alter column county type INT4;
update hmda set county = cast(state as int4) * 1000 + county where county < 1000;
select 
    state, 
    county // 1000 as imputed_state, 
    count(1) as n_total
from hmda group by state, imputed_state order by state, imputed_state;
select year, count(1) from hmda where state != county // 1000 group by year; -- still a few issues

select count(distinct census_tract) as tracts from hmda; -- too many
select year, count(distinct census_tract) as tracts from hmda group by year order by year; 
update hmda set census_tract = regexp_replace(census_tract, '\.', '');
update hmda set census_tract = NULL where census_tract = 'NA';
update hmda set census_tract = NULL where census_tract = 'na';
update hmda set census_tract = NULL where census_tract = 'Na';
update hmda set census_tract = NULL where census_tract = 'nA';
alter table hmda alter column census_tract type INT8;
update hmda set census_tract = cast(county as int8) * 1000000 + census_tract where length(cast(census_tract as varchar)) between 3 and 6;

select property_type, count(1) as n from hmda group by property_type order by property_type; -- change in 2017
select ethnicity, count(1) as n from hmda group by ethnicity order by ethnicity; -- change in 2017
select race, count(1) as n from hmda group by race order by race; -- change in 2017
select sex, count(1) as n from hmda group by sex order by sex; -- change in 2017
select action_taken, count(1) as n from hmda group by action_taken order by action_taken; -- consistent
select purchaser_type, count(1) as n from hmda group by purchaser_type order by purchaser_type; -- consistent
select preapproval, count(1) as n from hmda group by preapproval order by preapproval; -- consistent
select loan_type, count(1) as n from hmda group by loan_type order by loan_type; -- consistent
select loan_purpose, count(1) as n from hmda group by loan_purpose order by loan_purpose; -- consistent
select lien_status, count(1) as n from hmda group by lien_status order by lien_status; -- consistent
select hoepa, count(1) as n from hmda group by hoepa order by hoepa; -- consistent
select occupancy, count(1) as n from hmda group by occupancy order by occupancy; -- consistent
update hmda set income = NULL where income = 'NA'; 
alter table hmda alter column income type double; 
select year, median(income) as med, mean(income) as mean from hmda group by year order by year; -- consistent