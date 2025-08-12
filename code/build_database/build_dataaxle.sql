-- Extract relevant variables and insert into new database

-- Attach old database
attach '/gpfs/gibbs/pi/lapoint/corelogic/database/corelogic.db' as cl_raw (readonly);

-- Set up
select current_setting('threads') as threads;
select current_setting('memory_limit') as memory_limit;
set temp_directory = '../../temp/';

-- Extract individual keys
create table dataaxle as
select distinct id from (
    select individual_id_1 as id from cl_raw.dataaxle_annual where id is not null
    union all
    select individual_id_2 as id from cl_raw.dataaxle_annual where id is not null    
    union all
    select individual_id_3 as id from cl_raw.dataaxle_annual where id is not null
    union all
    select individual_id_4 as id from cl_raw.dataaxle_annual where id is not null
    union all
    select individual_id_5 as id from cl_raw.dataaxle_annual where id is not null
);