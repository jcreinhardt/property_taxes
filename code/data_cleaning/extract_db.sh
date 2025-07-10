#!/bin/bash
#
#
#SBATCH --account=lapoint # The account name for the job.
#SBATCH --job-name=owner # The job name.
#SBATCH --partition=bigmem
#SBATCH --time=2:00:00 # The time the job will take to run.
#SBATCH --nodes=1
#SBATCH --ntasks=1 
#SBATCH --cpus-per-task=20
#SBATCH --mem=1000G

# Run the extraction
#duckdb < extract_taxes.sql

# Do some manipulations too large for interactive jobs
duckdb ../../data/corelogic.db -s "with county_q99 as (
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
then false else true end;"

duckdb ../../data/corelogic.db -s "alter table ownertransfer add column windsorized boolean;
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
then false else true end;"