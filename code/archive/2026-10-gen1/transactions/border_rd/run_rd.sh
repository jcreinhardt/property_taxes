# 1. Navigate to the directory
pwd

# 2. Generate the job list file
module load R
Rscript generate_joblist.R

# 3. Load dSQ and create the batch script
module load dSQ
dsq --job-file joblist.txt --mem-per-cpu 32g -c 4 -t 30:00 

# 4. Submit the batch script
# sbatch dsq-joblist-2025-12-02.sh

# 5. After completion, combine results
Rscript combine_border_rd_results.R