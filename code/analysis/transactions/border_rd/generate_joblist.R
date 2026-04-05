#!/usr/bin/env Rscript
# Generate the job list file for dSQ submission
# Run this script to create joblist.txt, then use dSQ to submit

# Total tasks: 109 borders * 10 years = 1090
n_tasks <- 1090

# Generate job lines
job_lines <- sapply(1:n_tasks, function(i) {
    paste0("module reset; module load R; Rscript border_rd_worker.R ", i)
})

# Write job file
writeLines(job_lines, "joblist.txt")

cat("Job file written: joblist.txt\n")
cat("Total tasks:", n_tasks, "\n")
cat("\nNext steps:\n")
cat("1. module load dSQ\n")
cat("2. dsq --job-file joblist.txt --mem-per-cpu 32g -c 4 -t 30:00 -p day\n")
cat("3. sbatch dsq-joblist-*.sh\n")
