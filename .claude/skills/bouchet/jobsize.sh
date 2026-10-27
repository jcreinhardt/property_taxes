#!/bin/bash
# Usage: sbatch $(~/ptax_explore/jobsize.sh HH:MM:SS) --job-name=NAME --time=HH:MM:SS run_sql.sbatch file.sql
# Prints "--cpus-per-task=N --mem=XG" for the largest day-partition job that can start now (checked with
# sbatch --test-only), capped at 16 cores / 320G (override: MAXC=40 MAXG=640 env). Falls back to 4 cores/64G (queued).
# Light: only sinfo + sbatch --test-only, safe on the login node.
T=${1:-02:00:00}; P=day; MAXC=${MAXC:-16}; MAXG=${MAXG:-320}; BASEC=4; BASEG=64
# best node: maximise min(idle_cpus/4, free_mem/64G) over idle/mixed nodes
read -r C G < <(sinfo -h -p $P -N -O "CPUsState:20,Memory:12,AllocMem:12,StateLong:14" | sort -u |
  awk -v mc=$MAXC -v mg=$MAXG '$4 ~ /^(idle|mixed)/ { split($1,a,"/"); c=a[2]; g=int(($2-$3)/1024);
        if (c>mc) c=mc; if (g>mg) g=mg; k=(c/4 < g/64) ? c/4 : g/64; if (k>best) {best=k; bc=c; bg=g} }
       END { print bc+0, bg+0 }')
# Estimated start (epoch s) from sbatch --test-only. Its absolute value is padded by the scheduler, so a size
# counts as "no wait" if it is estimated to start no later than the 4-core/64G baseline (+2 min).
est() { local t; t=$(sbatch --test-only -p $P -c "$1" --mem="$2G" --time="$T" --wrap=true 2>&1 | grep -o 'to start at [0-9T:-]*' | awk '{print $4}')
  [ -n "$t" ] && date -d "$t" +%s || echo -1; }
BASE=$(est $BASEC $BASEG); [ "$BASE" -gt 0 ] || { echo "--cpus-per-task=$BASEC --mem=${BASEG}G"; exit 0; }
while [ "${C:-0}" -ge $BASEC ] && [ "${G:-0}" -ge $BASEG ]; do
  E=$(est "$C" "$G"); if [ "$E" -gt 0 ] && [ "$E" -le $((BASE + 120)) ]; then echo "--cpus-per-task=$C --mem=${G}G"; exit 0; fi
  C=$((C/2)); G=$((G/2))
done
echo "--cpus-per-task=$BASEC --mem=${BASEG}G"
