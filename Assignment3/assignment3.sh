#!/bin/bash
#SBATCH --time 72:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=17
#SBATCH --partition=assemblix
export DATA=/data/dataprocessing/MinIONData/all.fq
export REF=/data/dataprocessing/MinIONData/all_bacteria.fna
source /commons/conda/conda_load.sh
for n in {1..16} ; do /usr/bin/time -o timings.txt --append -f "${n}\t%e" minimap2  -a -t ${n} $REF $DATA > /dev/null ; done