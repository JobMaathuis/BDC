#!/bin/bash
export DATA=/data/dataprocessing/rnaseq_data/Brazil_Brain/
export WDIR=/homes/jmaathuis/Documents/BDC/Assignment4
echo Filenaam,Valide,Min_length,Max_length,Average_length
#echo /data/dataprocessing/rnaseq_data/./Brazil_Brain/SPM11_R1.fastq | parallel -j4 --workdir $WDIR --transferfile {} --basefile ${WDIR}/assignment4lin.py --return output.csv --results $WDIR --sshloginfile hosts.txt "assignment4lin.py {= s:.*/\./:./: =} > output.csv"
# echo /data/dataprocessing/rnaseq_data/./Brazil_Brain/SPM11_R1.fastq | parallel -j4 --workdir $WDIR --transferfile {} --basefile assignment4lin.py --results $WDIR --sshloginfile hosts.txt "assignment4lin.py {= s:.*/\./:./: =}"
# find $DATA -name 'SPM11_*.fastq' |
 find $DATA -name '*.fastq' | parallel --workdir $WDIR -j2 --results ./ --sshloginfile hosts.txt $WDIR/assignment4.py {}
# --results --nonall --sshloginfile hosts.txt --trc {}