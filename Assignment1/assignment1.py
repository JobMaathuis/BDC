#! usr/bin/env python3

import sys
import argparse as ap
import multiprocessing as mp
import csv

def chunks(l, n):
    """returns successive n-sized chunks from lst."""
    splitted = []
    len_l = len(l)
    for i in range(n):
        start = int(i*len_l/n)
        end = int((i+1)*len_l/n)
        splitted.append(l[start:end])
        
    return splitted

def read_fastq(fastq_file):
    quals = []
    qual = True
    
    with open(fastq_file) as fastq:
        while qual:
            header = fastq.readline()
            nucleotides = fastq.readline()
            strand = fastq.readline()
            qual = fastq.readline().rstrip()
            if qual:
                quals.append(qual)
            
    return quals

def calculate_quals(quals):
    """ Calculates quality scores """
    results = []
    for qual in quals:
        for j, c in enumerate(qual):
                try:
                    results[j] += ord(c) - 33
                except IndexError:
                    results.append(ord(c) - 33)
    return results


def generate_output(average_phredscores, csvfile, input_file):
    if csvfile is None:
            csv_writer = csv.writer(sys.stdout, delimiter=',')
            for i, score in enumerate(average_phredscores): 
                csv_writer.writerow([i, score])
    
    else:
        if len(input_file) == 1:
            with open(csvfile, 'w', encoding='UTF-8', newline='') as file:
                csv_writer = csv.writer(file, delimiter=',')
                for i, score in enumerate(average_phredscores): 
                    csv_writer.writerow([i, score])
        else:
            with open(f'{input_file}.{csvfile}', 'w', encoding='UTF-8', newline='') as file:
                csv_writer = csv.writer(file, delimiter=',')
                for i, score in enumerate(average_phredscores): 
                    csv_writer.writerow([i, score])
            

if __name__ == '__main__':
    # argparse
    argparser = ap.ArgumentParser(description="Script for assignment 1 of Big Data Computing")
    argparser.add_argument("-n", action="store",
                        dest="n", required=True, type=int,
                        help="Amount of cores to be used")
    argparser.add_argument("-o", action="store", dest="csvfile", 
                           required=False, help="CSV file to save the output. Default is output to terminal STDOUT")
    argparser.add_argument("fastq_files", action="store",
                        nargs='+', help="At least 1 ILLUMINA fastq file to process")
    args = argparser.parse_args()
    
    for file in args.fastq_files:
        # print(file + args.csvfile)
        qualities = read_fastq(file)
        qual_chunked = chunks(qualities, 4)
        
        with mp.Pool(args.n) as pool:
            phredscores = pool.map(calculate_quals, qual_chunked)
    
        phredscores_avg = [sum(i) / len(qualities) for i in zip(*phredscores)]
        generate_output(phredscores_avg, args.csvfile, file)
       
