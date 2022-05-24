#! usr/bin/env python3

"""Program that reads and processes an fastq file"""

import sys
import argparse as ap
import multiprocessing as mp
import csv

def chunks(lst, chunks):
    """Divides lst in n-chunks"""
    splitted = []
    len_l = len(lst)
    for i in range(chunks):
        start = int(i*len_l/chunks)
        end = int((i+1)*len_l/chunks)
        splitted.append(lst[start:end])

    return splitted

def read_fastq(fastq_file):
    """Reads fastq"""
    quals = []
    qual = True

    with open(fastq_file) as fastq:
        while qual:
            _ = fastq.readline()
            _ = fastq.readline()
            _ = fastq.readline()
            qual = fastq.readline().rstrip()
            if qual:
                quals.append(qual)

    return quals

def calculate_quals(quals):
    """ Calculates quality scores """
    results = []
    for qual in quals:
        for i, char in enumerate(qual):
            try:
                results[i] += ord(char) - 33
            except IndexError:
                results.append(ord(char) - 33)
    return results


def generate_output(average_phredscores, csvfile):
    """Generates CSV output"""
    if csvfile is None:
        csv_writer = csv.writer(sys.stdout, delimiter=',')
        for i, score in enumerate(average_phredscores):
            csv_writer.writerow([i, score])

    else:
        with open(csvfile, 'w', encoding='UTF-8', newline='') as file:
            csv_writer = csv.writer(file, delimiter=',')
            for i, score in enumerate(average_phredscores):
                csv_writer.writerow([i, score])

if __name__ == '__main__':
    # argparse
    argparser = ap.ArgumentParser(description="Script for assignment 1 of Big Data Computing")
    argparser.add_argument("-n", action="store",
                        dest="n", required=True, type=int,
                        help="Amount of cores to be used")
    argparser.add_argument("-o", action="store", dest="csvfile", required=False,
                        help="CSV file to save the output. Default is output to terminal STDOUT")
    argparser.add_argument("fastq_files", action="store",
                        nargs='+', help="At least 1 ILLUMINA fastq file to process")
    args = argparser.parse_args()

    for file in args.fastq_files:
        qualities = read_fastq(file)
        qual_chunked = chunks(qualities, 4)

        with mp.Pool(args.n) as pool:
            phredscores = pool.map(calculate_quals, qual_chunked)
        print(len(qualities))
        phredscores_avg = [sum(i) / len(qualities) for i in zip(*phredscores)]
        print(phredscores_avg)
        if len(args.fastq_files) > 1:
            if args.csvfile is None:
                print(file)
                csvfile = None
            else:
                csvfile = f'{file}.{args.csvfile}'
        else:
            csvfile = args.csvfile

        generate_output(phredscores_avg, csvfile)