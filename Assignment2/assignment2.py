#! usr/bin/env python3

"""Program that reads and processes an fastq file"""

import sys
import argparse as ap
import multiprocessing as mp
from multiprocessing.managers import BaseManager
import time
import queue
import csv
import subprocess

POISONPILL = "MEMENTOMORI"
ERROR = "DOH"
IP = '127.0.0.1'
PORTNUM = 8899
AUTHKEY = b'whathasitgotinitspocketsesss?'

class FileCorrupt(Exception):
    """Custom error for corrupt files"""
    pass

class MissingArgument(Exception):
    """Custom error when arguments are missing"""
    pass

def divide_lines(fastq_files, chunks):
    """Divides lines of fastq files in even sized chunks"""
    chunk_params = []
    line_counts = {}
    for fastq_file in fastq_files:

        lines = subprocess.check_output(f'wc -l {fastq_file}', shell=True)
        lines = int(lines.decode('UTF-8').split()[0]) / 4
        line_counts[fastq_file] = lines

        stop = 0
        for i in range(chunks):
            start = stop
            stop = round(lines / chunks * (i + 1))
            chunk_params.append([fastq_file, start, stop])

    return chunk_params, line_counts

def read_fastq(input):
    """Reads the fastq file and calcualates the phredscores"""
    fastqfile = input[0]
    start = input[1]
    stop = input[2]
    with open(fastqfile, 'r') as fastq:
    #fastforward
        counter = 0
        while counter < start:
            fastq.readline()
            fastq.readline()
            fastq.readline()
            fastq.readline()
            counter += 1

        results = {}
        # counter = 0
        while counter < stop:
            fastq.readline()
            nucleotides = fastq.readline().rstrip()
            fastq.readline()
            qual = fastq.readline().rstrip()
            counter += 1

            if len(qual) != len(nucleotides):
                raise FileCorrupt('Nucleotides length is different from quality length')

            if not qual:
                # we reached the end of the file
                break

            for j, c in enumerate(qual):
                try:
                    results[fastqfile][j] += ord(c) - 33
                except KeyError:
                    results[fastqfile] = [ord(c) - 33]
                except IndexError:
                    results[fastqfile].append(ord(c) - 33)

    return results

def calculate_mean_quals(results, num_reads):
    """Calculates the mean phredscores over multiple results"""
    phredscores_total = {}
    for item in results:
        for file_name, scores in item['result'].items():
            for i, score in enumerate(scores):
                try:
                    phredscores_total[file_name][i] += score
                except KeyError:
                    phredscores_total[file_name] = [score]
                except IndexError:
                    phredscores_total[file_name].append(score)

    phredscores_avg = {}
    for file_name, scores in phredscores_total.items():
        for score in scores:
            try:
                phredscores_avg[file_name].append(score / num_reads[file_name])
            except KeyError:
                phredscores_avg[file_name] = [score / num_reads[file_name]]

    return phredscores_avg

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

def make_server_manager(port, authkey):
    """ Create a manager for the server, listening on the given port.
        Return a manager object with get_job_q and get_result_q methods.
    """

    job_q = queue.Queue()
    result_q = queue.Queue()
    # This is based on the examples in the official docs of multiprocessing.
    # get_{job|result}_q return synchronized proxies for the actual Queue
    # objects.
    global QueueManager
    class QueueManager(BaseManager):
        pass
    QueueManager.register('get_job_q', callable=lambda: job_q)
    QueueManager.register('get_result_q', callable=lambda: result_q)
    manager = QueueManager(address=('', port), authkey=authkey)
    manager.start()
    # print(f'Server started at ip {IP} and port {port}')
    return manager

def runserver(fn, data, line_counts):
    """Runs the server"""
    # Start a shared manager server and access its queues
    manager = make_server_manager(PORTNUM, b'whathasitgotinitspocketsesss?')
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()

    if not data:
        print("Gimme something to do here!")
        return

    # print("Sending data!")
    for d in data:
        shared_job_q.put({'fn' : fn, 'arg' : d})

    time.sleep(2)

    results = []
    while True:
        try:
            result = shared_result_q.get_nowait()
            results.append(result)
            if len(results) == len(data):
                # print("Got all results!")
                break
        except queue.Empty:
            time.sleep(1)
            continue
    # Tell the client process no more data will be forthcoming
    # print("Time to kill some peons!")
    shared_job_q.put(POISONPILL)
    # Sleep a bit before shutting down the server - to give clients time to
    # realize the job queue is empty and exit in an orderly way.
    time.sleep(5)
    # print("Aaaaaand we're done for the server!")
    manager.shutdown()
    mean_quals = calculate_mean_quals(results, line_counts)

    for file_name, phredscore in mean_quals.items():
        if len(args.fastq_files) > 1:
            if args.csvfile is None:
                print(file_name)
                csvfile = None
            else:
                csvfile = f'{file_name}.{args.csvfile}'
        else:
            csvfile = args.csvfile
        generate_output(phredscore, csvfile)

def make_client_manager(ip, port, authkey):
    """ Create a manager for a client. This manager connects to a server on the
        given address and exposes the get_job_q and get_result_q methods for
        accessing the shared queues from the server.
        Return a manager object.
    """
    class ServerQueueManager(BaseManager):
        pass

    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')

    manager = ServerQueueManager(address=(ip, port), authkey=authkey)
    manager.connect()

    # print('Client connected to %s:%s' % (ip, port))
    return manager

def runclient(num_processes):
    """Runs the client"""
    manager = make_client_manager(IP, PORTNUM, AUTHKEY)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    run_workers(job_q, result_q, num_processes)

def run_workers(job_q, result_q, num_processes):
    """Runs the workers"""
    processes = []
    for _ in range(num_processes):
        temP = mp.Process(target=peon, args=(job_q, result_q))
        processes.append(temP)
        temP.start()
    # print("Started %s workers!" % len(processes))
    for temP in processes:
        temP.join()

def peon(job_q, result_q):
    """Runs a peon for a job"""
    my_name = mp.current_process().name
    while True:
        try:
            job = job_q.get_nowait()
            if job == POISONPILL:
                job_q.put(POISONPILL)
                # print("Aaaaaaargh", my_name)
                return
            else:
                try:
                    # print("Peon %s works on %s!" % (my_name, job['arg']))
                    result = job['fn'](job['arg'])
                    result_q.put({'job': job, 'result' : result})
                except NameError:
                    # print("Can't find yer fun Bob!")
                    result_q.put({'job': job, 'result' : ERROR})

        except queue.Empty:
            # print("sleepytime for", my_name)
            time.sleep(1)

if __name__ == '__main__':
    # argparse
    argparser = ap.ArgumentParser(description="Script for assignment 2 of Big Data Computing")
    mode = argparser.add_mutually_exclusive_group(required=True)
    mode.add_argument("-s", action="store_true",
                    help="Run the program in Server mode; see extra options needed below")
    mode.add_argument("-c", action="store_true",
                    help="Run the program in Client mode; see extra options needed below")

    server_args = argparser.add_argument_group(title="Arguments when run in server mode")
    server_args.add_argument("-chunks", action="store", type=int,
                        help="amount of chunks to be used")
    server_args.add_argument("-o", action="store", dest="csvfile", required=False,
                        help="CSV file to save the output. Default is output to terminal STDOUT")
    server_args.add_argument("fastq_files", action="store",
                        nargs='*', help="At least 1 ILLUMINA fastq file to process")

    client_args = argparser.add_argument_group(title="Arguments when run in client mode")
    client_args.add_argument("--host", dest="host", action="store", type=str,
                        help="The hostname where the Server is listening")
    client_args.add_argument("--port", dest="port", action="store", type=int,
                        help="The port on which the Server is listening")
    client_args.add_argument("-n", action="store", dest="n", required=False, type=int,
                        help="Number of  cores to be used per per host.")

    args = argparser.parse_args()

    if args.s is True:
        if args.port is not None:
            PORTNUM = args.port

        if len(args.fastq_files) == 0:
            raise MissingArgument('No Input files!')
        if args.chunks is None:
            raise MissingArgument('No chunks defined')

        processes, line_counters = divide_lines(args.fastq_files, args.chunks)
        server = mp.Process(target=runserver, args=(read_fastq, processes, line_counters))
        server.start()
        time.sleep(1)
        server.join()

    elif args.c is True:
        IP = args.host
        PORTNUM = args.port
        client = mp.Process(target=runclient, args=(args.n,))
        client.start()
        client.join()
