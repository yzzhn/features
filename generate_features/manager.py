import os
import logging
import datetime
import psutil
import schedule as sc
import time
import numpy as np
import multiprocessing
import shutil

import sys

from itertools import groupby

import subprocess
import glob

from collections import namedtuple


logging.basicConfig(format = '[%(levelname)s] [%(asctime)-15s]: %(message)s', level = logging.INFO)

SCRIPTS_DIR = "schedule-scripts/"
LOG_DIR = "schedule-logs"
ARCHIVE_DIR = "schedule-archive"

if not os.path.exists(LOG_DIR):
    logging.info("Create Schedule log Dir.")
    os.mkdir(LOG_DIR)

if not os.path.exists(ARCHIVE_DIR):
    logging.info("Create Schedule log Dir.")
    os.mkdir(ARCHIVE_DIR)


class JobManager():
    def __init__(self, ntasks=4, maxcpu_ratio=80, maxram_ratio=80, jobdir=SCRIPTS_DIR, logdir=LOG_DIR, archive=ARCHIVE_DIR):
        self.maxcpu_ratio = maxcpu_ratio
        self.maxram_ratio = maxram_ratio
        self.ntasks = ntasks
        self.job_queue = []
        self.priority_queue = []
        self.jobdir = jobdir
        self.work_queue = []
        self.logdir = logdir
        self.archive = archive

    def add_job(self, fpath):
        self.job_queue.append(fpath)
        return self.job_queue

    def delete_job(self, fpath):
        self.job_queue.remove(fpath)
        return self.job_queue

    def check_new_jobs(self, jobdir):
        for fpath in os.listdir(jobdir):
            if fpath not in self.job_queue:
                self.job_queue = self.add_job(fpath)
        self.job_queue.sort()
        return self.job_queue

    def compute_cpu_stats(self):
        cpu_l = []
        for i in range(5):
            cpu_l.append(psutil.cpu_percent())
            time.sleep(1)
        return np.mean(cpu_l)

    def run(self):
        return 0

print(psutil.cpu_percent())
print(psutil.virtual_memory())  # physical memory usage
print('memory % used:', psutil.virtual_memory()[2])

def run_cmd(jobfpath, logfile, archive):
    command = "nohup ./{} > {} &".format(os.path.join(manager.jobdir,jobfpath), logfile)
    with open(logfile, 'w+') as f:
        res = subprocess.call(command.split(" "), stdout=f)
    logging.info("Process Finished. Archive Scripts.")
    shutil.move(os.path.join(manager.jobdir,jobfpath), archive)
    return res

def create_worker(fncs, command, logfile, archive):
    p = multiprocessing.Process(target=fncs, args=(command, logfile, archive)) 
    return p

if __name__ == "__main__":

    manager = JobManager()

    jobs = manager.check_new_jobs(manager.jobdir)
    jobs.reverse()
    
    if len(sys.argv) == 2:
        try:
            if 0< int(sys.argv[1]) <= multiprocessing.cpu_count():
                nprocess = int(sys.argv[1])
            else:
                nprocess = 2
        except:
            nprocess = 2

    print("MultiProcess Count:", nprocess)

    current_process = 0
    job_handles = []
    updated = 0

    while len(jobs) > 0:

        for i in range(nprocess - len(job_handles)):
            print("busy worker:", len(job_handles))

            if len(jobs) == 0:
                print("No More Jobs to work! Hurray!")
                print("Let's wait for other jobs to ends!")
                break
            jobfpath = jobs[0]
            logfile = os.path.join(LOG_DIR, jobfpath)
            archive = os.path.join(ARCHIVE_DIR, jobfpath)
            
            j = create_worker(run_cmd, jobfpath, logfile, archive)
            logging.info("Worker {} created for job {}".format(j, jobfpath))
            job_handles.append(j)
            j.start()
            jobs.remove(jobfpath)
            logging.info("Total jobs left {}:".format(len(jobs)))
        
        for j in job_handles:
            if not j.is_alive():
                logging.info("worker {} finished. Remove Worker.".format(j))
                job_handles.remove(j)
                logging.info("Available workers: {}".format(nprocess-len(job_handles)))
            #else:
                #print("Worker {} still working.".format(j))
        time.sleep(10)
