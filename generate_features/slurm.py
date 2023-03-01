import os
import logging
import datetime

import re

from datetime import datetime

from itertools import groupby

import subprocess

import glob

from collections import namedtuple

from slurmpy import Slurm

from generate_features.generate_feats import GenerateFeatures

from generate_features import available_features
from generate_features.helpers import parquet_exists, check_timescale_cond, form_parquet_filepath


logging.basicConfig(format = '%(levelname)s: %(message)s', level = logging.INFO)


class SlurmGenerateFeatures(GenerateFeatures):

    def __init__(self, root_dir, account="netsec", partition="pcore", mem_per_cpu=36000, mock=True, ntasks_per_node=1, **kwargs):
        super(SlurmGenerateFeatures, self).__init__(root_dir)
        self.account = account
        self.partition = partition
        self.mem_per_cpu = mem_per_cpu
        self.mock = mock
        self.ntasks_per_node = ntasks_per_node
        self.job_dict = {}
        self.kwargs = kwargs

    def compute_job(self, feature, parquet_filepath, logtype, start_time, end_time, dependencies, final_df_type=None, cols=None, save_dir=None):
        # if parquet filepath exists in root_dir or in save_dir:
        if not save_dir and parquet_exists(parquet_filepath):
            return []
        
        if save_dir and parquet_exists(form_parquet_filepath(save_dir, logtype, start_time, end_time)):
            return []
        
        if feature.raw_loader and not save_dir and not parquet_exists(parquet_filepath):
            feature.raw_loader.filepath_or_error(self.root_dir, logtype, start_time, end_time)
            
        
        if feature.raw_loader and save_dir and not parquet_exists(form_parquet_filepath(save_dir, logtype, start_time, end_time)):
            feature.raw_loader.filepath_or_error(self.root_dir, logtype, start_time, end_time)

        if parquet_filepath not in self.job_dict and (save_dir or (check_timescale_cond(start_time, end_time, feature.timescale) and feature.memoize and not parquet_exists(parquet_filepath))):
            if save_dir:
                command = "features compute --root_dir '{}' --save_dir '{}' --start_time '{}' --end_time '{}' {}".format(self.root_dir, save_dir, start_time, end_time, logtype)
            else: 
                command = "features compute --root_dir '{}' --start_time '{}' --end_time '{}' {}".format(self.root_dir, start_time, end_time, logtype)
                
            options = {"account": self.account, "partition": self.partition, "mem-per-cpu": self.mem_per_cpu, "ntasks-per-node": self.ntasks_per_node}
            options.update(self.kwargs)

            if self.mock:
                return 42
            else: 
                s = Slurm(command, options)
                name_addition = str(datetime.now()).replace(' ', '_')

                if dependencies:
                    logging.info(dependencies)
                    dependencies = [d for ds in dependencies if ds is not None for d in ds if d is not None] 
                    s_id = s.run(command, depends_on=dependencies, name_addition=name_addition)
                else: 
                    s_id = s.run(command, name_addition=name_addition)
                logging.info("TO SEE SLURM JOB LOGS RUN:  tail -f logs/{}-{}*.err".format(s.name, name_addition))

            self.job_dict[parquet_filepath] = s_id
            return [s_id]
        else:
            return []

    def merge_timeframes(self, dep_list):
        return None



Job = namedtuple('Job', ['id', 'name', 'state'])
Parquet = namedtuple('Parquet', ['job', 'start_dt', 'end_dt', 'logtype', 'schedule_dt'])
job_name_pattern = "features-compute---.*starttime-(\d\d\d\d-\d\d-\d\d-\d\d\d\d\d\d)---endtime-(\d\d\d\d-\d\d-\d\d-\d\d\d\d\d\d)-(.*)-(\d\d\d\d-\d\d-\d\d_\d\d\:\d\d\:\d\d).*"

def correct_logtype(logtype):
    d = dict( (t.replace('_',''), t) for t in available_features.keys() )
    return d.get(logtype, logtype)

def sacct(scheduled_since, user = os.environ['USER'], start_dt=None, end_dt=None, logtypes=None, partition="pcore"):
    if logtypes:
        logtypes = set(logtypes)
    sacct = subprocess.run(["sacct", "-S", scheduled_since.strftime("%Y-%m-%d %H:%M"), "--partition", partition, "-u",user, "-n", "--format=JobID,JobName%200,State"], stdout=subprocess.PIPE)
    jobs = [ Job( *filter(lambda s: s, line.split(' ')) ) for line in str(sacct.stdout).split('\\n') if 'features-compute---' in line ]
    parquets = []
    for job in jobs:
        #print(job)
        try:
            p = Parquet( None, *re.match(job_name_pattern, job.name).groups() )
        except Exception as e:
            #print(e)
            continue
        p = Parquet(job, datetime.strptime(p.start_dt, "%Y-%m-%d-%H%M%S"), datetime.strptime(p.end_dt, "%Y-%m-%d-%H%M%S"), correct_logtype(p.logtype), datetime.strptime(p.schedule_dt, "%Y-%m-%d_%H:%M:%S"))
        #print(p)
        if (not start_dt or p.start_dt >= start_dt) and (not end_dt or p.end_dt <= end_dt) and (not logtypes or p.logtype in logtypes) and (not scheduled_since or p.schedule_dt >= scheduled_since):
            parquets.append(p)
    return parquets

def job_state_sort_order(parquet):
    if parquet.job.state.startswith("FAIL"):
        return 0
    if parquet.job.state.startswith("PEND"):
        return 1
    if parquet.job.state.startswith("COMP"):
        return 2
    return 42

def markdown(parquets, reverse=True):
    parquets.sort(key=job_state_sort_order, reverse=reverse)
    md = ""
    for state, ps in groupby(parquets, lambda p: p.job.state):
        md += "# {}\n\n".format(state)
        if state.startswith("FAIL"):
            for p in ps:
                md += markdown_with_log(p)
        else:
            for logtype, gps in groupby(sorted(ps, key=lambda p: p.logtype), lambda p: p.logtype):
                md += "\n## {}\n\n".format(logtype)
                for p in gps:
                    md += markdown_compact(p)
    return md

def get_log(parquet, ext='err'):
    globber = "logs/{}*.{}".format(parquet.job.name, ext)
    #print("globber", globber)
    g = glob.glob(globber)
    for fn in g:
        #print("fn", fn)
        with open(fn, 'r') as f:
            return fn, f.readlines()
    return None, None
    
def markdown_with_log(parquet):
    md = "\n\n## {} {} - {}\n\n".format(parquet.logtype, str(parquet.start_dt), str(parquet.end_dt))
    filepath, log = get_log(parquet)
    if log is not None:
        md += "\n\n<details>\n\n<summary>Error log</summary>\n\n"
        md += "".join(log)
        md += "\n\n</details>\n\n"
        md += "[{}]({})\n\n".format(filepath, filepath)
    return md

def markdown_compact(parquet):
    return "  {} - {}\n\n".format(str(parquet.start_dt), str(parquet.end_dt))
