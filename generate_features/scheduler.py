import os
import logging
import datetime
import time
import re

from datetime import datetime

from itertools import groupby

import subprocess

import glob

from collections import namedtuple
from generate_features.generate_feats import GenerateFeatures

from generate_features import available_features
from generate_features.helpers import parquet_exists, check_timescale_cond, form_parquet_filepath


logging.basicConfig(format = '[%(levelname)s] [%(asctime)-15s]: %(message)s', level = logging.INFO)

SCRIPTS_DIR = "schedule-scripts/"
if not os.path.exists(SCRIPTS_DIR):
    logging.info("Create Schedule Scripts Dir.")
    os.mkdir(SCRIPTS_DIR)

class ScheduleGenerateFeatures(GenerateFeatures):

    def __init__(self, root_dir, save_dir, mock=True, **kwargs):
        super(ScheduleGenerateFeatures, self).__init__(root_dir)
        self.mock = mock
        self.job_dict = {}
        self.kwargs = kwargs
        self.save_dir=save_dir
        print(save_dir)
    

    def compute_job(self, feature, parquet_filepath, logtype, start_time, end_time, dependencies, final_df_type=None, cols=None, save_dir=None):
        # if parquet filepath exists in root_dir or in save_dir:
        save_dir = self.save_dir

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
                
            #options = {"account": self.account, "partition": self.partition, "mem-per-cpu": self.mem_per_cpu, "ntasks-per-node": self.ntasks_per_node}
            #options.update(self.kwargs)

            if self.mock:
                logging.info(command)
                return 42
            else: 
                print("This is dependencies", dependencies)
                fname = "[{}]-compute-{}-{}-{}.sh".format(str(time.time()).replace(':','-'), 
                str(start_time).replace(' ', '_').replace(':','-'), str(end_time).replace(' ', '_').replace(':','-'), logtype)
                print(os.path.join(SCRIPTS_DIR, fname))
                
                with open(os.path.join(SCRIPTS_DIR, fname), 'w+') as f:
                    f.write("#!/bin/bash\n\n{}\n".format(command))
                
                logging.info(command)

                os.system('chmod 0777 {}'.format(os.path.join(SCRIPTS_DIR, fname)))
                
                logging.info("TO SEE SCRIPS: {}".format(fname))
            self.job_dict[parquet_filepath] = fname
            return [fname]
        else:
            return []

    def merge_timeframes(self, dep_list):
        return None

            