import os
import time
import logging
import math
import datetime
import random
import functools
import traceback

import pandas as pd

from generate_features.helpers import *
from generate_features import available_features
from generate_features.features import MissingRawLogError

logging.basicConfig(format = '[%(levelname)s] [%(asctime)-15s]: %(message)s', level = logging.INFO)


def validate_feature_coordinates(logtype, start_dt, end_dt):
    if logtype not in available_features:
        raise Exception(logtype + ' feature is not supported.  Supported features are:\n' + str(list(available_features.keys())))

    if (start_dt >= end_dt):
        raise Exception('start_dt must be less than end_dt')


class GenerateFeatures:
    def __init__(self, root_dir, logging_disabled=False):
        self.root_dir = root_dir
        logging.getLogger().disabled = logging_disabled


    def get_feature(self, logtype, start_dt, end_dt, final_df_type=None, cols=None, save_dir=None, allow_missing=False):
        """
        Return a dataframe containing features.

        If the specified logtype has `memoize` set to True, or if `save_dir` is not None, 
        parquet files of sizes set by the logtype's timescale will be saved from `start_dt` to `end_dt`.

        Arguments:
        logtype        --  the type/name of a feature, raw or computed
        start_dt     --  a datetime object
        end_dt       --  a datetime object

        Keyword arguments:
        final_df_type  --  'spark' or 'pandas' or None, which returns df native to specified feature type
        cols           --  list of columns to keep, or None to keep them all
        save_dir       --  alternative root directory to save results in; if feature.memoize is True, 
                           save_dir will be overridden by root_dir, where the result will be saved instead.
        """
        self._validate_user_input(logtype, start_dt, end_dt, final_df_type, cols, save_dir)

        feature = available_features[logtype]
        parquet_filepath = form_parquet_filepath(self.root_dir, logtype, start_dt, end_dt)

        if not final_df_type:
            final_df_type = feature.df_type

        result = None

        if parquet_exists(parquet_filepath):
            logging.info("PARQUET already exists {}".format(parquet_filepath))
            return self.compute_job(feature, parquet_filepath, logtype, start_dt, end_dt, 
                                    [], final_df_type=final_df_type, cols=cols, save_dir=save_dir)
        
        elif (not check_timescale_cond(start_dt, end_dt, feature.timescale)):
            logging.info("SPLITTING INTO SMALLER TIMEFRAMES")
            time_frames = generate_time_list(start_dt, end_dt, feature.timescale)
            time_frames.reverse()

            dep_list = []
            errors = []
            for t in time_frames:
                try:
                    dep = self.get_feature(logtype, t[0], t[1], final_df_type=final_df_type, cols=cols, save_dir=save_dir, allow_missing=allow_missing)
                    dep_list.append(dep)
                except MissingRawLogError as e:
                    errors.append(e)

            if errors and (all((d is None for d in dep_list)) or not allow_missing) :
                print("dep_list", dep_list, "errors", errors)
                print("logtype {}  start_dt {}  end_dt {}".format(logtype, start_dt, end_dt))

                error_missing_filepaths = functools.reduce(lambda a,b: a | b, [e.missing_filepaths for e in errors])
                raise MissingRawLogError(error_missing_filepaths, logtype, start_dt, end_dt)

            result = self.merge_timeframes(dep_list)

        else:
            logging.info("Scheduling job to COMPUTE {}".format(parquet_filepath))
            logging.info("COMPUTING FEATURE {}".format(logtype))
            dependencies = list()
            for dep in feature.dependencies:
                new_start_dt = start_dt + dep.offset
                new_end_dt = end_dt + dep.offset
                try:
                    dependency = self.get_feature(dep.feature_name, new_start_dt, new_end_dt,
                                                final_df_type=feature.df_type, cols=dep.columns, allow_missing=dep.allow_missing)
                except MissingRawLogError as e:
                    if dep.allow_missing:
                        dependency = None
                    else:
                        raise e
                dependencies.append(dependency)

            result = self.compute_job(feature, parquet_filepath, logtype, start_dt, end_dt, dependencies, 
                                        final_df_type=final_df_type, cols=cols, save_dir=save_dir)

        return result


    def _validate_user_input(self, logtype, start_dt, end_dt, final_df_type, cols, save_dir):
        try:
            validate_feature_coordinates(logtype, start_dt, end_dt)

        except Exception as e:
            logging.info(f'User input: logtype: {logtype}, start_dt: {start_dt}, end_dt: {end_dt}, final_df_type: {final_df_type}')
            print(e)
            

    def merge_timeframes(self, dep_list):
        dep_list = [dep for dep in dep_list if dep is not None]
        dep_list.reverse()
        if dep_list:
            return concat_df_rows(dep_list)
        else:
            return None



    def compute_job(self, feature, parquet_filepath, logtype, start_dt, end_dt, dependencies, final_df_type=None, cols=None, save_dir=None):

        logging.info("compute_job({}, {})".format(parquet_filepath, str(dependencies)))

        start = time.time()

        if parquet_exists(parquet_filepath):
            logging.info("LOADING PARQUET {}".format(parquet_filepath))
            result = read_parquet(parquet_filepath, final_df_type, cols=cols, filelock=feature.filelock)

        elif feature.raw_loader:
            if dependencies:
                raise Exception("Raw loader '{}' should have no dependencies but instead got {}!".format(parquet_filepath, str(len(dependencies))))

            logging.info("LOADING RAW LOG or FEED {}".format(logtype))
            try:
                result = feature.raw_loader.read(root_dir=self.root_dir, logtype=logtype, start_dt=start_dt, end_dt=end_dt, 
                                                 parquet_filepath=parquet_filepath, result_df_type=final_df_type, columns=cols, 
                                                 filelock=feature.filelock)
            except MissingRawLogError as e:
                raise e

            except Exception as err:
                print(err)
                traceback.print_tb(err.__traceback__)
                print("Something went wrong processing log for " + parquet_filepath)
                raise err

        else:
            compute_info = {'start_dt': start_dt, 'end_dt': end_dt}
            result = feature.compute_feature(*dependencies, compute_info=compute_info) 

        if result is not None:
            log_dataframe_info(result, parquet_filepath, ' IN ' + str(time.time() - start) + ' SECONDS')

            if check_timescale_cond(start_dt, end_dt, feature.timescale) and feature.memoize:
                if not save_dir and not os.path.exists(parquet_filepath):
                    print("saving to file:", form_parquet_filepath(self.root_dir, logtype, start_dt, end_dt, mkdr=True))
                    write_parquet(result, form_parquet_filepath(self.root_dir, logtype, start_dt, end_dt, mkdr=True))
                    
                elif save_dir and not parquet_exists(form_parquet_filepath(save_dir, logtype, start_dt, end_dt)):
                    print("saving to file:", form_parquet_filepath(save_dir, logtype, start_dt, end_dt, mkdr=True))
                    write_parquet(result, form_parquet_filepath(save_dir, logtype, start_dt, end_dt, mkdr=True))
                    
            elif save_dir:
                print("saving to file:", form_parquet_filepath(save_dir, logtype, start_dt, end_dt))
                write_parquet(result, form_parquet_filepath(save_dir, logtype, start_dt, end_dt))

            result = trim_df_columns(result, cols)
            result = convert_df_type(result, parquet_filepath, cols, final_df_type)

        return result

