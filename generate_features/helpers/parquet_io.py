import shutil
import os
import glob
import datetime
import time

import pandas as pd
from pyspark.sql import DataFrame

from fasteners import InterProcessLock

from generate_features.spark_utils.context import get_shared_sql_context
from generate_features.helpers.generic import parquet_open_permissions, remove_filepath
from generate_features.helpers.time import generate_time_list


def get_df_type(df):
    if isinstance(df, pd.DataFrame):
        return 'pandas'
    if isinstance(df, DataFrame):
        return 'spark'
    raise ValueError("Typeof(df) = ", type(df))



def form_parquet_filepath(root_dir, logtype, start_dt, end_dt, mkdr=False):
    ''' string, string, datetime, datetime -> string
        - creates path for the parquet file
        - creates directories for the parquet file if they don't already exist
    '''
    start_date = str(start_dt.date()).replace('-', '')
    start_time = str(start_dt.time()).split(':')
    start_time = start_time[0] + start_time[1]
    start = start_date + '-' + start_time

    end_date = str(end_dt.date()).replace('-', '')
    end_time = str(end_dt.time()).split(':')
    end_time = end_time[0] + end_time[1]
    end = end_date + '-' + end_time

    parquet_filename = logtype + '_' + start + '_' + end + '.parquet'

    # Overlap condition is meant to handle the case when we have a time range like so:
    # 2019-08-01 23:30:00 to 2019-08-02 00:00:00
    # In this case we would rather not create a directory that overlaps two days
    overlap_cond = False
    if (end_dt.date() - start_dt.date() == datetime.timedelta(days=1)):
        if (end_dt.time() == datetime.time(hour=0, minute=0)):
            overlap_cond = True

    log_dir = os.path.join(root_dir, logtype) 
    if (start_dt.date() == end_dt.date() or overlap_cond):
        date_str = str(start_dt.date())
    else:
        date_str =  str(start_dt.date()) + '_' + str(end_dt.date())
    date_dir = os.path.join(log_dir, date_str)

    if (not os.path.exists(log_dir) and mkdr == True):
        os.makedirs(log_dir)
        os.chmod(log_dir, 0o777)

    if (not os.path.exists(date_dir) and mkdr == True):
        os.makedirs(date_dir)
        os.chmod(date_dir, 0o777)

    return os.path.join(date_dir, parquet_filename)


def parquet_exists(filepath):
    ''' Note parquet files that are written in pandas are files, parquet
        files written in spark have a _SUCCESS file if they finish writing '''
    return os.path.exists(filepath) and \
          (os.path.isfile(filepath) or (os.path.exists(os.path.join(filepath, '_SUCCESS')) and len([f for f in os.listdir(filepath) if f.endswith('parquet')]) > 0))


def read_parquet(filepath, df_type='pandas', cols=None, filelock=False):
    if filelock:
        lock = InterProcessLock(filepath + ".lock")
        lock.acquire()
        result = read_parquet(filepath, df_type=df_type, cols=cols, filelock=False)
        lock.release()
        return result

    if (df_type == 'pandas'):
        # Check the special case where the parquet file is written in spark,
        # if it is we need to put all of the pieces together (for some reason pandas
        # can't read spark parquet files directly).
        if (os.path.exists(os.path.join(filepath, '_SUCCESS'))): # _SUCCESS file indicates spark parquet
            df_lst = []
            for i, name in enumerate(glob.glob(filepath + "/*.parquet")):
                df_lst.append(pd.read_parquet(name, columns=cols))
            if (len(df_lst) == 1):
                df = df_lst[0]
            else:
                df = pd.concat(df_lst, axis=0, ignore_index=True)
        else:
            df = pd.read_parquet(filepath, columns=cols)

    elif (df_type == 'spark'):
        sqlcontext = get_shared_sql_context()
        if cols is not None:
            df = sqlcontext.read.parquet(filepath).select(*cols)
        else:
            df = sqlcontext.read.parquet(filepath)
    else:
        raise Exception("Dataframe type not supported.")
    return df


def write_parquet(df, filepath):
    if parquet_exists(filepath):
        print("file already exists. Exiting.")
        return
    
    if os.path.exists(filepath):
        print("removing filepath:", filepath)
        remove_filepath(filepath)

    # Get the path up until the parquet filename
    # and create the dir if it doesn't exist
    file_dir = os.path.join(*filepath.split('/')[:-1])
    if (file_dir[0] != '/'):
        file_dir = '/' + file_dir
    os.makedirs(file_dir, mode=0o777, exist_ok=True)
    df_type = get_df_type(df)
    try:
        if (df_type == 'pandas'):
            df.to_parquet(filepath, compression=None)
        elif (df_type == 'spark'):
            if df.rdd.getNumPartitions() > 42:
                df = df.coalesce(15)
            #df.write.option("compression", "none").mode('overwrite').parquet(filepath)
            df.write.mode('overwrite').parquet(filepath)
        else:
            raise Exception("Dataframe type not supported.")
        parquet_open_permissions(filepath)

    except Exception as e:
        print()
        print("##############")
        print("EXCEPTION while writing {}!!!!!".format(filepath))
        s = str(e)
        print(type(e), s)
        if parquet_exists(filepath):
            print("parquet already exists, nevermind.")
            print("##############")
            print()
            return
        else:
            print("parquet_not_exists, removing!")
            print("##############")
            print()
            remove_filepath(filepath)
            raise e


class LockedParquet(object):
    def __init__(self, root_dir, feature, t, end):
        times = generate_time_list(t, end, feature.timescale)[0]
        self.start_dt = times[0]
        self.end_dt = times[1]
        self.path = form_parquet_filepath(root_dir, feature.name, times[0], times[1], mkdr=False)
        self.lock = InterProcessLock(self.path + ".lock")

    def write(self, df):
        self.lock.acquire()
        print("lock acquired on {}!".format(self.path))
        print('//////////////////////////////////////////////////////////')
        print(self.path)
        print(df.shape)
        print(df.head(10))
        df.to_parquet(self.path, append=True, compression=None)
        self.lock.release()

