import os
import glob
import random
import shutil
import datetime

from generate_features import available_features
from generate_features.helpers import generate_time_list, parquet_exists, \
                            form_parquet_filepath, read_parquet, concat_df_rows


def load_available_parquets(logtype, root_dir, start_dt, end_dt, df_type='pandas', mute=True):
    timescale = available_features[logtype].timescale
    time_list = generate_time_list(start_dt, end_dt, timescale)

    df_list = list()
    for t in time_list:
        parquet_filepath = form_parquet_filepath(root_dir, logtype, t[0], t[1])
        if parquet_exists(parquet_filepath):
            if not mute:
                print(parquet_filepath)
            df_list.append(read_parquet(parquet_filepath, df_type=df_type))

    if df_list:
        return concat_df_rows(df_list)
    else:
        return None

