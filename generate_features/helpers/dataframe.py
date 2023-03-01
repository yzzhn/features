import os
import time
import logging
from functools import reduce

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as F

from generate_features.helpers.parquet_io import write_parquet, read_parquet

def log_dataframe_info(df, filepath, message=None):
    df_type = get_df_type(df)
    if (df_type == 'pandas'):
        logging.info(str((filepath.split('/')[-1])) + ' ' + str(df.shape) + ' - ' + message)
    elif (df_type == 'spark'):
        logging.info(str((filepath.split('/')[-1])) + ' ' + str((df.count(), len(df.columns))) + ' - ' + message)
    else:
        logging.warning('Dataframe type not supported for logging.')


def get_df_type(df):
    if isinstance(df, pd.DataFrame):
        return 'pandas'
    if isinstance(df, DataFrame):
        return 'spark'
    raise ValueError("Typeof(df) = ", type(df))


def columns_in_df(df, cols):
    if cols is None:
        return True
    cols_set = set(list(cols))
    df_cols_set = set(list(df.columns))
    return cols_set.issubset(df_cols_set)


def drop_list_type_cols(df):
    # Finds which pandas columns contain list types and drops those cols. 
    df_type = get_df_type(df)

    if df_type == 'pandas':
        df_cols = list()
        for col in df.columns:
            if not df[col].apply(lambda v: isinstance(v, list)).any():
                df_cols.append(col)
        result = df[df_cols]
    else:
        raise Exception(df_type + ' df type not supported by the drop_list_type_cols function.')
    return result


def convert_df_type(df, parquet_filepath, cols, final_df_type):
    df_type = get_df_type(df)
    if df_type != final_df_type:
        result = None
        start = time.time()
        if df_type == 'spark' and final_df_type == 'pandas':
            return df.toPandas()
        if df_type == 'pandas' and final_df_type == 'spark':
            write_parquet(df, parquet_filepath)
            return read_parquet(parquet_filepath, final_df_type, cols=cols)
        if result is not None:
            log_dataframe_info(result, parquet_filepath, final_df_type, "TOOK " + str(time.time() - start) + " SEC TO CONVERT.")
            return result
        raise ValueError("Cannot convert {} to {}",format(df_type, final_df_type))
    return df


def trim_df_columns(df, desired_cols):
    if desired_cols is None:
        return df

    df_type = get_df_type(df)

    desired_cols_set = set(list(desired_cols))
    df_cols_set = set(list(df.columns))

    if desired_cols_set == df_cols_set:
        return df
    else:
        if (desired_cols_set.issubset(df_cols_set)):
            if df_type == 'pandas':
                return df[desired_cols]
            elif df_type == 'spark':
                return df.select(desired_cols)
            else:
                raise Exception('Unknown df type.')
        else:
            raise Exception('Dataframe is missing desired columns')


def intersect_iterables(*iterables):
    sets_of_iterables = map(set, iterables)
    return list(reduce(lambda a, b: a.intersection(b), sets_of_iterables))


def concat_df_rows(df_list):
    ''' Helper- Used for combining several dataframes (row-wise). '''
    df_type = get_df_type(df_list[0])
    for df in df_list:
        if (get_df_type(df) != df_type):
            raise Exception("Dataframes in df_list are of different types.")

    columns = intersect_iterables(*[df.columns for df in df_list])
 
    if (df_type == 'pandas'):
        df_list = [df[columns] for df in df_list]
        df = pd.concat(df_list, axis=0)
    elif (df_type == 'spark'):
        df_list = [df.select(columns) for df in df_list]
        df = reduce(DataFrame.union, df_list)
    else:
        raise Exception("Dataframe type not supported.")
    return df
