name = "domain_history"

import math
import datetime

import pandas as pd
import numpy as np
import ipaddress

from generate_features.features import *
from generate_features.features.domain_history.dom_history import *
from generate_features.features.track1_time_series.preprocess import *

""" For writing an empty parquet file:
import pandas as pd
df = pd.DataFrame(columns=['host', 'firstseen_date', 'lastseen_date', 'firstseen_log_type', 'lastseen_log_type', 'days_since_lastseen', 'days_since_firstseen'], dtype=str)
df['days_since_lastseen'] = df['days_since_lastseen'].astype(int)
df['days_since_firstseen'] = df['days_since_firstseen'].astype(int)
df.to_parquet('domain_history_20190803-0000_20190804-0000.parquet', engine='fastparquet')
"""


@FeatureConfig(
    dependencies=[
        Dependency("http_domain_history", offset=-datetime.timedelta(days=1)),
        Dependency("http", columns=["host", "id_resp_h", "anon_host"], allow_missing=True),
    ],
    df_type="pandas",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def http_domain_history(domain_history_df, http_df, compute_info):
    if http_df is None:
        return domain_history_df

    http_df = http_df.drop_duplicates()
    http_df = http_df.dropna()

    print(http_df.shape)
    
    res = gen_domain_history(domain_history_df, http_df, compute_info)
    
    return res



@FeatureConfig(
    dependencies=[
        Dependency("http_domain_history_vt", offset=-datetime.timedelta(days=1)),
        Dependency("http_vt", columns=["host", "id_resp_h", "anon_host"], allow_missing=True),
    ],
    df_type="pandas",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def http_domain_history_vt(domain_history_df, http_df, compute_info):
    if http_df is None:
        return domain_history_df

    http_df = http_df.drop_duplicates()
    http_df = http_df.dropna()

    print(http_df.shape)

    res = gen_domain_history(domain_history_df, http_df, compute_info)
    
    return res



@FeatureConfig(
    dependencies=[
        Dependency("ssl_domain_history", offset=-datetime.timedelta(days=1)),
        Dependency("ssl", columns=["server_name", "id_resp_h", "anon_resp"], allow_missing=True),
    ],
    df_type="pandas",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def ssl_domain_history(domain_history_df, zeek_df, compute_info):
    if zeek_df is None:
        return domain_history_df

    print(zeek_df.shape)

    zeek_df = zeek_df.drop_duplicates()
    zeek_df = zeek_df.dropna()

    print(zeek_df.shape)

    res = gen_domain_history(domain_history_df, zeek_df, compute_info, logtyp="SSL")
    
    return res


@FeatureConfig(
    dependencies=[
        Dependency("ssl_domain_history_vt", offset=-datetime.timedelta(days=1)),
        Dependency("ssl", columns=["server_name", "id_resp_h", "anon_resp"], allow_missing=True),
    ],
    df_type="pandas",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def ssl_domain_history_vt(domain_history_df, zeek_df, compute_info):
    if zeek_df is None:
        return domain_history_df

    print(zeek_df.shape)

    zeek_df = zeek_df.drop_duplicates()
    zeek_df = zeek_df.dropna()

    print(zeek_df.shape)

    res = gen_domain_history(domain_history_df, zeek_df, compute_info, logtyp="SSL")
    
    return res



@FeatureConfig(
    dependencies=[
        Dependency("http_periodicity_hist", offset=-datetime.timedelta(days=1)),
        Dependency("http_periodicity", columns = ["host", "true_periods"], allow_missing=True),
    ],
    df_type="pandas",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def http_periodicity_hist(domain_history_df, perdf, compute_info):
    if perdf is None:
        return domain_history_df

    perdf = perdf.loc[(perdf["true_periods"].map(len)>0)]
    print(perdf.shape)
    perdf = perdf.drop(columns=["true_periods"])
    res = gen_periodicity_history(domain_history_df, perdf, compute_info)
    
    return res


@FeatureConfig(
    dependencies=[
        Dependency("httpssl_periodicity_hist", offset=-datetime.timedelta(days=1)),
        Dependency("httpssl_periodicity", allow_missing=True),
    ],
    df_type="pandas",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def httpssl_periodicity_hist(domain_history_df, perdf, compute_info):
    if perdf is None:
        return domain_history_df

    if "server_name" in perdf.columns:
        perdf = perdf.rename(columns={"server_name": "host"})
    perdf = perdf[["host", "true_periods"]]
    perdf = perdf.loc[(perdf["true_periods"].map(len)>0)]
    print(perdf.shape)
    perdf = perdf.drop(columns=["true_periods"])
    res = gen_periodicity_history(domain_history_df, perdf, compute_info)
    
    return res
