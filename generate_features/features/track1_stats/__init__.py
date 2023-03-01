name = "track1_time_series"

import datetime

import pyspark.sql.functions as fn
import pandas

from generate_features.generate_feats import GenerateFeatures
from generate_features.features.zeek_logs.file_io import ZeekRawLoader
from generate_features.helpers import *
from generate_features.features import *
from generate_features import available_features


@FeatureConfig(
    dependencies=[
        Dependency("http", ["host","id_resp_h","uid"], allow_missing=True)
    ],
    df_type="spark",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def http_cnt_respIP(http_df, compute_info):
    df_lst = [http_df] 
    if any([df is None for df in df_lst]):
        return None

    gdf = http_df.groupBy(["host"]).agg(fn.countDistinct(fn.col("id_resp_h")).alias("cnt_IP"))
    return gdf


@FeatureConfig(
    dependencies=[
        Dependency("http", ["host","id_resp_h","uid"], allow_missing=True)
    ],
    df_type="spark",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def http_fqdn_perIP(http_df, compute_info):
    df_lst = [http_df] 
    if any([df is None for df in df_lst]):
        return None

    gdf = http_df.groupBy(["id_resp_h"]).agg(fn.countDistinct(fn.col("host")).alias("cnt_server"))
    return gdf


@FeatureConfig(
    dependencies=[
        Dependency("http_vt", ["host","id_resp_h","uid"], allow_missing=True)
    ],
    df_type="spark",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def vt_http_cnt_respIP(http_df, compute_info):
    df_lst = [http_df] 
    if any([df is None for df in df_lst]):
        return None

    gdf = http_df.groupBy(["host"]).agg(fn.countDistinct(fn.col("id_resp_h")).alias("cnt_IP"))
    return gdf


@FeatureConfig(
    dependencies=[
        Dependency("http_vt", ["host","id_resp_h","uid"], allow_missing=True)
    ],
    df_type="spark",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def vt_http_fqdn_perIP(http_df, compute_info):
    df_lst = [http_df] 
    if any([df is None for df in df_lst]):
        return None

    gdf = http_df.groupBy(["id_resp_h"]).agg(fn.countDistinct(fn.col("host")).alias("cnt_server"))
    return gdf



@FeatureConfig(
    dependencies=[
        Dependency("ssl", ["server_name","id_resp_h","uid"], allow_missing=True)
    ],
    df_type="spark",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def ssl_cnt_respIP(ssl_df, compute_info):
    df_lst = [ssl_df] 
    if any([df is None for df in df_lst]):
        return None

    gdf = ssl_df.groupBy(["server_name"]).agg(fn.countDistinct(fn.col("id_resp_h")).alias("cnt_IP"))
    return gdf



@FeatureConfig(
    dependencies=[
        Dependency("ssl", ["server_name","id_resp_h","uid"], allow_missing=True)
    ],
    df_type="spark",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def ssl_fqdn_perIP(ssl_df, compute_info):
    df_lst = [ssl_df] 
    if any([df is None for df in df_lst]):
        return None

    gdf = ssl_df.groupBy(["id_resp_h"]).agg(fn.countDistinct(fn.col("server_name")).alias("cnt_server"))
    return gdf

@FeatureConfig(
    dependencies=[
        Dependency("ssl_vt", ["server_name","id_resp_h","uid"], allow_missing=True)
    ],
    df_type="spark",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def vt_ssl_cnt_respIP(ssl_df, compute_info):
    df_lst = [ssl_df] 
    if any([df is None for df in df_lst]):
        return None

    gdf = ssl_df.groupBy(["server_name"]).agg(fn.countDistinct(fn.col("id_resp_h")).alias("cnt_IP"))
    return gdf



@FeatureConfig(
    dependencies=[
        Dependency("ssl_vt", ["server_name","id_resp_h","uid"], allow_missing=True)
    ],
    df_type="spark",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def vt_ssl_fqdn_perIP(ssl_df, compute_info):
    df_lst = [ssl_df] 
    if any([df is None for df in df_lst]):
        return None

    gdf = ssl_df.groupBy(["id_resp_h"]).agg(fn.countDistinct(fn.col("server_name")).alias("cnt_server"))
    return gdf


###########tmp##################

@FeatureConfig(
    dependencies=[
        Dependency("ssl",  allow_missing=True),
        Dependency("conn", ["uid", "proto", "service", "duration", "conn_state", "history"], allow_missing=True)
    ],
    df_type="spark",
    timescale=Timescales.HALF_HOUR.value,
    memoize=True,
)
def conn_ssl(ssl_df, conn_df, compute_info):

    if ssl_df is None and conn_df is None:
        return None

    res = ssl_df.join(conn_df, on="uid", how="left")
    return res

@FeatureConfig(
    dependencies=[
        Dependency("http",  allow_missing=True),
        Dependency("conn", ["uid", "proto", "service", "duration", "conn_state", "history"], allow_missing=True)
    ],
    df_type="spark",
    timescale=Timescales.HALF_HOUR.value,
    memoize=True,
)
def conn_http(http_df, conn_df, compute_info):

    if http_df is None and conn_df is None:
        return None

    res = http_df.join(conn_df, on="uid", how="left")
    return res
