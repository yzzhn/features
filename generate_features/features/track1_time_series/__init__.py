name = "track1_time_series"

import datetime

import pyspark.sql.functions as fn
import pandas

from generate_features.generate_feats import GenerateFeatures
from generate_features.helpers import *
from generate_features.features import *
from generate_features import available_features

from generate_features.features.track1_time_series.preprocess  import *
from generate_features.features.track1_time_series.build_ts  import *
from generate_features.features.track1_time_series.periodicity_detection  import *


#################### Time Series Recreation #######################

###### HTTP ######

@FeatureConfig(
    dependencies=[
        Dependency("http", ["uid", "ts", "id_resp_h", "host"], allow_missing=True)
    ],
    df_type="spark",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def http_ts(http_df, compute_info):
    if http_df is None:
        return None
    gdf = build_ts(http_df, compute_info, fqdn_col = "host")
    return gdf

@FeatureConfig(
    dependencies=[
        Dependency("http_vt", ["uid", "ts", "id_resp_h", "host"], allow_missing=True)
    ],
    df_type="spark",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def http_ts_vt(http_df, compute_info):
    if http_df is None:
        return None
    gdf = build_ts(http_df, compute_info, fqdn_col = "host")
    return gdf

###### SSL ######
@FeatureConfig(
    dependencies=[
        Dependency("ssl", ["uid", "ts", "id_resp_h", "server_name"], allow_missing=True)
    ],
    df_type="spark",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def ssl_ts(ssl_df, compute_info):
    if ssl_df is None:
        return None
    gdf = build_ts(ssl_df, compute_info, fqdn_col = "server_name")
    return gdf



###### SSL ######
@FeatureConfig(
    dependencies=[
        Dependency("ssl", ["uid", "ts", "id_resp_h", "server_name"], allow_missing=True)
    ],
    df_type="spark",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def ssl_ts_tmp(ssl_df, compute_info):
    if ssl_df is None:
        return None
    gdf = build_ts(ssl_df, compute_info, fqdn_col = "server_name")
    return gdf

@FeatureConfig(
    dependencies=[
        Dependency("ssl_vt", ["uid", "ts", "id_resp_h", "server_name"], allow_missing=True)
    ],
    df_type="spark",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def ssl_ts_vt(ssl_df, compute_info):
    if ssl_df is None:
        return None
    gdf = build_ts(ssl_df, compute_info, fqdn_col = "server_name")
    return gdf

##### HTTP + SSL #####
@FeatureConfig(
    dependencies=[
        Dependency("http_ts", allow_missing=True),
        Dependency("ssl_ts", allow_missing=True),
    ],
    df_type="pandas",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def httpssl_ts(httpts, sslts, compute_info):
    if sslts is None:
        return httpts

    if httpts is None:
        return sslts

    if sslts is None and httpts is None:
        return None
    
    print("httpts columns:", httpts.columns)
    print("sslts columns:", sslts.columns)
    gdf = merge_httpssl_ts(httpts, sslts)
    
    return gdf

@FeatureConfig(
    dependencies=[
        Dependency("http_ts_vt", allow_missing=True),
        Dependency("ssl_ts_vt", allow_missing=True),
    ],
    df_type="pandas",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def httpssl_ts_vt(httpts, sslts, compute_info):
    if sslts is None:
        return httpts
        
    if httpts is None:
        return sslts
    
    if sslts is None and httpts is None:
        return None

    gdf = merge_httpssl_ts(httpts, sslts)
    
    return gdf

#################### Periodicity Detection #######################

###### HTTP ######

@FeatureConfig(
    dependencies=[
        Dependency("http_ts", allow_missing=False)
    ],
    df_type="pandas",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def http_periodicity(http_df, compute_info):
    if http_df is None:
        return None
    res = mltproc_detection(http_df)
    return res


@FeatureConfig(
    dependencies=[
        Dependency("http_ts_vt", allow_missing=False)
    ],
    df_type="pandas",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def http_periodicity_vt(http_df, compute_info):
    if http_df is None:
        return None
    res = mltproc_detection(http_df)
    return res


@FeatureConfig(
    dependencies=[
        Dependency("http_ts", allow_missing=False),
        Dependency("http_ts_vt", allow_missing=False)
    ],
    df_type="pandas",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def http_periodicity_agg(uvadf, vtdf, compute_info):
    if uvadf is None or vtdf is None:
        return None
    uvadf = uvadf.set_index("host").add_suffix("_uva")
    vtdf = vtdf.set_index("host").add_suffix("_vt")
    aggdf = uvadf.join(vtdf, how="inner")
    print("Total FQDNs visited by both UVA and VT:", aggdf.shape)
    aggdf["tdf"] = aggdf["tdf_uva"] + aggdf["tdf_vt"]
    aggdf["ts_cnt"] = aggdf["ts_cnt_uva"] + aggdf["ts_cnt_vt"]
    aggdf = aggdf.reset_index()
    res = mltproc_detection(aggdf)
    return res

###### SSL ######

@FeatureConfig(
    dependencies=[
        Dependency("ssl_ts", allow_missing=False)
    ],
    df_type="pandas",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def ssl_periodicity(ssl_df, compute_info):
    if ssl_df is None:
        return None
    res = mltproc_detection(ssl_df)
    return res


@FeatureConfig(
    dependencies=[
        Dependency("ssl_ts_vt", allow_missing=False)
    ],
    df_type="pandas",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def ssl_periodicity_vt(ssl_df, compute_info):
    if ssl_df is None:
        return None
    res = mltproc_detection(ssl_df)
    return res


@FeatureConfig(
    dependencies=[
        Dependency("ssl_ts", allow_missing=False),
        Dependency("ssl_ts_vt", allow_missing=False)
    ],
    df_type="pandas",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def ssl_periodicity_agg(uvadf, vtdf, compute_info):
    if uvadf is None or vtdf is None:
        return None
    uvadf = uvadf.set_index("server_name").add_suffix("_uva")
    vtdf = vtdf.set_index("server_name").add_suffix("_vt")
    aggdf = uvadf.join(vtdf, how="inner")
    print("Total FQDNs visited by both UVA and VT:", aggdf.shape)
    aggdf["tdf"] = aggdf["tdf_uva"] + aggdf["tdf_vt"]
    aggdf["ts_cnt"] = aggdf["ts_cnt_uva"] + aggdf["ts_cnt_vt"]
    aggdf = aggdf.reset_index()
    res = mltproc_detection(aggdf)
    return res



##### HTTP + SSL Periodicity #####
@FeatureConfig(
    dependencies=[
        Dependency("httpssl_ts", allow_missing=False)
    ],
    df_type="pandas",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def httpssl_periodicity(tsdf, compute_info):
    if tsdf is None:
        return None
    tsdf = tsdf.loc[tsdf["ts_cnt"]>0]
    res = mltproc_detection(tsdf)
    return res


@FeatureConfig(
    dependencies=[
        Dependency("httpssl_ts_vt", allow_missing=False)
    ],
    df_type="pandas",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def httpssl_periodicity_vt(tsdf, compute_info):
    if tsdf is None:
        return None
    res = mltproc_detection(tsdf)
    return res


@FeatureConfig(
    dependencies=[
        Dependency("httpssl_ts", allow_missing=False),
        Dependency("httpssl_ts_vt", allow_missing=False)
    ],
    df_type="pandas",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def httpssl_periodicity_agg(uvadf, vtdf, compute_info):
    if uvadf is None or vtdf is None:
        return None
    uvadf = uvadf.set_index("host").add_suffix("_uva")
    vtdf = vtdf.set_index("host").add_suffix("_vt")
    aggdf = uvadf.join(vtdf, how="inner")
    print("Total FQDNs visited by both UVA and VT:", aggdf.shape)
    aggdf["tdf"] = aggdf["tdf_uva"] + aggdf["tdf_vt"]
    aggdf["ts_cnt"] = aggdf["ts_cnt_uva"] + aggdf["ts_cnt_vt"]
    aggdf = aggdf.reset_index()
    res = mltproc_detection(aggdf)
    return res