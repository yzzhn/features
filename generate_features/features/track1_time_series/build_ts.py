import os
import sys
import pandas as pd
import numpy as np
import datetime
import pyspark
import pyspark.sql.functions as fn
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, ArrayType
from pyspark.sql import Row, DataFrame
from generate_features.features.track1_time_series.preprocess import *


## TODO: this should be able to parse  sample_freq to correct datapoints 
def resample_tslist(ts_list, logday, sample_freq='1T', datapoints = 1440):
    tmp = pd.DataFrame(columns=["ts"], data = ts_list)
    tmp["ts"] = pd.to_datetime(tmp["ts"], unit='s')
    tmp['cnt'] = 1
    tmp = tmp.set_index("ts")
    tmp = tmp.resample(sample_freq).sum()
    # resample to one day
    ts_idx = pd.date_range(datetime.datetime.strptime(logday, "%Y-%m-%d"), periods = datapoints, freq = sample_freq)
    tsnew = pd.DataFrame(columns=["ts"], data=ts_idx)
    tsnew = tsnew.set_index("ts")
    tmp = tsnew.join(tmp).fillna(0).astype(int)
    
    return tmp['cnt'].values.tolist()

resample_tslist_udf = fn.udf(resample_tslist, ArrayType(IntegerType()))

def total_connections(ts_list):
    tmp = np.array(ts_list)
    return len(tmp[tmp>0])

total_connections_udf = fn.udf(total_connections, IntegerType())


def build_ts(http_df, compute_info, fqdn_col="host"):
    # date of the raw logs
    logdate = compute_info["start_dt"]
    
    # preprocess: if hostname =="missing", hostname = "missing_{id_resp_h}"
    http_df = fix_missing_fqdn(http_df, fqdn_col)
    
    # preprocess: raw log ts to ETC timestamp based on logdate
    # Summer Daylight: UTC - 5 hours; Winter Daylight UTC - 4 hours
    http_df = fix_dftime_utc2etc(http_df, logdate)
    
    # one http connnection can have multiple http logs
    # for each (uid, id_resp_h, fqdn) pair, find its first time stamp, drop the others
    http_df = http_df.groupBy(["uid", "id_resp_h", fqdn_col]).agg(fn.min("ts").alias("ts"))
    
    # resample ts to 1440 (1-day) datapoints for periodicity connection
    gdf = http_df.groupby(fqdn_col).agg(fn.collect_list(fn.col("ts")).alias("ts_list"))
    gdf = gdf.withColumn("tdf", resample_tslist_udf(fn.col("ts_list"), fn.lit(logdate.strftime("%Y-%m-%d"))))
    gdf = gdf.withColumn("ts_cnt", total_connections_udf(fn.col("tdf")))
    gdf = gdf.select([fqdn_col, "tdf", "ts_cnt"])
    gdf = gdf.repartition(1)

    return gdf


def agg_httpssl_ts(http_ts, ssl_ts):
    
    if isinstance(http_ts, float):
        return ssl_ts
    if isinstance(ssl_ts, float):
        return http_ts
    return http_ts + ssl_ts


def merge_httpssl_ts(httpts, sslts):
    # date of the raw logs
    httpts = httpts.set_index("host").add_suffix("_http")
    sslts = sslts.rename(columns={'server_name': 'host'}).set_index("host").add_suffix("_ssl")
    aggdf = httpts.join(sslts, how="outer")

    print("Total FQDNs visited by both HTTP and SSL:", aggdf.shape)
    print(aggdf.columns)
    aggdf["tdf"]= aggdf.apply(lambda x: agg_httpssl_ts(x["tdf_http"], x["tdf_ssl"]), axis=1)
    aggdf = aggdf.reset_index()
    
    
    aggdf['ts_cnt'] = aggdf['tdf'].apply(lambda x: total_connections(x))
    return aggdf