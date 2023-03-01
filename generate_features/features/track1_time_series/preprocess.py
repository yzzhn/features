import os
import sys
import pandas as pd
import numpy as np
import datetime
import pyspark
import pyspark.sql.functions as fn
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, ArrayType
from pyspark.sql import Row, DataFrame


def fix_missing_fqdn(zeek_df, fqdn_col="host"):
    # solve missing FQDNs 
    zeek_df = zeek_df.withColumn(fqdn_col, fn.when((fn.col(fqdn_col)=="missing")|(fn.col(fqdn_col)==""), 
                                 fn.concat(fn.lit("missing_"), fn.col("id_resp_h"))).otherwise(fn.col(fqdn_col)))
    return zeek_df

## convert timestamp from UTC to ETC based on log date (only valid for YEAR 2020)
## for missing FQDN, convert FQDN as missing_{resp_IP}.
def fix_dftime_utc2etc(zeek_df, logdate, 
                  summertime_date = datetime.datetime(2020, 3, 8), 
                  wintertime_date = datetime.datetime(2020, 11, 1), 
                  mute=False):
    
    #logdate = datetime.datetime.strptime(logday, "%Y-%m-%d")
    if logdate < summertime_date or (logdate >= wintertime_date and logdate <= datetime.datetime(2021,3,14)):
        if not mute:        print("WINTER DaylMemoryErroright:", logdate.strftime("%Y-%m-%d"))
        zeek_df = zeek_df.withColumn("ts", fn.unix_timestamp(fn.col("ts").cast("timestamp") - fn.expr("INTERVAL 5 HOURS")))
    elif logdate >= summertime_date and logdate < wintertime_date or logdate > datetime.datetime(2021,3,14):
        if not mute:        print("SUMMER Daylight:", logdate.strftime("%Y-%m-%d"))
        zeek_df = zeek_df.withColumn("ts", fn.unix_timestamp(fn.col("ts").cast("timestamp") - fn.expr("INTERVAL 4 HOURS")))
    else:
        print("logdate error.")
        raise ValueError
    
    return zeek_df
