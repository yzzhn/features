import os
import sys
import pandas as pd
import pyspark
import pyspark.sql.functions as fn
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from datetime import datetime, timedelta
from pyspark import SparkFiles
from pyspark.sql import Row, DataFrame
from pyspark.sql.functions import *
from functools import reduce

# TODO this should be turned into regular pyspark, does not need to be a UDF
def check_time(ts, rt):
    five_min = 5 * 60
    if ts != None and rt != None and ((ts - five_min) <= rt <= (ts + five_min)):
        return 1
    return 0


def uva_match(fireeye_df, zeek_df):
    # Drop these fields to make the merge easy - they overlap with Zeek fields
    fe_drop = fireeye_df.drop("_lpp_ver").drop("_lpp_plugin")
    # only need the IPs, anon fields, ports for match; can keep ts/uid to match back to conn

    # match from fe_src->zeek_orig & fe_dst->zeek_resp as well as fe_src->zeek_resp & fe_dst->zeek_orig
    # may be able to do this per FireEye event type to reduce computation
    direct_match = (
        fe_drop.withColumnRenamed("src", "id_orig_h")
        .withColumnRenamed("dst", "id_resp_h")
        .withColumnRenamed("spt", "id_orig_p")
        .withColumnRenamed("dpt", "id_resp_p")
        .withColumnRenamed("anon_src", "anon_orig")
        .withColumnRenamed("anon_dst", "anon_resp")
    )
    other_way = (
        fe_drop.withColumnRenamed("src", "id_resp_h")
        .withColumnRenamed("dst", "id_orig_h")
        .withColumnRenamed("spt", "id_resp_p")
        .withColumnRenamed("dpt", "id_orig_p")
        .withColumnRenamed("anon_src", "anon_resp")
        .withColumnRenamed("anon_dst", "anon_orig")
    )
    both_ways = direct_match.union(other_way)
    result = zeek_df.join(
        broadcast(both_ways),
        on=[
            "id_orig_h",
            "id_resp_h",
            "id_orig_p",
            "id_resp_p",
            "anon_orig",
            "anon_resp",
        ],
        how="inner",
    )

    # convert the timestamps into comparable forms - can probably be done better
    result = result.withColumn(
        "unix_rt", unix_timestamp("rt", "MMM dd yyyy HH:mm:ss z")
    )
    result = result.withColumn("unix_ts", col("ts"))

    # compare timestamps of fireeye and zeek logs; label if within 5 minutes
    time_udf = udf(check_time, IntegerType())
    result = (
        result.withColumn("fe_label", time_udf(result["unix_ts"], result["unix_rt"]))
        .drop("unix_rt")
        .drop("unix_ts")
        .dropDuplicates()
    )
    # only pick the exact matches; get rid of this line if you don't want to exclude the matches outside of the 5 minute span
    result = result.where(result["fe_label"] == 1)
    return result
