name = "popularity_uri"

import datetime

from pyspark.sql.functions import explode
import pyspark.sql.functions as fn
from pyspark.sql.types import StringType
from pyspark.sql.types import ArrayType
from pyspark.sql.functions import col

from datetime import timedelta

import pyspark.sql.functions as f

from generate_features.helpers.dataframe import concat_df_rows


from generate_features.features import *
from generate_features.features.popularity_uri.popular_uri import (
    gen_popular_ip_uri,
    gen_popular_log_uri,
    uri_split_udf,
)


@FeatureConfig(
    dependencies=[Dependency("http", ["id_orig_h", "uri"], allow_missing=True)],
    df_type="spark",
    memoize=True,
    timescale=Timescales.ONE_DAY.value,
)
def popularity_uri(df, compute_info):
    df_lst = [df]
    if any([df is None for df in df_lst]):
        return None

    df = df.withColumn("uri_split", uri_split_udf(df.uri))
    df = df.withColumn("uri_token", explode(df.uri_split))
    df = df.drop("uri_split")

    df1 = gen_popular_ip_uri(df)
    df2 = gen_popular_log_uri(df)
    pop_uri_df = df1.join(df2, ["uri_token"])

    """ Find the token in the http uri with the highest
        frequency, assign that frequency to the entire unique uri
        (purpose is to make joins/filtering easier).
    """

    res = df.join(pop_uri_df, ["uri_token"])
    res = res.dropDuplicates(["uri", "uri_token"])

    res = res.groupby("uri").agg(
        fn.max(res.cnt_distinct_ip_uri_token).alias("max_cnt_distinct_ip_uri_token"),
        fn.max(res.cnt_log_uri_token).alias("max_cnt_log_uri_token"),
        fn.max(res.ip_uri_token_freq).alias("max_ip_uri_token_freq"),
        fn.max(res.log_uri_token_freq).alias("max_log_uri_token_freq"),
    )

    return res.coalesce(42)


@FeatureConfig(
    dependencies=[
        Dependency("popularity_uri", offset=timedelta(days=-i - 1)) for i in range(5)
    ],
    df_type="spark",
    memoize=True,
    timescale=Timescales.ONE_DAY.value,
)
def fiveday_popularity_uri(*ph_dfs, compute_info):
    ph_dfs = [df for df in ph_dfs if df is not None]
    if not ph_dfs:
        return None

    for df in ph_dfs:
        print(str((df.count(), len(df.columns))))
    df = concat_df_rows(ph_dfs)

    print(str((df.count(), len(df.columns))))
    df = df.groupBy("uri").agg(
        f.min("max_cnt_distinct_ip_uri_token").alias(
            "min5day_max_cnt_distinct_ip_uri_token"
        ),
        f.max("max_cnt_distinct_ip_uri_token").alias(
            "max5day_max_cnt_distinct_ip_uri_token"
        ),
        f.min("max_cnt_log_uri_token").alias("min5day_max_cnt_log_uri_token"),
        f.max("max_cnt_log_uri_token").alias("max5day_max_cnt_log_uri_token"),
        f.min("max_ip_uri_token_freq").alias("min5day_max_ip_uri_token_freq"),
        f.max("max_ip_uri_token_freq").alias("max5day_max_ip_uri_token_freq"),
        f.min("max_log_uri_token_freq").alias("min5day_max_log_uri_token_freq"),
        f.max("max_log_uri_token_freq").alias("max5day_max_log_uri_token_freq"),
    )

    # df = df.drop('max_cnt_distinct_ip_uri_token', 'max_cnt_log_uri_token',
    #                'max_ip_uri_token_freq', 'max_log_uri_token_freq')

    print(str((df.count(), len(df.columns))))

    df.show(10)

    return df
