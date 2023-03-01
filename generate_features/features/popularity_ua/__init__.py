name = "popularity_ua"


from datetime import timedelta

import pyspark.sql.functions as f

from generate_features.features import *
from generate_features.features.popularity_ua.popular_ua import gen_popular_ua
from generate_features.helpers.dataframe import concat_df_rows


@FeatureConfig(
    dependencies=[Dependency("http", ["id_orig_h", "user_agent"], allow_missing=True)],
    df_type="spark",
    memoize=True,
    timescale=Timescales.ONE_DAY.value,
)
def popularity_ua(http_log, compute_info):
    df_lst = [http_log]
    if any([df is None for df in df_lst]):
        return None

    return gen_popular_ua(http_log)


@FeatureConfig(
    dependencies=[
        Dependency("popularity_ua", offset=timedelta(days=-i - 1)) for i in range(5)
    ],
    df_type="spark",
    memoize=True,
    timescale=Timescales.ONE_DAY.value,
)
def fiveday_popularity_ua(*ph_dfs, compute_info):
    ph_dfs = [df for df in ph_dfs if df is not None]
    if not ph_dfs:
        return None

    for df in ph_dfs:
        print(str((df.count(), len(df.columns))))
    df = concat_df_rows(ph_dfs)
    print(str((df.count(), len(df.columns))))
    df = df.groupBy("user_agent").agg(
        f.min("ua_norm").alias("min5day_ua_norm"),
        f.max("ua_norm").alias("max5day_ua_norm"),
        f.min("cnt_distinct_hosts").alias("min5day_cnt_distinct_hosts"),
        f.max("cnt_distinct_hosts").alias("max5day_cnt_distinct_hosts"),
    )

    # df = df.drop('ua_norm', 'cnt_distinct_hosts')

    print(str((df.count(), len(df.columns))))

    df.show(10)

    return df
