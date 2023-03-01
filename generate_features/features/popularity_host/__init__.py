name = "popularity_host"


from datetime import timedelta

import pyspark.sql.functions as f

from generate_features.features import *
from generate_features.features.popularity_host.popular_host import gen_popular_host
from generate_features.helpers.dataframe import concat_df_rows


@FeatureConfig(
    dependencies=[Dependency("http", allow_missing=True)],
    df_type="spark",
    memoize=True,
    timescale=Timescales.ONE_DAY.value,
    description="""
        Groups by hostname and computes features 
        based on the host popularity (such as 
        cnt_distinct_hosts, cnt_logs). 
    """,
)
def popularity_host(http_log, compute_info):
    df_lst = [http_log]
    if any([df is None for df in df_lst]):
        return None
    
    return gen_popular_host(http_log, fqdn_col="host")


@FeatureConfig(
    dependencies=[Dependency("http_vt", allow_missing=True)],
    df_type="spark",
    memoize=True,
    timescale=Timescales.ONE_DAY.value,
    description="""
        Groups by hostname and computes features 
        based on the host popularity (such as 
        cnt_distinct_hosts, cnt_logs). 
    """,
)
def popularity_host_vt(http_log, compute_info):
    df_lst = [http_log]
    if any([df is None for df in df_lst]):
        return None
    
    return gen_popular_host(http_log, fqdn_col="host")



@FeatureConfig(
    dependencies=[Dependency("ssl", allow_missing=True)],
    df_type="spark",
    memoize=True,
    timescale=Timescales.ONE_DAY.value,
    description="""
        Groups by hostname and computes features 
        based on the host popularity (such as 
        cnt_distinct_hosts, cnt_logs). 
    """,
)
def ssl_popularity_host(ssl_log, compute_info):
    df_lst = [ssl_log]
    if any([df is None for df in df_lst]):
        return None
    res = gen_popular_host(ssl_log, fqdn_col="server_name")
    return res.repartition(1)


@FeatureConfig(
    dependencies=[Dependency("ssl_vt", allow_missing=True)],
    df_type="spark",
    memoize=True,
    timescale=Timescales.ONE_DAY.value,
    description="""
        Groups by hostname and computes features 
        based on the host popularity (such as 
        cnt_distinct_hosts, cnt_logs). 
    """,
)
def ssl_popularity_host_vt(ssl_log, compute_info):
    df_lst = [ssl_log]
    if any([df is None for df in df_lst]):
        return None
    res = gen_popularity_host(ssl_log, fqdn_col="server_name")
    return res.repartition(1)


@FeatureConfig(
    dependencies=[Dependency("ssl", ["server_name", "id_orig_h", "id_resp_h"], allow_missing=True),
                  Dependency("http", ["host", "id_orig_h","id_resp_h"], allow_missing=True)],
    df_type="spark",
    memoize=True,
    timescale=Timescales.ONE_DAY.value,
    description="""
        Groups by hostname and computes features 
        based on the host popularity (such as 
        cnt_distinct_hosts, cnt_logs). 
    """,
)
def httpssl_popularity_host(ssl_log, http_log, compute_info):
    
    ssl_log = ssl_log.withColumnRenamed("server_name", "host")
    ssl_log = ssl_log.union(http_log)
    res = gen_popular_host(ssl_log, fqdn_col="host")
    return res.repartition(1)


@FeatureConfig(
    dependencies=[Dependency("ssl_vt", ["server_name", "id_orig_h","id_resp_h"], allow_missing=True),
                  Dependency("http_vt", ["host", "id_orig_h","id_resp_h"], allow_missing=True)],
    df_type="spark",
    memoize=True,
    timescale=Timescales.ONE_DAY.value,
    description="""
        Groups by hostname and computes features 
        based on the host popularity (such as 
        cnt_distinct_hosts, cnt_logs). 
    """,
)
def httpssl_popularity_host_vt(ssl_log, http_log, compute_info):
    
    ssl_log = ssl_log.withColumnRenamed("server_name", "host")
    ssl_log = ssl_log.union(http_log)
    res = gen_popular_host(ssl_log, fqdn_col="host")
    return res.repartition(1)







@FeatureConfig(
    dependencies=[
        Dependency("popularity_host", offset=timedelta(days=-i - 1)) for i in range(5)
    ],
    df_type="spark",
    memoize=True,
    timescale=Timescales.ONE_DAY.value,
)
def fiveday_popularity_host(*ph_dfs, compute_info):
    ph_dfs = [df for df in ph_dfs if df is not None]
    if not ph_dfs:
        return None

    for df in ph_dfs:
        print(str((df.count(), len(df.columns))))
    df = concat_df_rows(ph_dfs)
    print(str((df.count(), len(df.columns))))
    df = df.groupBy("host").agg(
        f.min("cnt_logs").alias("min5day_cnt_logs"),
        f.max("cnt_logs").alias("max5day_cnt_logs"),
        f.min("fqdn_norm").alias("min5day_fqdn_norm"),
        f.max("fqdn_norm").alias("max5day_fqdn_norm"),
        f.min("cnt_distinct_hosts").alias("min5day_cnt_distinct_hosts"),
        f.max("cnt_distinct_hosts").alias("max5day_cnt_distinct_hosts"),
    )

    # df = df.drop('fqdn_norm', 'cnt_distinct_hosts')

    print(str((df.count(), len(df.columns))))

    df.show(10)

    return df
