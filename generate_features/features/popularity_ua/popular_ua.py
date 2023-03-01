import pyspark
import pyspark.sql.functions as fn


def gen_popular_ua(df):
    df = df.filter(df.user_agent.isNotNull())  # drop null UA

    res = df.groupby("user_agent").agg(
        fn.countDistinct(df.id_orig_h).alias("cnt_distinct_hosts")
    )
    total_host = df.select("id_orig_h").dropDuplicates().count()
    res = res.withColumn("ua_norm", res.cnt_distinct_hosts / total_host)
    res.repartition(1)

    return res
