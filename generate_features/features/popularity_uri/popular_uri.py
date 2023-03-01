import pyspark
import pyspark.sql.functions as fn
from pyspark.sql.types import StringType
from pyspark.sql.types import ArrayType
from pyspark.sql.functions import col


def uri_split(uri_string):
    return uri_string.split("/")


uri_split_udf = fn.udf(lambda x: uri_split(x), ArrayType(StringType()))


def gen_popular_ip_uri(df):
    df = df.filter(df.uri.isNotNull())
    df = df.where(col("uri_token") != "")
    res = df.groupby("uri_token").agg(
        fn.countDistinct(df.id_orig_h).alias("cnt_distinct_ip_uri_token")
    )
    total_uri = df.select("id_orig_h").dropDuplicates().count()
    res = res.withColumn("ip_uri_token_freq", res.cnt_distinct_ip_uri_token / total_uri)
    res.repartition(1)
    return res


def gen_popular_log_uri(df):
    df = df.filter(df.uri.isNotNull())
    df = df.where(col("uri_token") != "")
    res = df.groupby("uri_token").agg(fn.count(fn.lit(1)).alias("cnt_log_uri_token"))
    total_logs = df.count()
    res = res.withColumn("log_uri_token_freq", res.cnt_log_uri_token / total_logs)
    res.repartition(1)
    return res
