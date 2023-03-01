import pyspark
import pyspark.sql.functions as fn

def fix_missing_fqdn(zeek_df, fqdn_col="host"):
    # solve missing FQDNs 
    zeek_df = zeek_df.withColumn(fqdn_col, fn.when((fn.col(fqdn_col)=="missing")|(fn.col(fqdn_col)==""), 
                                 fn.concat(fn.lit("missing_"), fn.col("id_resp_h"))).otherwise(fn.col(fqdn_col)))
    return zeek_df

def gen_popular_host(df, fqdn_col="host"):
    target_cols = ["id_orig_h", fqdn_col]
    print("Beginning accumulation/aggregation phase:")
    
    df = fix_missing_fqdn(df, fqdn_col)
    df = df.filter(fn.col(fqdn_col).isNotNull())
    res = df.groupby(fqdn_col).agg(
        fn.countDistinct(df.id_orig_h).alias("cnt_distinct_hosts"),
        fn.count(df.id_orig_h).alias("cnt_logs"),
    )
    
    total_host = df.select("id_orig_h").dropDuplicates().count()
    res = res.withColumn("fqdn_norm", res.cnt_distinct_hosts / total_host)
    res.repartition(1)
    return res