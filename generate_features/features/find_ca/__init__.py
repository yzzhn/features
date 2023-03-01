name = "find_ca"


from datetime import timedelta

import pyspark.sql.functions as f

from generate_features.features import *
from generate_features.features.popularity_host.popular_host import gen_popular_host
from generate_features.helpers.dataframe import concat_df_rows

#, ["anon_orig", "validation_status", "subject_CN", "issuer_O", "server_name"]

@FeatureConfig(
    dependencies=[Dependency("ssl", allow_missing=True)],
    df_type="spark",
    memoize=True,
    timescale=Timescales.ONE_DAY.value,
    description="""
        Filters out the SSL Logs to find connections that originate from UVA and whose
        destinations are servers that have valid signed certificates. From those, we will find
        all organization names of these certificate authorities. 
        """
)
def find_ca(ssl_log, compute_info):
    #If we can't find an SSL Log, return nothing
    if ssl_log is None:
        return None
    #Filter out connections that originate from UVA and are signed by a valid CA
    df_ok = ssl_log.filter((ssl_log["anon_orig"] == "uva") & (ssl_log["validation_status"] == "ok"))
    df_uva = df_ok.dropDuplicates(["subject_CN"])

    #Group by issuer organization
    df_ca = df_uva.groupBy('issuer_O').agg(f.count(f.col('server_name')).alias('count')).orderBy('count', ascending=False)
    return df_ca