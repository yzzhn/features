name = "http_fireeye_match"

import datetime

from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf, col

from generate_features.features import *
from generate_features.features.fireeye_match.fireeye_match import uva_match


@FeatureConfig(
    dependencies=[
        Dependency("fireeye", allow_missing=True),
        Dependency("http", ["id_orig_h", "id_resp_h", "id_orig_p", "id_resp_p", "anon_orig", "anon_resp", "ts", "uid", "host"], allow_missing=True),
    ],
    df_type="spark",
    memoize=True,
    timescale=Timescales.ONE_DAY.value,
)
def http_fireeye_match(fireeye_df, zeek_df, compute_info):
    df_lst = [fireeye_df, zeek_df]
    if any([df is None for df in df_lst]):
        return None
    return uva_match(fireeye_df, zeek_df)


@FeatureConfig(
    dependencies=[
        Dependency("fireeye", allow_missing=True),
        Dependency("ssl", ["id_orig_h", "id_resp_h", "id_orig_p", "id_resp_p", "anon_orig", "anon_resp", "ts", "uid", "server_name"], allow_missing=True),
    ],
    df_type="spark",
    memoize=True,
    timescale=Timescales.ONE_DAY.value,
)
def ssl_fireeye_match(fireeye_df, zeek_df, compute_info):
    df_lst = [fireeye_df, zeek_df]
    if any([df is None for df in df_lst]):
        return None
    return uva_match(fireeye_df, zeek_df)



def fe_event_to_score(event_name):
    if event_name == "malware-callback":
        return 1.0
    elif event_name == "malware-object":
        return 0.01
    elif event_name == "infection-match":
        return 0.009
    elif event_name == "ips-event":
        return 0.008
    else:
        return 0.0


fe_event_to_score_udf = udf(fe_event_to_score, DoubleType())


@FeatureConfig(
    dependencies=[
        Dependency(
            "http_fireeye_match", columns=["host", "eventName"], allow_missing=True
        ),
    ],
    df_type="spark",
    memoize=True,
    timescale=Timescales.ONE_MONTH.value,
)
def fireeye_score_v1(fireeye_df, compute_info):
    df_lst = [fireeye_df]
    if any([df is None for df in df_lst]):
        return None

    fireeye_df = fireeye_df.where(
        (col("eventName") == "malware-callback")
        | (col("eventName") == "malware-object")
        | (col("eventName") == "infection-match")
        | (col("eventName") == "ips-event")
    )

    fe_df = fireeye_df.select(["host", "eventName"])
    fe_df = fe_df.select(col("host"), col("eventName"))
    fe_df = fe_df.withColumn("fe_score", fe_event_to_score_udf(fe_df["eventName"]))
    fe_df = fe_df.drop("eventName")

    return fe_df



@FeatureConfig(
    dependencies=[
        Dependency(
            "http_fireeye_match", columns=["host", "eventName"], allow_missing=True
        ),
    ],
    df_type="spark",
    memoize=True,
    timescale=Timescales.ONE_DAY.value,
)
def fireeye_score_v2(fireeye_df, compute_info):
    df_lst = [fireeye_df]
    if any([df is None for df in df_lst]):
        return None

    fireeye_df = fireeye_df.where(
        (col("eventName") == "malware-callback")
        | (col("eventName") == "malware-object")
        | (col("eventName") == "infection-match")
        | (col("eventName") == "ips-event")
    )

    fe_df = fireeye_df.select(["host", "eventName"])
    fe_df = fe_df.select(col("host"), col("eventName"))
    fe_df = fe_df.withColumn("fe_score", fe_event_to_score_udf(fe_df["eventName"]))
    fe_df = fe_df.drop("eventName")

    return fe_df


