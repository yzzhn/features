name = "ssh_conn"

import datetime

from generate_features.features import *
import pyspark.sql.functions as fn

# from generate_features.features.ssh_conn.ssh_conn_join import merge_ssh_conn


ssh_conn_deps = [
    Dependency("ssh", allow_missing = True),
    Dependency("conn", columns = ["uid", "duration", "orig_bytes", "orig_cc", 
                                  "orig_ip_bytes", "orig_pkts", "resp_bytes", 
                                  "resp_cc", "resp_ip_bytes", "resp_pkts",
                                  "service", "community_id", "conn_state", "missed_bytes"
                                 ], allow_missing = True),
]


@FeatureConfig(
    dependencies=ssh_conn_deps,
    df_type="spark",
    memoize=True,
    timescale=Timescales.HALF_HOUR.value,
    description="""
        Merge Conn log to SSH log based on uid
    """
)
def ssh_conn(ssh_df, conn_df, compute_info):
    if ssh_df is None or conn_df is None:
        print("No data available.")
        return None
    res = conn_df.join(fn.broadcast(ssh_df), on="uid", how="right").coalesce(42)
    return res