name = "ssh_bf_succ"

import datetime

from generate_features.features import *
from generate_features.features.ssh_bf_succ.honeypot_match import honeypot_match


@FeatureConfig(
    dependencies=[Dependency("ssh", allow_missing=True),],
    df_type="spark",
    memoize=True,
    timescale=Timescales.ONE_DAY.value,
    description="""
        One DAY of successful SSH brute force attack logs
    """,
)
def ssh_bf_succ(zeek_df, compute_info):
    if zeek_df is None:
        return None
    
    threshold = 3
    if "auth_success" in zeek_df.columns:
        return zeek_df.where(zeek_df.auth_attempts > threshold).where(zeek_df.auth_success == "1")
    else: # tmp fix to avoid missing fields
        return zeek_df.where(zeek_df.auth_attempts > 9999999)



@FeatureConfig(
    dependencies=[Dependency("ssh_bf_succ", allow_missing = True),
                  Dependency("honeypot", allow_missing = False)],
    df_type="pandas",
    memoize=True,
    timescale=Timescales.ONE_DAY.value,
    description="""
        One DAY of successful SSH brute force attack logs that match honeypot blacklist
    """,
)
def ssh_bf_succ_honeypot(zeek_df, honeypot_df, compute_info):
    if zeek_df is None:
        return None
    
    return honeypot_match(zeek_df, honeypot_df)


@FeatureConfig(
    dependencies=[Dependency("ssh_bf_succ", allow_missing = True),
                  Dependency("honeypot", offset=+datetime.timedelta(days=1), allow_missing = False)],
    df_type="pandas",
    memoize=True,
    timescale=Timescales.ONE_DAY.value,
    description="""
        One DAY of successful SSH brute force attack logs that match (day+1) honeypot blacklist
    """,
)
def ssh_bf_succ_honeypot_retro1(zeek_df, honeypot_df, compute_info):
    if zeek_df is None:
        return None
    return honeypot_match(zeek_df, honeypot_df)


@FeatureConfig(
    dependencies=[Dependency("ssh_bf_succ", allow_missing=True),
                  Dependency("honeypot", offset=+datetime.timedelta(days=2), allow_missing = False)],
    df_type="pandas",
    memoize=False,
    timescale=Timescales.ONE_DAY.value,
    description="""
        One DAY of successful SSH brute force attack logs that match (day+2) honeypot blacklist
    """,
)
def ssh_bf_succ_honeypot_retro2(zeek_df, honeypot_df, compute_info):
    if zeek_df is None:
        return None
    return honeypot_match(zeek_df, honeypot_df)
