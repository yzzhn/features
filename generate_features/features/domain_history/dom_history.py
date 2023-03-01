import math
import datetime

import pandas as pd
import numpy as np
import ipaddress


def check_age(row, date, aging_out_cond=datetime.timedelta(days=30)):
    row_lastseen_dt = datetime.datetime.strptime(row["lastseen_date"], "%Y-%m-%d")
    row_date_dt = datetime.datetime.strptime(date, "%Y-%m-%d")
    return row_date_dt - row_lastseen_dt < aging_out_cond

def compute_days_since(date, current_date):
    date = datetime.datetime.strptime(date, "%Y-%m-%d")
    current_date = datetime.datetime.strptime(current_date, "%Y-%m-%d")
    return (current_date - date).days


def isIP(dom_text):
    try:
        _ = ipaddress.ip_address(dom_text)
        return 1
    except:
        return 0

def fix_missing_fqdn(zeek_df, fqdn_col="host", anoncol = "anon_host"):
    zeek_missing = zeek_df.loc[(zeek_df[fqdn_col]=="missing")|(zeek_df[fqdn_col]=="")]
    zeek_missing['fqdn'] = zeek_missing.apply(lambda x: ("missing_" + x['id_resp_h']), axis=1)
    zeek_missing = zeek_missing.drop(columns=[fqdn_col, "id_resp_h"]).rename(columns={"fqdn": fqdn_col})
    zeek_missing = zeek_missing.drop_duplicates()
    
    zeek_sub = zeek_df.loc[(zeek_df[fqdn_col]!="missing")&(zeek_df[fqdn_col]!="")][[fqdn_col, anoncol]].drop_duplicates()
    
    
    res = pd.concat([zeek_sub, zeek_missing], ignore_index=True)

    return res     


    
def gen_domain_history(hist_df, zeek_df, compute_info, logtyp="HTTP"):
    
    if logtyp == "HTTP":
        fqdncol = "host"
        anoncol = "anon_host"
    elif logtyp == "SSL":
        fqdncol = "server_name"
        anoncol = "anon_resp"
    else:
        raise ValueError
    
    zeek_df = fix_missing_fqdn(zeek_df, fqdncol)
    
    start_dt = compute_info.get("start_dt")
    start_date = start_dt.strftime("%Y-%m-%d")

    zeek_df["temp"] = 1
    
    df = pd.merge(hist_df, zeek_df, on=[fqdncol, anoncol], how="outer")
    df.loc[df["count_since_firstseen"].isnull(), "count_since_firstseen"] = 0
    df.loc[df["firstseen_date"].isnull(), "firstseen_date"] = start_date
    df.loc[df["lastseen_date"].isnull(), "lastseen_date"] = start_date
    df.loc[(df["temp"].notnull()) & (df["lastseen_date"].notnull()), "lastseen_date"] = start_date
    
    df["firstseen_log_type"] = logtyp
    df["lastseen_log_type"] = logtyp

    df["drop"] = df.apply(check_age, axis=1, args=([start_date]))
    df = df.loc[df["drop"]]
    
    df.loc[df[fqdncol].isin(zeek_df[fqdncol]), "count_since_firstseen"] += 1

    df = df.drop(labels=["temp", "drop"], axis=1)

    df["days_since_firstseen"] = df["firstseen_date"].apply(compute_days_since, args=([start_date]))
    df["days_since_lastseen"] = df["lastseen_date"].apply(compute_days_since, args=([start_date]))

    df.loc[df["isIP"].isnull(), "isIP"] = df.loc[df["isIP"].isnull()][fqdncol].apply(lambda x: isIP(x))
    
    return df



    
def gen_periodicity_history(hist_df, zeek_df, compute_info, logtyp="HTTP"):
    
    if logtyp == "HTTP":
        fqdncol = "host"
    elif logtyp == "SSL":
        fqdncol = "server_name"
    else:
        raise ValueError
    
    #zeek_df = fix_missing_fqdn(zeek_df, fqdncol)
    
    start_dt = compute_info.get("start_dt")
    start_date = start_dt.strftime("%Y-%m-%d")

    zeek_df["temp"] = 1
    
    df = pd.merge(hist_df, zeek_df, on=[fqdncol], how="outer")
    df.loc[df["count_since_firstseen"].isnull(), "count_since_firstseen"] = 0
    df.loc[df["firstseen_date"].isnull(), "firstseen_date"] = start_date
    df.loc[df["lastseen_date"].isnull(), "lastseen_date"] = start_date
    df.loc[(df["temp"].notnull()) & (df["lastseen_date"].notnull()), "lastseen_date"] = start_date
    
    #df["firstseen_log_type"] = logtyp
    #df["lastseen_log_type"] = logtyp

    df["drop"] = df.apply(check_age, axis=1, args=([start_date]))
    df = df.loc[df["drop"]]
    
    df.loc[df[fqdncol].isin(zeek_df[fqdncol]), "count_since_firstseen"] += 1

    df = df.drop(labels=["temp", "drop"], axis=1)

    df["days_since_firstseen"] = df["firstseen_date"].apply(compute_days_since, args=([start_date]))
    df["days_since_lastseen"] = df["lastseen_date"].apply(compute_days_since, args=([start_date]))

    #df.loc[df["isIP"].isnull(), "isIP"] = df.loc[df["isIP"].isnull()][fqdncol].apply(lambda x: isIP(x))
    
    return df