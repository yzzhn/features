import pandas as pd
import numpy as np
import time

############ Aggregation Tuple ############
TUPLE = [
    "id.orig_h",
    "id.resp_h",
    "id.resp_p",
]

############ Output Columns ############
OUTPUT_COLUMNS_DICT = {
    "ssl_agg_count": [],
    "dur_mean": [],
    "dur_std": [],
    "prcnt_outside_std_dev": [],
    "sum_orig_bytes": [],
    "sum_resp_bytes": [],
    "sum_orig_pkts": [],
    "sum_resp_pkts": [],
    "periodicity_mean": [],
    "periodicity_std": [],
    "mean_cert_fuids": [],
    "mean_validity_len": [],
    "std_validity_len": [],
    "num_cert_invalid": [],
    "ratio_cert_age_valid": [],
    "unique_cert_fuids": [],
    "mean_num_san_dns_domains": [],
    "ratio_responder_to_all_bytes": [],
    "ratio_established_conn_state": [],
    "ratio_conn_to_ssl": [],
    "ratio_tls_ssl": [],
    "ratio_null_sni": [],
    "ratio_self_signed": [],
    "all_sni_in_san_dns": [],
}

OUTPUT_COLUMNS_LIST = [
    "ssl_agg_count",
    "dur_mean",
    "dur_std",
    "prcnt_outside_std_dev",
    "sum_orig_bytes",
    "sum_resp_bytes",
    "sum_orig_pkts",
    "sum_resp_pkts",
    "periodicity_mean",
    "periodicity_std",
    "mean_cert_fuids",
    "mean_validity_len",
    "std_validity_len",
    "num_cert_invalid",
    "ratio_cert_age_valid",
    "unique_cert_fuids",
    "mean_num_san_dns_domains",
    "ratio_responder_to_all_bytes",
    "ratio_established_conn_state",
    "ratio_conn_to_ssl",
    "ratio_tls_ssl",
    "ratio_null_sni",
    "ratio_self_signed",
    "all_sni_in_san_dns",
]

############ Columns to be Converted to Numeric ############
TO_NUMERIC = ["cert_invalid", "ratio_cert_age_valid"]

############ Intermediate Columns to Drop from Final Output ############
DROP_AT_END = [
    "ssl_in_version",
    "tls_in_version",
    "all_bytes",
    "sni_in_san_dns",
    "valid_san_dns",
    "established_state",
    "non_established_state",
    "ssl_and_tls_count",
    "self_signed",
    "null_sni",
    "ts_x",
]

DROP_AT_END_SINGLES = [
    "all_bytes",
]

############ Feature-Specific Constants ############
ESTABLISHED_STATES = ["SF", "S1", "S2", "S3", "RSTO", "RSTR"]

NON_ESTABLISHED_STATES = ["S0", "OTH", "REJ", "SH", "SHR", "RSTOS0", "RSTRH"]


OUTPUT_COLUMNS_LIST = ["ts", "uid", "label"]


def log_timestamp(s):
    """Function for converting log timestamps to epoch time written by CPK. If the log timestamp format changes this will break badly... Works fine for Aug-22-2019"""
    default = 0.0
    millis_float = 0.0
    if s:
        millis = str(s).split(".")
        if len(millis) > 1:
            default = time.mktime(time.strptime(str(s), "%Y-%m-%d %H:%M:%S.%f"))
            millis_float = int(millis[1]) / (10 ** len(millis[1]))
            default = default + millis_float
        else:
            default = time.mktime(time.strptime(str(s), "%Y-%m-%d %H:%M:%S"))
    # default = time.mktime( time.strptime(s, "%Y-%m-%d %H:%M:%S") )
    return round(default, 2)


def compute_ssl_mismatch_labels(ssl_df, conn_df):
    """Heads up this was written in a way that throws tons of python/pandas warnings regarding the misuse of a copy from a slice... Should be rewritten."""
    # Set label 0
    conn_df_not_443 = conn_df[~conn_df["id_resp_p"].isin([443])]
    conn_df_not_443["label"] = 0

    # Reduce set of conn records to those on port 443
    conn_df_443 = conn_df[conn_df["id_resp_p"].isin([443])]

    # Filter to only established TCP
    conn_df_443["established_state"] = (
        conn_df_443["conn_state"].str.contains("|".join(ESTABLISHED_STATES)).astype(int)
    )
    conn_df_443 = conn_df_443[conn_df_443["established_state"] == 1]

    # Get set of records that does not have an SSL match
    no_ssl_conn_df = pd.merge(
        conn_df_443, ssl_df, on="uid", how="outer", indicator=True
    )
    no_ssl_conn_df = no_ssl_conn_df[no_ssl_conn_df["_merge"] == "left_only"]
    no_ssl_conn_df["tuple"] = list(
        zip(
            no_ssl_conn_df["id_orig_h_x"],
            no_ssl_conn_df["id_resp_h_x"],
            no_ssl_conn_df["id_resp_p_x"],
        )
    )

    # Get set of records that does have an SSL match
    ssl_conn_df = pd.merge(conn_df_443, ssl_df, on="uid", how="inner")
    ssl_conn_df["tuple"] = list(
        zip(
            ssl_conn_df["id_orig_h_x"],
            ssl_conn_df["id_resp_h_x"],
            ssl_conn_df["id_resp_p_x"],
        )
    )

    # Set label 1 – TCP and SSL established
    tcp_and_ssl_successful = ssl_conn_df[ssl_conn_df["established"] == "T"]
    tcp_and_ssl_successful["label"] = 1
    tcp_and_ssl_successful["ts"] = tcp_and_ssl_successful["ts_x"]
    tcp_and_ssl_successful["ts"] = tcp_and_ssl_successful["ts"].apply(log_timestamp)
    tcp_and_ssl_successful["tuple"] = list(
        zip(
            tcp_and_ssl_successful["id_orig_h_x"],
            tcp_and_ssl_successful["id_resp_h_x"],
            tcp_and_ssl_successful["id_resp_p_x"],
        )
    )

    # Get records where TCP succeeded but SSL failed
    tcp_success_and_ssl_failed = ssl_conn_df[ssl_conn_df["established"] == "F"]
    tcp_success_and_ssl_failed["tuple"] = list(
        zip(
            tcp_success_and_ssl_failed["id_orig_h_x"],
            tcp_success_and_ssl_failed["id_resp_h_x"],
            tcp_success_and_ssl_failed["id_resp_p_x"],
        )
    )

    # Set label 2 – SSL failed but sibling had successful TCP and SSL
    ssl_failed_but_sibling_success = tcp_success_and_ssl_failed[
        tcp_success_and_ssl_failed["tuple"].isin(tcp_and_ssl_successful["tuple"])
    ]
    ssl_failed_but_sibling_success["label"] = 2
    ssl_failed_but_sibling_success["ts"] = ssl_failed_but_sibling_success["ts_x"]

    # Set label 3 – SSL failed and no sibling
    ssl_failed_no_sibling = tcp_success_and_ssl_failed[
        ~tcp_success_and_ssl_failed["tuple"].isin(tcp_and_ssl_successful["tuple"])
    ]
    ssl_failed_no_sibling["label"] = 3
    ssl_failed_no_sibling["ts"] = ssl_failed_no_sibling["ts_x"]

    # Set label 4 – SSL not attempted but sibling exists
    ssl_not_attempted_with_sibling = no_ssl_conn_df[
        no_ssl_conn_df["tuple"].isin(ssl_conn_df["tuple"])
    ]
    ssl_not_attempted_with_sibling["label"] = 4
    ssl_not_attempted_with_sibling["ts"] = ssl_not_attempted_with_sibling["ts_x"]

    # Set label 5 – SSL not attempted and no sibling exists
    ssl_not_attempted_no_sibling = no_ssl_conn_df[
        ~no_ssl_conn_df["tuple"].isin(ssl_conn_df["tuple"])
    ]
    ssl_not_attempted_no_sibling["label"] = 4
    ssl_not_attempted_no_sibling["ts"] = ssl_not_attempted_with_sibling["ts_x"]

    # Assign labels and return
    conn_df_not_443 = conn_df_not_443[OUTPUT_COLUMNS_LIST]
    tcp_and_ssl_successful = tcp_and_ssl_successful[OUTPUT_COLUMNS_LIST]
    ssl_failed_but_sibling_success = ssl_failed_but_sibling_success[OUTPUT_COLUMNS_LIST]
    ssl_failed_no_sibling = ssl_failed_no_sibling[OUTPUT_COLUMNS_LIST]
    ssl_not_attempted_with_sibling = ssl_not_attempted_with_sibling[OUTPUT_COLUMNS_LIST]
    ssl_not_attempted_no_sibling = ssl_not_attempted_no_sibling[OUTPUT_COLUMNS_LIST]

    output = conn_df_not_443.append(tcp_and_ssl_successful)
    output = output.append(ssl_failed_but_sibling_success)
    output = output.append(ssl_failed_no_sibling)
    output = output.append(ssl_not_attempted_with_sibling)
    output = output.append(ssl_not_attempted_no_sibling)
    return output
