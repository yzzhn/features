name = "spark_utils"

from pyspark.sql.functions import col, size, struct, lit
from pyspark.sql.types import *

ID_fields = set(['index', 'uid', 'id_orig_p','id_resp_p', 'id_orig_h', 'id_resp_h', 'ts', '_lpp_ver', '_lpp_plugin', "ExternalIP", "InternalIP" ])

#May still be missing some fields from ZeekV2 and definitely missing fields from custom features.
continuous_fields = set(("duration", "orig_bytes", "resp_bytes", "orig_pkts", "resp_pkts", "request_body_len", "response_body_len", "certificate_not_valid_after", "certificate_not_valid_before", "dcc_file_size", "overflow_byes", "missing_bytes", "seen_bytes", "missed_bytes", "orig_ip_bytes", "resp_ip_bytes", "orig_bytes", "resp_bytes", "orig_pkts", "resp_pkts", "Num_IP_addresses_using_UA", "Ratio_of_failure", "Num_2xx_connection", "Num_3xx_connection", "Num_4xx_connection", "Num_5xx_connection", "Num_total_connection", "IP_Popularity_Total_Counts", "IP_Popularity_Total_Counts_Norm"))

ignore_fields = set(("ts_x", "ts_y", "user_agent_x", "user_agent_y", "likely_benign", "info_code", "status_code", 'kudu_score', 'fe_score', 'tc_score', 'vt_score'))

count_fields = [ 'cert_chain_fuids', 'client_cert_chain_fuids', 'tunnel_parents', 'san_dns', 'analyzers', 'resp_filenames', 'resp_fuids', 'resp_mime_types', 'orig_filenames', 'orig_fuids', 'orig_mime_types' ]
index_first_fields = [ 'cert_chain_fuids', 'tx_hosts', 'rx_hosts', 'conn_uids', 'tunnel_parents', 'resp_filenames', 'resp_fuids', 'resp_mime_types', 'orig_filenames', 'orig_fuids', 'orig_mime_types', 'proxied', 'tags' ]
time_fields = [ 'ts', 'certificate_not_valid_before', 'certificate_not_valid_after' ]


made_fields = ['distinct_ct', 'frac_ct_empty', 'frac_ct_img', 'frac_ct_video', 'frac_ct_text', 'frac_ct_app', 'frac_ct_sh', 'frac_ct_exe', 'frac_ct_jar', 'frac_ct_js', 'frac_ct_html', 'frac_no_ref', 'frac_http_ref', 'num_distinct_ref', 'ratio_ref_over_host', 'avg_ref_perhost', 'min_ref_perhost', 'max_ref_perhost', 'has_diff_ref', 'num_100', 'num_200', 'num_300', 'num_400', 'num_500', 'frac_100', 'frac_200', 'frac_300', 'frac_400', 'frac_500', 'ratio_fail', 'num_distinct_ua', 'frac_no_ua', 'ratio_ua_over_host', 'ua_inverse_pop', 'ua_norm_pop', 'frac_ua1', 'frac_ua10', 'avg_ua_perhost', 'min_ua_perhost', 'max_ua_perhost', 'num_distinct_urls', 'num_files', 'avg_url_len', 'max_url_len', 'min_url_len', 'avg_url_depth', 'max_url_depth', 'min_url_depth', 'num_params', 'avg_params', 'max_params', 'min_params', 'num_sh', 'num_tmp', 'num_wget', 'num_chmod', 'num_tmp_sh', 'frac_bare', 'frac_sh', 'frac_tmp', 'frac_wget', 'frac_chmod', 'frac_tmp_sh', 'total_hosts', 'total_logs', 'avg_logs', 'min_logs', 'max_logs', 'total_sent_bytes', 'total_recv_bytes', 'avg_recv_over_sent', 'min_recv_over_sent', 'max_recv_over_sent', 'total_GET', 'total_POST', 'avg_ratio_PG', 'min_ratio_PG', 'max_ratio_PG', 'cisco1m_index', 'cnt_distinct_hosts_pop_host', 'cnt_logs', 'cnt_distinct_hosts_pop_ua', 'ua_norm', 'max_cnt_distinct_ip_uri_token', 'max_cnt_log_uri_token', 'max_ip_uri_token_freq', 'max_log_uri_token_freq', 'min5day_cnt_logs', 'max5day_cnt_logs', 'min5day_fqdn_norm', 'max5day_fqdn_norm', 'min5day_cnt_distinct_hosts_pop_host', 'max5day_cnt_distinct_hosts_pop_host', 'min5day_ua_norm', 'max5day_ua_norm', 'min5day_cnt_distinct_hosts_pop_ua', 'max5day_cnt_distinct_hosts_pop_ua', 'min5day_max_cnt_distinct_ip_uri_token', 'max5day_max_cnt_distinct_ip_uri_token', 'min5day_max_cnt_log_uri_token', 'max5day_max_cnt_log_uri_token', 'min5day_max_ip_uri_token_freq', 'max5day_max_ip_uri_token_freq', 'min5day_max_log_uri_token_freq', 'max5day_max_log_uri_token_freq', 'total_hosts', 'avg_logs', 'min_logs', 'max_logs', 'total_sent_bytes', 'total_recv_bytes', 'avg_recv_over_sent', 'min_recv_over_sent', 'max_recv_over_sent', 'total_GET', 'total_POST', 'avg_ratio_PG', 'min_ratio_PG', 'max_ratio_PG', 'cnt_distinct_hosts', 'fqdn_norm', 'dom_length', 'dom_level', 'dom_sld_entropy', 'dom_sub_cnt']

filterable_fields = ['min_cisco1m_index', 'min_cnt_distinct_hosts_pop_host', 'min_cnt_logs', 'min_fqdn_norm', 'min_cnt_distinct_hosts_pop_ua', 'min_ua_norm', 'min_max_cnt_distinct_ip_uri_token', 'min_max_cnt_log_uri_token', 'min_max_ip_uri_token_freq', 'min_max_log_uri_token_freq', 'min_min5day_cnt_logs', 'min_max5day_cnt_logs', 'min_min5day_fqdn_norm', 'min_max5day_fqdn_norm', 'min_min5day_cnt_distinct_hosts_pop_host', 'min_max5day_cnt_distinct_hosts_pop_host', 'min_min5day_ua_norm', 'min_max5day_ua_norm', 'min_min5day_cnt_distinct_hosts_pop_ua', 'min_max5day_cnt_distinct_hosts_pop_ua', 'min_min5day_max_cnt_distinct_ip_uri_token', 'min_max5day_max_cnt_distinct_ip_uri_token', 'min_min5day_max_cnt_log_uri_token', 'min_max5day_max_cnt_log_uri_token', 'min_min5day_max_ip_uri_token_freq', 'min_max5day_max_ip_uri_token_freq', 'min_min5day_max_log_uri_token_freq', 'min_max5day_max_log_uri_token_freq', 'max_cisco1m_index', 'max_cnt_distinct_hosts_pop_host', 'max_cnt_logs', 'max_fqdn_norm', 'max_cnt_distinct_hosts_pop_ua', 'max_ua_norm', 'max_max_cnt_distinct_ip_uri_token', 'max_max_cnt_log_uri_token', 'max_max_ip_uri_token_freq', 'max_max_log_uri_token_freq', 'max_min5day_cnt_logs', 'max_max5day_cnt_logs', 'max_min5day_fqdn_norm', 'max_max5day_fqdn_norm', 'max_min5day_cnt_distinct_hosts_pop_host', 'max_max5day_cnt_distinct_hosts_pop_host', 'max_min5day_ua_norm', 'max_max5day_ua_norm', 'max_min5day_cnt_distinct_hosts_pop_ua', 'max_max5day_cnt_distinct_hosts_pop_ua', 'max_min5day_max_cnt_distinct_ip_uri_token', 'max_max5day_max_cnt_distinct_ip_uri_token', 'max_min5day_max_cnt_log_uri_token', 'max_max5day_max_cnt_log_uri_token', 'max_min5day_max_ip_uri_token_freq', 'max_max5day_max_ip_uri_token_freq', 'max_min5day_max_log_uri_token_freq', 'max_max5day_max_log_uri_token_freq', 'avg_cisco1m_index', 'avg_cnt_distinct_hosts_pop_host', 'avg_cnt_logs', 'avg_fqdn_norm', 'avg_cnt_distinct_hosts_pop_ua', 'avg_ua_norm', 'avg_max_cnt_distinct_ip_uri_token', 'avg_max_cnt_log_uri_token', 'avg_max_ip_uri_token_freq', 'avg_max_log_uri_token_freq', 'avg_min5day_cnt_logs', 'avg_max5day_cnt_logs', 'avg_min5day_fqdn_norm', 'avg_max5day_fqdn_norm', 'avg_min5day_cnt_distinct_hosts_pop_host', 'avg_max5day_cnt_distinct_hosts_pop_host', 'avg_min5day_ua_norm', 'avg_max5day_ua_norm', 'avg_min5day_cnt_distinct_hosts_pop_ua', 'avg_max5day_cnt_distinct_hosts_pop_ua', 'avg_min5day_max_cnt_distinct_ip_uri_token', 'avg_max5day_max_cnt_distinct_ip_uri_token', 'avg_min5day_max_cnt_log_uri_token', 'avg_max5day_max_cnt_log_uri_token', 'avg_min5day_max_ip_uri_token_freq', 'avg_max5day_max_ip_uri_token_freq', 'avg_min5day_max_log_uri_token_freq', 'avg_max5day_max_log_uri_token_freq']


continuous_fields.update(made_fields)
continuous_fields.update(filterable_fields)

bad = set(count_fields + index_first_fields + time_fields)


def get_continuous_fields(df):
    fields = []
    for col in list(df.columns):
        if col in continuous_fields and col not in ignore_fields:
            print("    continuous field {: <42}".format(col))
            fields.append(col)
        else:
            print("non-continuous field {: <42}".format(col))
    print(fields)
    return fields    

def get_discrete_fields(df):
    """Guesstimate fields in an RDD that are discrete (not continuous nor keys)
    """
    fields = []
    for col in list(df.columns):

        if col not in (continuous_fields | ID_fields | ignore_fields | bad):
            print("    discrete field {: <42}".format(col))
            fields.append(col)
        else:
            print("non-discrete field {: <42}".format(col))
    print(fields)
    return fields

