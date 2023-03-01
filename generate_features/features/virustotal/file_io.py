import os
import ast
import glob

import pandas as pd

from generate_features.features import RawLoader
from generate_features.features import MissingRawLogError
from generate_features.features.zeek_logs.cleanup_rdd import cleanup_RDD
from generate_features.spark_utils.context import get_shared_sql_context


VIRUS_TOTAL_TYPES = ["detected_urls", "detected_communicating_samples", "detected_downloaded_samples", "detected_referrer_samples",
            "undetected_communicating_samples", "undetected_downloaded_samples", "undetected_referrer_samples"]

def get_positives(ip_report, key):
    if key not in ip_report:
        count = 0
        pos = 0
        total = 0
    else:
        count = len(ip_report[key])
        found = False
        for u in ip_report[key]:
            if "positives" in u:
                found = True

        if count == 0 or found == False:
            pos = 0
            total = 0
        else:
            pos = round(sum([u["positives"] for u in ip_report[key]]) / count, 1)
            total = round(sum([u["total"] for u in ip_report[key]]) / count, 1)

    return pos, total


#def get_score(response):
#    positive_sum = 0
#    total_sum = 0
#    for key in VIRUS_TOTAL_TYPES:
#        pos, total = get_positives(response, key)    
#        positive_sum += pos
#        total_sum += total
#    return positive_sum / (total_sum + 1)

def get_score(response):
    positives = response.get('positives')
    total = response.get('total')
    if total == 0 or total is None:
        return 0 
    return positives / total


#class VirusTotalRawLoader(RawLoader):
#    def _form_filepath(self, root_dir, logtype, start_dt, end_dt):
#        date = start_dt.strftime("%Y-%m-%d")
#        filename = "{}_{}.log".format(logtype, date)
#        return os.path.join(root_dir, logtype, date, filename)
#
#
#    def read(self, root_dir, logtype, start_dt, end_dt, *args, **kwargs):
#        log_dir = os.path.join(root_dir, logtype, '*/*')
#
#        files = glob.glob(log_dir, recursive=True)
#
#        df = None
#        for fil in files:
#            with open(fil, "r") as f:
#                columns = ['fqdn', 'vt_date', 'vt_score']
#                for line in f:
#                    response = ast.literal_eval(line)
#                    fqdn = response.get('query')
#
#                    date = response.get('scan_date')
#                    if date is not None:
#                        date = date.split(' ')[0]
#                    else:
#                        date = response.get('query_date')
#                    score = get_score(response)
#
#                    if df is None:
#                        df = pd.DataFrame([[fqdn, date, score]], columns=columns)
#                    else:
#                        new_row = pd.DataFrame([[fqdn, date, score]], columns=columns)
#                        df = df.append(new_row)
#
#        if df is None:
#           raise MissingRawLogError(set(), logtype, start_dt, end_dt)
# 
#        return df.reset_index(drop=True)



def vt_log_to_df(filepath):
    with open(filepath, "r") as f:
        df = None
        columns = ['fqdn', 'date', 'positives', 'total']
        for line in f:
            try:
                response = ast.literal_eval(line)
                fqdn = response.get('query')
                date = response.get('query_date')
                positives = response.get('positives')
                total = response.get('total')
                scans = response.get('scans')

                if any([x is None for x in [fqdn, date, positives, total, scans]]):
                    pass

                detect_name, result_name = [], []
                detect_val, result_val = [], []
                for k in scans.keys():
                    k_name = ''.join(e for e in k if e.isalnum()).lower()
                    detect_name.append(k_name + '_detected')
                    result_name.append(k_name + '_result')
                    detect_val.append(int(scans.get(k).get('detected')))
                    result_val.append(str(scans.get(k).get('result')))

                if not any([val != 0 for val in detect_val]):
                    pass

                if df is None:
                    df = pd.DataFrame([[fqdn, date, positives, total] + detect_val + result_val],
                                        columns=(columns + detect_name + result_name))
                else:
                    if (df['fqdn'] == fqdn).any():
                        pass
                    row = pd.DataFrame([[fqdn, date, positives, total] + detect_val + result_val],
                                        columns=(columns + detect_name + result_name))
                    df = pd.concat([df, row], sort=False)
            except:
                pass

    if df is not None:
        df.reset_index(drop=True, inplace=True)
    return df



class VirusTotalRawLoader(RawLoader):
    def _form_filepath(self, root_dir, logtype, start_dt, end_dt):
        date = start_dt.strftime("%Y-%m-%d")
        filename = "{}_{}.log".format(logtype, date)
        return os.path.join(root_dir, logtype, date, filename)


    def read(self, root_dir, logtype, start_dt, end_dt, *args, **kwargs):
        log_dir = os.path.join(root_dir, logtype, '*/*')

        files = glob.glob(log_dir, recursive=True)

        df = None
        for fil in files:
            new_df = vt_log_to_df(fil)
            if new_df is not None:
                df = pd.concat([df, new_df], sort=False)

        if df is None:
           raise MissingRawLogError(set(), logtype, start_dt, end_dt)
 
        return df.reset_index(drop=True)



