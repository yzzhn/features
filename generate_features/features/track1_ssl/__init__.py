name = "track1_ssl"

import datetime

import pyspark.sql.functions as fn
import pandas as pd

from generate_features.generate_feats import GenerateFeatures
from generate_features.helpers import *
from generate_features.features import *
from generate_features import available_features

import tldextract

####################################################### UDFs ############################################################
def extract_sandns(domlist):
    if domlist is None:
        return []
    res = []
    for dom in domlist:
        res.append(tldextract.extract(dom).domain + '.' + tldextract.extract(dom).suffix)
    ret = list(set(res))
    return ret
extract_sandns_udf = fn.udf(extract_sandns)


def parse_issuer_O(s):
    if s:
        for internalstring in s.split(","):
            lhsrhs = internalstring.split("=")
            if len(lhsrhs) == 2:
                if lhsrhs[0] == "O":
                    issuer_O = lhsrhs[1]
    return issuer_O
parse_issuer_O_udf = fn.udf(parse_issuer_O)


def parse_subject_O(s):
    if s:
        for internalstring in s.split(","):
            lhsrhs = internalstring.split("=")
            if len(lhsrhs) == 2:
                if lhsrhs[0] == "O":
                    subject_O = lhsrhs[1]
    return subject_O
parse_subject_O_udf = fn.udf(parse_subject_O)


def parse_issuer(text):
    return text.replace('\\', '').replace('\"', '')\
               .split(" Inc")[0].split(" INC")[0].split(" L.L.C.")[0].split(" LLC")[0].split(" llc")[0]\
               .split(" Co.")[0].split(" CO.")[0].split(" Ltd")[0].split(" Limited")[0].split(" GmbH")[0]
extract_sandns_udf = fn.udf(extract_sandns)





######################################################################################################################

@FeatureConfig(
    dependencies=[
        Dependency("x509", ["id", "basic_constraints_ca", "basic_constraints_path_len", "certificate_issuer", "certificate_curve", \
                            "certificate_exponent", "certificate_not_valid_after", "certificate_not_valid_before", "certificate_subject", \
                            "certificate_key_length", "certificate_key_alg", "ts", "san_dns_len", "san_dns", "certificate_version", \
                            "certificate_sig_alg", "certificate_key_type", "certificate_serial"], allow_missing=True,),
        Dependency("ssl", ["anon_orig", "cert_chain_fuids", "cipher", "client_cert_chain_fuids", "client_issuer", "client_subject", "first_cert_chain_fuids", \
                            "curve", "established", "id_orig_h", "id_orig_p", "id_resp_h", "id_resp_p", "issuer", "subject", "ja3", "ja3s", \
                            "last_alert", "next_protocol", "resumed", "server_name", "validation_status", "version", "cert_chain_fuids_len", \
                            "client_cert_chain_fuids_len"], allow_missing=True,),
    ],
    df_type="spark",
    timescale=Timescales.HALF_HOUR.value,
    memoize=True,
)
def pre_process(x509, ssl, compute_info):
    
    ###############################################
    ## Select and calculate x509 features
    if x509 is None or ssl is None:
        return None
    x509 = x509.withColumn("ts", x509["ts"].cast("timestamp"))
    x509 = x509.withColumn("cert_not_yet_valid", x509["ts"] < x509["certificate_not_valid_before"])
    x509 = x509.withColumn("cert_expired", x509["ts"] > x509["certificate_not_valid_after"])
    x509 = x509.withColumn("validity_days", fn.datediff(fn.col('certificate_not_valid_after'), fn.col('certificate_not_valid_before')))
    x509 = x509.withColumn('domain_suffix_from_sandns', extract_sandns_udf(fn.col("san_dns")))
    
    age = fn.unix_timestamp('ts') - fn.unix_timestamp('certificate_not_valid_before').cast('long')
    x509 = x509.withColumn("cert_age", age / 86400)
    
    columns_to_drop = ["ts"]
    x509_sub = x509.drop(*columns_to_drop)
    
    ###############################################
    ## Select and calculate ssl features, merge with x509_sub
    ssl_origuva = ssl.filter(fn.col("anon_orig") == "uva")
    ssl_explode = ssl_origuva.select(*ssl_origuva.columns, fn.posexplode('cert_chain_fuids').alias('pos', 'cert_id'))
    _cols2drop = ['anon_orig']
    ssl_expsub = ssl_explode.drop(*_cols2drop)
    ssl_merge = ssl_expsub.join(x509_sub, ssl_expsub.cert_id == x509.id)
    ssl_leaf = ssl_merge.filter(fn.col('pos') == 0)
    ssl_root = ssl_merge.filter(fn.col('pos') == fn.col('cert_chain_fuids_len') - 1)
    
    _cols4root = ['cert_chain_fuids', 'basic_constraints_ca', 'basic_constraints_path_len', 'certificate_curve', 'certificate_exponent', \
                  'certificate_issuer', 'certificate_key_alg', 'certificate_key_length', 'certificate_key_type', 'certificate_not_valid_after', \
                  'certificate_not_valid_before', 'certificate_serial', 'certificate_sig_alg', 'certificate_subject', 'certificate_version', 'id']
    ssl_rootsub = ssl_root.select(*_cols4root)
    ssl_rootsub = ssl_rootsub.withColumnRenamed('basic_constraints_ca','root_basic_constraints_ca') \
                             .withColumnRenamed('cert_chain_fuids', 'cert_chain_fuids2') \
                             .withColumnRenamed('basic_constraints_path_len', 'root_basic_constraints_path_len') \
                             .withColumnRenamed('certificate_curve', 'root_certificate_curve') \
                             .withColumnRenamed('certificate_exponent', 'root_certificate_exponent') \
                             .withColumnRenamed('certificate_issuer', 'root_certificate_issuer') \
                             .withColumnRenamed('certificate_key_alg', 'root_certificate_key_alg') \
                             .withColumnRenamed('certificate_key_length', 'root_certificate_key_length') \
                             .withColumnRenamed('certificate_key_type', 'root_certificate_key_type') \
                             .withColumnRenamed('certificate_not_valid_after', 'root_certificate_not_valid_after') \
                             .withColumnRenamed('certificate_not_valid_before', 'root_certificate_not_valid_before') \
                             .withColumnRenamed('certificate_serial', 'root_certificate_serial') \
                             .withColumnRenamed('certificate_sig_alg', 'root_certificate_sig_alg') \
                             .withColumnRenamed('certificate_subject', 'root_certificate_subject') \
                             .withColumnRenamed('certificate_version', 'root_certificate_version') \
                             .withColumnRenamed('id', 'root_id')
    ssl_chain = ssl_leaf.join(ssl_rootsub, ssl_leaf.cert_chain_fuids == ssl_rootsub.cert_chain_fuids2, how='left')
    columns_to_drop = ["cert_chain_fuids2"]
    res = ssl_chain.drop(*columns_to_drop)
    
    return res.repartition(1)

#############################################################################################################

@FeatureConfig(
    dependencies=[
        Dependency("pre_process", allow_missing=True,)
    ],
    df_type="spark",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def ca_categorize_v2(df, compute_info):
    # read in maintained CA data
    pub_df = pd.read_csv("/home/hd7gr/track1-threathunting/ssl/cert_analysis/ca_category/202009-public.csv")
    pub_lis = pub_df['issuer_name'].tolist()
    
    priv_df = pd.read_csv("/home/hd7gr/track1-threathunting/ssl/cert_analysis/ca_category/202009-private.csv")
    priv_lis = priv_df['issuer_name'].tolist()
    
    semi_df = pd.read_csv("/home/hd7gr/track1-threathunting/ssl/cert_analysis/ca_category/202009-semi.csv")
    semi_lis = semi_df['issuer_name'].tolist()
    
    inter_df = pd.read_csv("/home/hd7gr/track1-threathunting/ssl/cert_analysis/ca_category/202009-inter.csv")
    inter_lis = inter_df['issuer_name'].tolist()
    
    err_df = pd.read_csv("/home/hd7gr/track1-threathunting/ssl/cert_analysis/ca_category/202009-err.csv")
    err_lis = err_df['issuer_name'].tolist()
    
    susp_df = pd.read_csv("/home/hd7gr/track1-threathunting/ssl/cert_analysis/ca_category/202009-susp.csv")
    susp_lis = susp_df['issuer_name'].tolist()
    
    df_sub = df.select("cert_chain_fuids", "first_cert_chain_fuids", "established", "id_orig_h", "id_orig_p", "id_resp_h", "id_resp_p", \
                       "server_name", "validation_status", "cert_chain_fuids_len", \
                       "validity_days", "cert_age", "domain_suffix_from_sandns", "basic_constraints_ca", "basic_constraints_path_len", \
                       'certificate_curve', 'certificate_exponent', 'certificate_issuer', 'certificate_key_alg', 'certificate_key_length', \
                       'certificate_key_type', 'certificate_not_valid_after', 'certificate_not_valid_before', 'certificate_serial', 'certificate_sig_alg', \
                       'certificate_subject', 'certificate_version', 'id', 'san_dns_len', 'root_basic_constraints_ca', 'root_basic_constraints_path_len', \
                       'root_certificate_curve', 'root_certificate_exponent', 'root_certificate_issuer', 'root_certificate_key_alg', \
                       'root_certificate_key_length', 'root_certificate_key_type', 'root_certificate_not_valid_after', 'root_certificate_not_valid_before', \
                       'root_certificate_serial', 'root_certificate_sig_alg', 'root_certificate_subject', 'root_certificate_version', 'root_id')
    comm_cols = ["server_name", "cert_chain_fuids_len", "validity_days", "certificate_issuer", "certificate_serial", "certificate_subject", "certificate_version", \
                 "san_dns_len", "root_certificate_issuer", "root_certificate_subject", "root_certificate_version"]
    df_drop = df_sub.dropDuplicates(comm_cols)
    
    df_drop = df_drop.withColumn("certificate_issuer_O", parse_issuer_O_udf(fn.col("certificate_issuer")))
    df_drop = df_drop.withColumn("certificate_subject_O", parse_subject_O_udf(fn.col("certificate_subject")))
    df_drop = df_drop.withColumn("parsed_issuer_O", parse_issuer_udf(fn.col("certificate_issuer_O")))
    res = df_drop.withColumn("ca_category", fn.when(df_drop.parsed_issuer_O.isin(pub_lis), "public")\
                                              .when(df_drop.parsed_issuer_O.isin(priv_lis), "private")\
                                              .when(df_drop.parsed_issuer_O.isin(semi_lis), "semi")\
                                              .when(df_drop.parsed_issuer_O.isin(inter_lis), "intercept")\
                                              .when(df_drop.parsed_issuer_O.isin(err_lis), "error")\
                                              .when(df_drop.parsed_issuer_O.isin(susp_lis), "suspicious").otherwise("other"))
    
    return res.repartition(1)