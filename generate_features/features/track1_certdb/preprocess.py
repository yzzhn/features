import pandas as pd
import pyspark
import pyspark.sql.functions as fn
from pyspark.sql.types import *
import hashlib


def ssl_subject(s):
    default = ["", "", "", "", ""]
    if s:
        for internalstring in s.split(","):
            lhsrhs = internalstring.split("=")
            if len(lhsrhs) == 2:
                if lhsrhs[0] == "CN":
                    default[0] = lhsrhs[1]
                if lhsrhs[0] == "O":
                    default[1] = lhsrhs[1]
                if lhsrhs[0] == "C":
                    default[2] = lhsrhs[1]
                if lhsrhs[0] == "L":
                    default[3] = lhsrhs[1]
                if lhsrhs[0] == "ST":
                    default[4] = lhsrhs[1]
    return default

subject_schema = StructType(
    [
        StructField("CN", StringType(), False),
        StructField("O", StringType(), False),
        StructField("C", StringType(), False),
        StructField("L", StringType(), False),
        StructField("ST", StringType(), False),
    ]
)
parse_subject_UDF = pyspark.sql.functions.udf(ssl_subject, subject_schema)

def ssl_issuer(s):
    default = ["", "", ""]
    if s:
        for internalstring in s.split(","):
            lhsrhs = internalstring.split("=")
            if len(lhsrhs) == 2:
                if lhsrhs[0] == "CN":
                    default[0] = lhsrhs[1]
                if lhsrhs[0] == "O":
                    default[1] = lhsrhs[1]
                if lhsrhs[0] == "C":
                    default[2] = lhsrhs[1]
    return default

issuer_schema = StructType(
    [
        StructField("CN", StringType(), False),
        StructField("O", StringType(), False),
        StructField("C", StringType(), False),
    ]
)
parse_issuer_UDF = pyspark.sql.functions.udf(ssl_issuer, issuer_schema)


parsed_cols = ['certificate_issuer', 'certificate_subject']

parse_fields = {
    "subject": parse_subject_UDF,
    "issuer": parse_issuer_UDF,
    "certificate_subject": parse_subject_UDF,
    "certificate_issuer": parse_issuer_UDF,
#    "query": udfs.parse_domain_UDF,
#    "host": udfs.parse_domain_UDF,
#    "user_agent": udfs.parse_agents_UDF,
#    "server_name": udfs.parse_domain_UDF,
}

keys_to_fields = {
    "subject": ["CN", "O", "C", "L", "ST"],
    "issuer": ["CN", "O", "C"],
    "certificate_subject": ["CN", "O", "C", "L", "ST"],
    "certificate_issuer": ["CN", "O", "C"],
    "query": ["subdomain", "domain"],
    "host": ["subdomain", "domain"],
    "server_name": ["subdomain", "domain"],
    "user_agent": [
        "family",
        "major",
        "os_family",
        "os_major",
        "device_family",
        "device_brand",
        "device_model",
    ],
}

def parse_x509(RDD):
    for colname in RDD.columns:
        if colname in parsed_cols:
            RDD = RDD.withColumn("parsed_"+colname, parse_fields[colname](colname))
            for newsubfield in keys_to_fields[colname]:
                RDD = RDD.withColumn(colname + "_" + newsubfield, RDD["parsed_" + colname].getItem(newsubfield),)
            # Drop the parsed field after parsing
            RDD = RDD.drop(fn.col("parsed_" + colname))
    return RDD


# Define UDF and apply it
def format_func(serial, issuer_CN, subject_CN):
    return "{},{},{}".format(serial, issuer_CN, subject_CN)

format_udf = fn.udf(format_func, StringType())


def hashing(s):
    if s:
        return hashlib.md5(s.encode()).hexdigest()
    return ""

hash_udf = fn.udf(hashing, StringType())


def preproc_x509(rdd):
    rdd = parse_x509(rdd)
    rdd = rdd.withColumn('certificate_not_valid_before_ts', fn.unix_timestamp(fn.col("certificate_not_valid_before")))
    rdd = rdd.withColumn('certificate_not_valid_after_ts', fn.unix_timestamp(fn.col("certificate_not_valid_after")))
    rdd = rdd.withColumn('certificate_validity_seconds', fn.col("certificate_not_valid_after_ts") - fn.col("certificate_not_valid_before_ts"))
    rdd = rdd.withColumn("hashstring", format_udf(fn.col("certificate_serial"), fn.col("certificate_issuer_CN"),  fn.col("certificate_subject_CN")))
    rdd = rdd.withColumn("hashkey", hash_udf(fn.col("hashstring")))
    rdd = rdd.withColumn("san_dns_json", fn.to_json(fn.col("san_dns")))

    valid_cols = []
    target_cols = ['basic_constraints_ca', 'basic_constraints_path_len', 'certificate_curve', 'certificate_exponent', 'certificate_key_alg',
                'certificate_key_length', 'certificate_key_type', 'certificate_not_valid_after_ts', 'certificate_not_valid_before_ts', 'certificate_serial',
                'certificate_sig_alg', 'certificate_version', 'certificate_issuer_CN', 
                'certificate_issuer_O', 'certificate_issuer_C', 'certificate_subject_CN','certificate_subject_O', 'certificate_subject_C', 'san_dns_len',
                'certificate_validity_seconds', 'hashkey', 'san_dns_json']

    for c in target_cols:
        if c in rdd.columns:
            valid_cols.append(c)
    print(valid_cols)
    rdd = rdd.select(valid_cols).dropDuplicates(["hashkey"])
    return rdd
