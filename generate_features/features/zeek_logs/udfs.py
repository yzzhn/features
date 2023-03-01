import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import col, udf
from netaddr import *

import os
import datetime

from os import sys


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


def domain(s):
    """ a User Defined Function that Parses the domains in DNS queries into subdomain and domain fields.
    """
    import tldextract

    if (s is None) == False:
        try:
            tld = tldextract.extract(s)
            return [tld.subdomain, tld.domain]
        except:
            return ["missing", "missing"]
    return ["missing", "missing"]


def user_agent(s):
    """ A User Defined Function that Parses the domains in DNS queries into it's respective components:
    user_agent-family, user_agent-major,o s-family, os-major, device-family, device-brand, device-model
    """
    from ua_parser import user_agent_parser

    if ((s is None) == False) and ((s == "") == False):
        uap = user_agent_parser.Parse(s)
        result = [
            uap["user_agent"]["family"],
            uap["user_agent"]["major"],
            uap["os"]["family"],
            uap["os"]["major"],
            uap["device"]["family"],
            uap["device"]["brand"],
            uap["device"]["model"],
        ]
        for i, r in enumerate(result):
            if r is None:
                result[i] = "missing"
            elif isinstance(r, str) == False:
                result[i] = str(r)
        return result
    return ["missing", "missing", "missing", "missing", "missing", "missing", "missing"]


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

issuer_schema = StructType(
    [
        StructField("CN", StringType(), False),
        StructField("O", StringType(), False),
        StructField("C", StringType(), False),
    ]
)
parse_issuer_UDF = pyspark.sql.functions.udf(ssl_issuer, issuer_schema)


query_schema = StructType(
    [
        StructField("subdomain", StringType(), False),
        StructField("domain", StringType(), False),
    ]
)
parse_domain_UDF = pyspark.sql.functions.udf(domain, query_schema)


# Handle the user agent field in https logs
ua_schema = StructType(
    [
        StructField("family", StringType(), False),
        StructField("major", StringType(), False),
        StructField("os_family", StringType(), False),
        StructField("os_major", StringType(), False),
        StructField("device_family", StringType(), False),
        StructField("device_brand", StringType(), False),
        StructField("device_model", StringType(), False),
    ]
)
parse_agents_UDF = pyspark.sql.functions.udf(user_agent, ua_schema)


# Converts a log time stamp: "%Y-%m-%d %H:%M:%S.%f" to epoch time
def log_timestamp(s):
    import time

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
    result = round(default, 6)
    return result


parse_log_timestamp_UDF = pyspark.sql.functions.udf(log_timestamp, DoubleType())


# From a given record, discern which IP is internal to the network and which is external.
def externalinternal(s):  # S[ orig internal? , Orig , Resp ]
    default = ["", ""]  # Internal, External
    if (isinstance(s[0], bool) and s[0]) or (
        s[0] != "none"
    ):  # Is true if the orig is internal
        default = [s[2], s[1]]
    else:
        default = [s[1], s[2]]
    return default


extint_schema = StructType(
    [
        StructField("External", StringType(), False),
        StructField("Internal", StringType(), False),
    ]
)
external_internal_UDF = pyspark.sql.functions.udf(externalinternal, extint_schema)


def load_subnet_list(filepath):
    with open(filepath, "r") as f:
        subnets = f.readlines()
    subnet_list = list()
    for sub in subnets:
        subnet = sub.rstrip()
        if len(subnet) != 0:
            subnet_list.append(IPNetwork(subnet))
    return subnet_list


file_dir = os.path.dirname(os.path.abspath(__file__))
subnet_list = load_subnet_list(os.path.join(file_dir, "subnets.txt"))


def find_subnet_index(internal_ip):
    internal_ipaddr = IPAddress(internal_ip)
    result = -1
    for subnet in subnet_list:
        if internal_ipaddr in subnet:
            result = subnet_list.index(subnet)
    return result


find_subnet_index_udf = udf(find_subnet_index, IntegerType())
