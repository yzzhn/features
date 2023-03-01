from generate_features.features.zeek_logs import udfs
from pyspark.sql.functions import col, size, struct, lit
from pyspark.sql import functions as F
from pyspark.sql.types import *


ID_fields = [
    "index",
    "uid",
    "id_orig_p",
    "id_resp_p",
    "id_orig_h",
    "id_resp_h",
    "ts",
    "_lpp_ver",
    "_lpp_plugin",
    "ExternalIP",
    "InternalIP",
]
# Make a store of fields that should be counted and not dealt with in their native form.
# These are typically JSON lists
count_fields = [
    "cert_chain_fuids",
    "client_cert_chain_fuids",
    "tunnel_parents",
    "san_dns",
    "analyzers",
    "resp_mime_types",
    "resp_filenames",
    "resp_fuids",
    "orig_filenames",
    "orig_fuids",
    "orig_mime_types",
]
index_first_fields = [
    "cert_chain_fuids",
    "tx_hosts",
    "rx_hosts",
    "conn_uids",
    "tunnel_parents",
    "resp_mime_types",
    "resp_filenames",
    "resp_fuids",
    "orig_filenames",
    "orig_fuids",
    "orig_mime_types",
    "proxied",
    "tags",
]
time_fields = ["ts", "certificate_not_valid_before", "certificate_not_valid_after"]

# Make store of field-names and functions to parse them-with
parse_fields = {
#    "subject": udfs.parse_subject_UDF,
#    "issuer": udfs.parse_issuer_UDF,
#    "certificate_subject": udfs.parse_subject_UDF,
#    "certificate_issuer": udfs.parse_issuer_UDF,
#    "query": udfs.parse_domain_UDF,
#    "host": udfs.parse_domain_UDF,
#    "user_agent": udfs.parse_agents_UDF,
#    "server_name": udfs.parse_domain_UDF,
}

# Some advanced preprocessing methods(ie: parsing) will turn fields into key-value pairs
# This is not advantageous to us, so let's make a store for turning each pair into a field
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

# May still be missing some fields from ZeekV2 and definitely missing fields from custom features.
continuous_fields = set(
    (
        "duration",
        "orig_bytes",
        "resp_bytes",
        "orig_pkts",
        "resp_pkts",
        "request_body_len",
        "response_body_len",
        "certificate_not_valid_after",
        "certificate_not_valid_before",
        "dcc_file_size",
        "overflow_byes",
        "missing_bytes",
        "seen_bytes",
        "missed_bytes",
        "orig_ip_bytes",
        "resp_ip_bytes",
        "orig_bytes",
        "resp_bytes",
        "orig_pkts",
        "resp_pkts",
        "Num_IP_addresses_using_UA",
        "Ratio_of_failure",
        "Num_2xx_connection",
        "Num_3xx_connection",
        "Num_4xx_connection",
        "Num_5xx_connection",
        "Num_total_connection",
        "IP_Popularity_Total_Counts",
        "IP_Popularity_Total_Counts_Norm",
    )
)

ignore_fields = set(("ts_x", "ts_y", "user_agent_x", "user_agent_y"))


def get_discrete_fields(RDD):
    """Guesstimate fields in an RDD that are discrete (not continuous nor keys)
    """
    fields = []
    for fname, field in zip(RDD.schema.names, RDD.schema.fields):
        if (
            ((fname in ID_fields) == False)
            and fname not in continuous_fields
            and fname not in ignore_fields
        ):
            print(
                "    discrete field {: <42} of type {: <42}".format(
                    fname, str(field.dataType)
                )
            )
            fields.append(fname)
        else:
            print(
                "non-discrete field {: <42} of type {: <42}".format(
                    fname, str(field.dataType)
                )
            )
    return fields


def cleanup_RDD(RDD):
    """Applies a series of corrections to a PySpark RDD of ."""
    field_names = RDD.schema.names
    # Look for violations in Spark naming conventions(ie: convert .'s to _'s)
    for fname in field_names:
        if "." in fname:
            newname = fname.replace(".", "_")
            RDD = RDD.withColumnRenamed(fname, newname)
        if " " in fname:
            newname = fname.replace(" ", "_")
            RDD = RDD.withColumnRenamed(fname, newname)

    field_names = RDD.schema.names

    parsekeys = list(parse_fields.keys())
    transformkeys = list(parse_fields.keys())
    # Look for datatypes naively - this could be made far more flexible and handle special cases
    for fname in field_names:
        # Gaurantee we are not manipulating an Indexing Field
        if ~(fname in ID_fields):
            # Is this a field to be converted to counts?
            if fname in count_fields:
                RDD = RDD.withColumn(fname + "_len", size(fname))
            # Is this field one that should be parsed?
            elif fname in parsekeys:
                RDD = RDD.withColumn("parsed_" + fname, parse_fields[fname](fname))
                for newsubfield in keys_to_fields[fname]:
                    RDD = RDD.withColumn(
                        fname + "_" + newsubfield,
                        RDD["parsed_" + fname].getItem(newsubfield),
                    )
                # Drop the parsed field after parsing
                RDD = RDD.drop(col("parsed_" + fname))
            if fname in index_first_fields:
                RDD = RDD.withColumn(
                    "first_" + fname,
                    F.when(size(RDD[fname]) > 0, RDD[fname].getItem(0)).otherwise(""),
                )
        if fname in time_fields:
            RDD = RDD.withColumn(fname, RDD[fname].cast(TimestampType()))
            if fname == "ts":
                RDD = RDD.withColumn(fname, udfs.parse_log_timestamp_UDF(fname))
    
    # For some possible features it's beneficial to know the internal and external IPs of a record. Think every record has these three fields...
    if set(["anon_orig", "id_orig_h", "id_resp_h"]).issubset(set(RDD.columns)):
        RDD = RDD.withColumn(
            "IntExt",
            udfs.external_internal_UDF(struct("anon_orig", "id_orig_h", "id_resp_h")),
        )  # Check for internal ips
        RDD = RDD.withColumn("InternalIP", RDD["IntExt"].getItem("Internal"))
        RDD = RDD.withColumn("ExternalIP", RDD["IntExt"].getItem("External"))
        RDD = RDD.drop(col("IntExt"))
        RDD = RDD.withColumn("subnet", udfs.find_subnet_index_udf("InternalIP"))

    # Handle missing values for discrete fields or else all will explode on computing likelihoods via pyspark joins...
    RDD = RDD.fillna("missing", subset=get_discrete_fields(RDD))

    return RDD
