name = "track1_certdb"

import datetime

import pyspark.sql.functions as fn
import pandas
import sys 
import tldextract

from generate_features.generate_feats import GenerateFeatures
from generate_features.helpers import *
from generate_features.features import *
from generate_features import available_features
import generate_features.features.track1_certdb.preprocess as cdb

### get shared spark context
from generate_features.spark_utils.context import get_shared_sql_context


@FeatureConfig(
    dependencies=[
        Dependency("x509", allow_missing=True,)
    ],
    df_type="spark",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def x509dailycert(x509, compute_info):
    df_lst = [x509]
    if any([df is None for df in df_lst]):
        return None

    x509 = x509.dropDuplicates(["certificate_serial", "certificate_issuer", "certificate_subject"])
    return cdb.preproc_x509(x509).repartition(1)

