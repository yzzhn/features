name = "zeek_logs"

import datetime

from generate_features.features.zeek_logs.file_io import ZeekRawLoader
from generate_features.features import FeatureConfig, Timescales
from generate_features.features import DEFAULT_TIME_INTERVAL
from generate_features import available_features

# add raw logs to available_features`
raw_log_types = ["dns", "files", "irc", "ssh", "x509", "ssl", "http"]

for rawtype in raw_log_types:
    if rawtype in available_features:
        raise ValueError("Feature named {} is defined twice!!!".format(rawtype))
    available_features[rawtype] = FeatureConfig(
        name=rawtype,
        df_type="spark",
        timescale=Timescales.HALF_HOUR.value,
        memoize=True,
        raw_loader=ZeekRawLoader(),
    )


# add raw VT logs to available_features`
vt_raw_log_types = [ "ssl_vt", "conn_vt", "http_vt"]


for rawtype in vt_raw_log_types:
    if rawtype in available_features:
        raise ValueError("Feature named {} is defined twice!!!".format(rawtype))
    available_features[rawtype] = FeatureConfig(
        name=rawtype,
        df_type="spark",
        timescale=Timescales.HALF_HOUR.value,
        memoize=True,
        raw_loader=ZeekRawLoader(),
    )
    
    
# add raw logs to available_features`
non_per_log_types = ["conn_long", "conn",]

for rawtype in non_per_log_types:
    if rawtype in available_features:
        raise ValueError("Feature named {} is defined twice!!!".format(rawtype))
    available_features[rawtype] = FeatureConfig(
        name=rawtype,
        df_type="spark",
        timescale=Timescales.HALF_HOUR.value,
        memoize=False,
        raw_loader=ZeekRawLoader(),
    )
