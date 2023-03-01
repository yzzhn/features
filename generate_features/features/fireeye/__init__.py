name = "fireeye"

import datetime

from generate_features.features.fireeye.file_io import FireeyeRawLoader
from generate_features.features import FeatureConfig, Timescales
from generate_features import available_features

# add raw logs to available_features`
feed_types = ["fireeye"]

for typ in feed_types:
    if typ in available_features:
        raise ValueError("Feature named {} is defined twice!!!".format(typ))
    available_features[typ] = FeatureConfig(
        name=typ,
        df_type="spark",
        timescale=Timescales.ONE_DAY.value,
        memoize=True,
        raw_loader=FireeyeRawLoader(),
    )
