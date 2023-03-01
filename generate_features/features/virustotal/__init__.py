name = "virustotal"

import datetime

from generate_features.features.virustotal.file_io import VirusTotalRawLoader
from generate_features.features import FeatureConfig, Timescales
from generate_features import available_features

feed_types = ["virustotal"]

for typ in feed_types:
    if typ in available_features:
        raise ValueError("Feature named {} is defined twice!!!".format(typ))
    available_features[typ] = FeatureConfig(
        name=typ,
        df_type="spark",
        timescale=Timescales.ONE_YEAR.value,
        memoize=True,
        raw_loader=VirusTotalRawLoader(),
    )
