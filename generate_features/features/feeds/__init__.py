name = "feeds"

import datetime

from generate_features.features import FeatureConfig, Timescales
from generate_features.features.feeds.file_io import CsvLoader

from generate_features import available_features


available_features["renisac"] = FeatureConfig(
    name="renisac",
    df_type="spark",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
    raw_loader=CsvLoader(),
)


