name = "honeypot"

import datetime

from generate_features.features import FeatureConfig, Timescales
from generate_features.features.feeds.file_io import CsvLoader

from generate_features import available_features


available_features["honeypot"] = FeatureConfig(
    name="honeypot",
    df_type="pandas",
    timescale=Timescales.ONE_DAY.value,
    memoize=False,
    raw_loader=CsvLoader(),
)
