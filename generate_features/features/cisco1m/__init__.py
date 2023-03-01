name = "cisco1m"

from generate_features.features import *


@FeatureConfig(
    dependencies=[
        Dependency("http", ["host"], allow_missing=True),
        Dependency("wl_cisco_top1m"),
    ],
    df_type="pandas",
    timescale=Timescales.ONE_DAY.value,
    memoize=True,
)
def cisco1m(http_log, ciscodf, compute_info):
    if http_log is None:
        return None
    ciscodf["cisco1m_index"] = ciscodf.index.astype(int)
    result = http_log.merge(ciscodf, on="host", how="left")
    return result.drop_duplicates(subset=["host"])
