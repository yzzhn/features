name = "ssl_mismatch_label"

from generate_features.features import *
from generate_features.features.ssl_mismatch_label.ssl_mismatch_label_tool import (
    compute_ssl_mismatch_labels,
)

ssl_mismatch_deps = [Dependency("ssl"), Dependency("conn")]


@FeatureConfig(
    dependencies=ssl_mismatch_deps,
    df_type="spark",
    memoize=True,
    timescale=DEFAULT_TIME_INTERVAL,
)
def ssl_mismatch_label(ssl_log, conn_log, compute_info):
    return compute_ssl_mismatch_labels(ssl_log, conn_log)
