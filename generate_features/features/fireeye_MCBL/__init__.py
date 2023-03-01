name = "fireeye_MCBL"

import datetime
from datetime import timedelta

import pyspark.sql.functions as f

from generate_features.features import *
from generate_features.features.fireeye_MCBL.fireeye_mcbl import *
from generate_features.helpers.dataframe import concat_df_rows


@FeatureConfig(
    dependencies=[Dependency("fireeye", allow_missing = False,)],
    df_type="pandas",
    memoize=True,
    timescale=Timescales.ONE_DAY.value,
    description=""" 
        Filters fireEye down to just the malware-callback logs,
        and parses out relevant columns: 'cncHost', 'cncPort',
        'request', 'fqdn', 'fileHash', 'sha256sum',
        'requestMethod', 'sname'
    """,
)
def fireeye_MCBL_v1(fireeye_df, compute_info):
    if fireeye_df is None:
        return None
    return get_fireeye_MCBL(fireeye_df)

