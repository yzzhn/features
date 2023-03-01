name = "virustotal_cache"

from generate_features.features.virustotal_cache.file_io import VirusTotalLoader
from generate_features.features import FeatureConfig, Timescales
from generate_features import available_features

desc = """ Cache of threatcrowd query results, whether something was returned or not.  Uses filelock in case threatcrowd daemon is writing to the log. """

rawtype = "virustotal_cache"

if rawtype in available_features:
    raise ValueError("Feature named {} is defined twice!!!".format(rawtype))
available_features[rawtype] = FeatureConfig(
    df_type="spark",
    memoize=True,
    timescale=Timescales.ONE_MONTH.value,
    description=desc,
    filelock=True,
    raw_loader=VirusTotalLoader(),
    name=rawtype,
)
