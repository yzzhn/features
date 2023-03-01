import os

from generate_features.features.zeek_logs.cleanup_rdd import cleanup_RDD
from generate_features.spark_utils.context import get_shared_sql_context
from generate_features.features import RawLoader


class FireeyeRawLoader(RawLoader):
    def _form_filepath(self, root_dir, logtype, start_dt, end_dt):
        date = start_dt.strftime("%Y-%m-%d")
        json_filename = "{}_{}.log.gz".format(logtype, date.replace('-', ''))
        return os.path.join(root_dir, logtype, date, json_filename)

    def read(self, root_dir, logtype, start_dt, end_dt, *args, **kwargs):
        json_filepath = self.filepath_or_error(root_dir, logtype, start_dt, end_dt)
        spark_context = get_shared_sql_context()
        df = cleanup_RDD(spark_context.read.json(json_filepath))
        return df
