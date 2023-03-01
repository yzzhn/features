import os

from generate_features.spark_utils.context import get_shared_sql_context
from generate_features.features.zeek_logs.cleanup_rdd import cleanup_RDD
from generate_features.features import RawLoader


class ZeekRawLoader(RawLoader):
    def _form_filepath(self, root_dir, logtype, start_dt, end_dt):
        json_filename = "anon.{}_{}_{}-{}.log.gz"
        date = str(start_dt.date()).replace("-", "")
        start_tokens = str(start_dt.time()).split(":")
        start_time = start_tokens[0] + start_tokens[1]
        end_tokens = str(end_dt.time()).split(":")
        end_time = end_tokens[0] + end_tokens[1]
        json_filename = json_filename.format(logtype, date, start_time, end_time)
        json_dir = os.path.join(root_dir, logtype, str(start_dt.date()))
        return os.path.join(json_dir, json_filename)

    def read(self, root_dir, logtype, start_dt, end_dt, *args, **kwargs):
        filepath = self.filepath_or_error(root_dir, logtype, start_dt, end_dt)
        spark_context = get_shared_sql_context()
        return cleanup_RDD(spark_context.read.json(filepath))
