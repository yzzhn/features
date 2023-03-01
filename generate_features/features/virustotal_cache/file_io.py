import os

import pandas as pd

from generate_features.spark_utils.context import get_shared_sql_context

from generate_features.features import RawLoader


class VirusTotalLoader(RawLoader):
    def _form_filepath(self, root_dir, logtype, start_dt, end_dt):
        feed_filename = "{}_{}.csv"
        date = str(start_dt.date())

        feed_filename = feed_filename.format(logtype, date)
        feed_dir = os.path.join(root_dir, logtype, date)
        return os.path.join(feed_dir, feed_filename)

    def read(self, root_dir, logtype, start_dt, result_df_type, **kwargs):
        feed_filepath = self.filepath_or_error(root_dir, logtype, start_dt, None)
        if result_df_type == "pandas":
            df = pd.read_csv(feed_filepath)
        elif result_df_type == "spark":
            spark_context = get_shared_sql_context()
            df = (
                spark_context.read.option("inferSchema", "true")
                .option("header", "true")
                .csv(feed_filepath)
            )
        else:
            raise Exception("Dataframe type not supported.")
        return df
