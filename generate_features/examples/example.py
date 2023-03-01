import datetime

from generate_features.generate_feats import GenerateFeatures
from generate_features.helpers import *

pd.options.display.max_rows = 100

start_time = datetime.datetime(2019, 11, 22, hour=0, minute=0)
end_time = datetime.datetime(2019, 11, 23, hour=0, minute=0)

features = GenerateFeatures('/scratch/jjp3n/root_dir/')

df = features.get_feature('filterable_http_grpby_host', start_time, end_time, 'pandas')
