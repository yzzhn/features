name = "features"

import os
from collections import OrderedDict
from functools import reduce
import copy
from datetime import timedelta
from enum import Enum

from pprint import pformat

from generate_features import available_features


# Possible timeframes
class Timescales(Enum):
    HALF_HOUR = timedelta(minutes=30)
    ONE_HOUR = timedelta(hours=1)
    TWO_HOURS = timedelta(hours=2)
    FOUR_HOURS = timedelta(hours=4)
    EIGHT_HOURS = timedelta(hours=8)
    ONE_DAY = timedelta(days=1)
    ONE_MONTH = "month"
    ONE_YEAR = "year"


# TODO: make this be raw log time interval depending on dataset...
DEFAULT_TIME_INTERVAL = Timescales.HALF_HOUR.value


def md_list_item(k, v):
    if v:
        return "**{}**  {}\n\n".format(k, v)
    else:
        return ""


class FeatureConfig(object):

    # construct with default config
    def __init__(
        self,
        description="",
        dependencies=[],
        df_type="pandas",
        memoize=False,
        timescale=DEFAULT_TIME_INTERVAL,
        raw_loader=None,
        compute_feature=None,
        name=None,
        filelock=False,
    ):
        self.dependencies = dependencies
        self.df_type = df_type
        self.memoize = memoize
        self.timescale = timescale
        self.raw_loader = raw_loader
        self.compute_feature = compute_feature
        self.name = name
        self.description = description
        self.filelock = filelock

    # TODO: write a test asserting that the output of __repr__ can be evaluated by python into a FeatureConfig object
    def __repr__(self):
        return "FeatureConfig({})".format(
            ", ".join(["{}={}".format(k, pformat(v)) for k, v in vars(self).items()])
        )

    # override default config
    def also(self, **kwargs):
        result = copy.copy(self)
        for k, v in kwargs.items():
            result.__dict__[k] = v
        return result

    # the decorator
    def __call__(self, func):
        if self.name is None:
            self.name = func.__name__
        if self.name in available_features:
            raise ValueError("Feature named {} is defined twice!!!".format(self.name))
        available_features[self.name] = self.also(compute_feature=func)

    def markdown(self):
        s = "## [`{}`](https://code.vt.edu/p-core/features/blob/master/generate_features/features/{}/__init__.py)\n\n".format(
            self.name, self.name
        )
        if self.raw_loader:
            s += md_list_item("raw", "")
        else:
            dep_name_set = set([dep.feature_name for dep in self.dependencies])

            deps = ["[`{}`](#{})".format(dep, dep) for dep in list(dep_name_set)]
            s += md_list_item("dependencies", "  ".join(deps))
            s += md_list_item("framework", self.df_type)
        s += md_list_item("timescale", self.timescale)
        s += md_list_item("memoize", self.memoize)
        s += md_list_item("description", self.description)
        return s


class Dependency(object):
    def __init__(
        self, feature_name, columns=None, offset=timedelta(0), allow_missing=False
    ):
        """ feature_name -> str, columns -> list(str), offset -> datetime.timedelta """
        self.feature_name = feature_name
        self.columns = columns
        self.offset = offset
        self.allow_missing = allow_missing


class MissingRawLogError(Exception):
    def __init__(self, missing_filepaths, logtype, start_dt, end_dt):
        assert (
            type(missing_filepaths) == set
        ), "MissingRawLogError(missing_filepaths) expects missing_filepaths to be a set of filepath strings."
        super(MissingRawLogError, self).__init__(
            "Could not compute {} from {} to {} due to missing raw logs: {}".format(
                logtype, start_dt, end_dt, missing_filepaths
            )
        )
        self.missing_filepaths = missing_filepaths


class RawLoader:
    def filepath_or_error(self, root_dir, logtype, start_dt, end_dt):
        filepath = self._form_filepath(root_dir, logtype, start_dt, end_dt)
        if os.path.exists(filepath):
            return filepath
        else:
            # print("raise MissingRawLogError({}, {}, {})".format(logtype, start_dt, end_dt))
            raise MissingRawLogError(set([filepath]), logtype, start_dt, end_dt)

    def _form_filepath(self, root_dir, logtype, start_dt, end_dt):
        print("RawLoader._form_filepath SHOULD NEVER BE CALLED!!!", self)
        pass

    def read(self, root_dir, logtype, start_dt, end_dt):
        # subclass implementations need to call self.filepath_or_error
        pass
