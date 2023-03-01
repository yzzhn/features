import os

import math

import pandas as pd
import numpy as np
from scipy import stats

from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import *


class Scorer:
  
  global_prefix = 'scored_'


class Discrete(Scorer):

  @staticmethod
  def score(df, field):
      num_unique_ids = df.nunique() 
      norm = 1. / (len(df)+ len(num_unique_ids))
      lookups = (num_unique_ids + 1) * norm
      try:
        result = df[field].map( lambda x: - math.log(lookups.get(x, norm)) )
      except:
        print(field)  
      return result / result.mean()


class Continuous(Scorer):

  @staticmethod
  def score(df, field):
      vals = np.log1p(df[field].values)
      mean = np.mean(vals)
      var  = np.var(vals)
      if var > 0:
        invar = 0.5 / var
        ll = np.power(df[field] - mean, 2) * invar + 0.5 * np.log(2 * math.pi * var)
        return ll / ll.mean()
      else:
        return df[field].apply(lambda x: 1.)


def __bayes_func( df, scorers, groupby_fields, keep_original_columns, grpby_description=""):
  if keep_original_columns:
    newdf = df
  else:
    newdf = df[groupby_fields]
  for scorer, fields in scorers:
    for field in fields:
      if field not in groupby_fields and not field.startswith(Scorer.global_prefix):
        newfield = scorer.global_prefix + grpby_description + field
        if newfield not in fields:
          newdf[ newfield ] = scorer.score(df, field)
  return newdf


def bayes(df, scorers, groupby_fields, keep_original_columns=True, grpby_description=""):
    df = df.groupby(groupby_fields).apply(lambda x: __bayes_func(x, scorers, groupby_fields, keep_original_columns=True, grpby_description=grpby_description))
    return df
