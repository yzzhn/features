name = "generate_features"

from collections import OrderedDict

# GLOBAL
available_features = {}


import os 
import datetime

from generate_features.features import FeatureConfig


import importlib
from setuptools import find_packages

# add all features defined in `generate_features/features/*.py` to the global `available_feats` dict
file_dir = os.path.dirname(os.path.abspath(__file__))
package_dir = os.path.join(file_dir, '..')
for module_name in find_packages(package_dir):
    if 'generate_features.features.' in module_name:
        importlib.import_module(module_name)
        


#############################################################
# DAG helpers

def add_dependencies(logtypedict):
    result = OrderedDict(logtypedict)
    for lt in logtypedict:
        result[lt] = 0
        for dep in available_features[lt].dependencies:
            result[dep.feature_name] = 0
    return result

def logtypes_with_dependencies(logtypes):
    old_deps = OrderedDict({ lt: 0 for lt in logtypes})
    new_deps = add_dependencies(old_deps)
    while len(new_deps) > len(old_deps):
        old_deps = new_deps
        new_deps = add_dependencies(new_deps)
    return list(new_deps.keys())
