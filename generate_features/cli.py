import os
import sys
import time
import random
from datetime import datetime, timedelta
import logging
from itertools import groupby
from collections import OrderedDict

import click
import pyarrow.parquet

from generate_features import available_features, logtypes_with_dependencies
from generate_features.generate_feats import GenerateFeatures
from generate_features.slurm import SlurmGenerateFeatures, sacct, markdown
from generate_features.scheduler import ScheduleGenerateFeatures
from generate_features.helpers import generate_time_list, parquet_exists, form_parquet_filepath, read_parquet, parse_time
from generate_features.spark_utils.context import spark_tmp_dir

from collections import namedtuple

Feature = namedtuple('Feature', 'feature start_time end_time exists')

def list_feature(root_dir, logtype, start_time, end_time):
    
    if logtype not in available_features:
        raise Exception(str(logtype) + ' feature is not supported.  Supported features are:\n' + str(list(available_features.keys())))

    if (start_time >= end_time):
        raise Exception('start_time must be less than end_time')
    
    feature = available_features[logtype]
    time_frames = generate_time_list(start_time, end_time, feature.timescale)

    return  [ Feature(feature, t[0], t[1], parquet_exists(form_parquet_filepath(root_dir, logtype, t[0], t[1]))) for t in time_frames ]

def random_feature(root_dir, logtype, start_time, end_time):
    return random.choice([f for f in list_feature(root_dir, logtype, start_time, end_time) if not f.exists])

available_logtypes = list(available_features.keys())

yesterday = str(datetime.today())[:10]

@click.group(context_settings=dict(terminal_width=110))
def cli():
    pass

def feature_ranges(root_dir, start_time, end_time, logtypes, existing):
    existing_str = "missing"
    if existing:
      existing_str = "existing"
    for logtype in logtypes:
        grouped = groupby(list_feature(root_dir, logtype, start_time, end_time), lambda f: f.exists)
        print()
        print("[  {}  {} ]".format(existing_str, logtype))
        for group in grouped:
            if group[0] == existing:
                values = list(group[1])
                print("   {}   --->   {}".format(values[0].start_time, values[-1].end_time))

def get_logtypes(root_dir, logtypes):
    if not logtypes:
        logtypes = [ logtype for logtype in os.listdir(root_dir) if logtype in available_features ]
    return logtypes


def form_parquet_filepath_with_columns(root_dir, logtype, start_dt, end_dt):
    filepath = form_parquet_filepath(root_dir, logtype, start_dt, end_dt)
    ds = pyarrow.parquet.ParquetDataset(filepath)
    cols = [col.name for col in ds.schema]
    return filepath + ' --- ' + str(os.path.getmtime(filepath))  + ' --- ' + str(len(cols)) + ' --- ' + str(cols)


def feature_columns_to_md(logtype):
    try:
        root_dir = '/scratch/jjp3n/test_dir/'
        start_time = datetime(2019, 8, 6, hour=0, minute=0)
        end_time = datetime(2019, 8, 6, hour=0, minute=30)
        
        sys.stdout = open(os.devnull, "w")
        df = GenerateFeatures(root_dir, logging_disabled=True).get_feature(logtype, start_time, end_time)
        sys.stdout = sys.__stdout__

        result = '**columns** \n'
        for col in sorted(list(df.columns)):
            result += ('* ' + str(col) + '\n')
        return result
    except:
        return None
    

DEFAULT_ROOT_DIR = "/mnt/chaseproject/uva/yz6me/"
DEFAULT_START_TIME = "2019-08-01 00:00:00"

###################################
#### deprecated the ls command ####
###################################

#@cli.command()
#@click.argument('logtypes', nargs=-1) #, help='desired log or feature type in [{}]'.format(", ".join(available_logtypes)))
#@click.option('--root_dir', default=DEFAULT_ROOT_DIR, type=click.Path(), help='Root dir containing logs, features and feeds.')
#@click.option('--start_time', default=DEFAULT_START_TIME, help='start time (default: "{}") either in "%Y-%m-%d" or in "%Y-%m-%d %H:%M:%S" format'.format(DEFAULT_START_TIME))
#@click.option('--end_time', default=yesterday, help='end date (default: "{}") either in "%Y-%m-%d" or in "%Y-%m-%d %H:%M:%S" format'.format(yesterday[:10]))
#@click.option('--output_format', default="ranges", type=click.Choice(['paths', 'columns', 'ranges', 'options']), help='print list of paths, datetime ranges (default), or CLI options that can be passed to subsequent invocations of the `features` command.')
#@click.option('--existing/--missing', default=True, help='only list existing (default) or missing features')
#def ls(logtypes, root_dir, start_time, end_time, existing, output_format):
#   """List parquets that exist or are missing for given logtype(s), from a `start_time` to an `end_time`, in various output formats.
#
#    Examples:
#    features ls http renisac
#    features ls --output_format ranges http renisac
#    features ls --root_dir /scratch/jjp3n/root_dir --start_time '2019-08-07 00:00:00' --end_time '2019-10-25 00:00:00' --output_format ranges http renisac
#    """
#    start_time = parse_time(start_time)
#    end_time = parse_time(end_time)
#    logtypes = get_logtypes(root_dir, logtypes)
#    if output_format == 'ranges':
#        feature_ranges(root_dir, start_time, end_time, logtypes, existing)
#    else:
#        if output_format == 'paths':
#            def output(f):
#                print(form_parquet_filepath(root_dir, logtype, f.start_time, f.end_time))
#        elif output_format == 'columns':
#            def output(f):
#                print(form_parquet_filepath_with_columns(root_dir, logtype, f.start_time, f.end_time))
#        else:
#            def output(f):
#                print("--start_time '{}' --end_time '{}' {}".format(f.start_time, f.end_time, f.feature.name))
#        for logtype in logtypes:
#            for f in list_feature(root_dir, logtype, start_time, end_time):
#                if f.exists == existing:
#                    output(f)


#@cli.command()
#@click.argument('logtypes', nargs=-1) #, help='desired log or feature type in [{}]'.format(", ".join(available_logtypes)))
#@click.option('--root_dir', default=DEFAULT_ROOT_DIR, type=click.Path(), help='Root dir containing zeek logs, features and feeds.')
#@click.option('--start_time', default="2020-08-01 00:00:00", help='start time (default: "{}") either in "%Y-%m-%d" or in "%Y-%m-%d %H:%M:%S" format'.format(DEFAULT_START_TIME))
#@click.option('--end_time', default="2020-08-02 00:00:00", help='end date (default: "{}") either in "%Y-%m-%d" or in "%Y-%m-%d %H:%M:%S" format'.format(yesterday[:10]))
#def reachable(logtypes, root_dir, start_time, end_time):
#    """Returns a set of parquet filepaths for features/dependencies not able to be computed from given logtype(s), from a `start_time` to an `end_time`.
#    """
#    start_t = parse_time(start_time)
#    end_t = parse_time(end_time)
#    print(logtypes)
#    if not logtypes:
#        logtypes = list(available_features.keys())
#    for logtype in logtypes:
#        missing_logs = ReachableGenerateFeatures(root_dir, logging_disabled=False).get_feature(logtype, start_t, end_t)
#        for log in missing_logs:
#            print("CAN'T COMPUTE: ", log)

@cli.command()
@click.argument('logtypes', nargs=-1) #, help='desired log or feature type in [{}]'.format(", ".join(available_logtypes)))
@click.option('--root_dir', default=DEFAULT_ROOT_DIR, type=click.Path(), help='Root dir containing zeek logs, features and feeds.')
@click.option('--start_time', default=DEFAULT_START_TIME, help='start time (default: "{}") either in "%Y-%m-%d" or in "%Y-%m-%d %H:%M:%S" format'.format(DEFAULT_START_TIME))
@click.option('--end_time', default=yesterday, help='end date (default: "{}") either in "%Y-%m-%d" or in "%Y-%m-%d %H:%M:%S" format'.format(yesterday[:10]))
@click.option('--save_dir', default=None, help='save parquets in this directory')
@click.option('--pick-one-random/--all', default=False, help='only compute one randomly chosen parquet / compute them all')
def compute(logtypes, root_dir, start_time, end_time, save_dir, pick_one_random):
    """Compute and save the parquet file(s) for given logtype(s), from a `start_time` to an `end_time`.

    \b
    Example:
    features compute --save_dir /scratch/my/favorite/root --start_time "2019-08-07 02:30:00" --end_time "2019-08-07 03:30:00" made_features
    """
    try: 
        start_t = parse_time(start_time)
        end_t = parse_time(end_time)
        if not logtypes:
            logtypes = list(available_features.keys())
        print("save_dir", save_dir)
        for logtype in logtypes:
            if pick_one_random:
                f = random_feature(root_dir, logtype, start_t, end_t)
                start_time, end_time = f.start_time, f.end_time
            else:
                start_time, end_time = start_t, end_t
            GenerateFeatures(root_dir).get_feature(logtype, start_time, end_time, save_dir=save_dir)
    finally:
        if spark_tmp_dir is not None:
            spark_tmp_dir.cleanup()   
"""

@cli.command()
@click.argument('logtypes', nargs=-1) #, help='desired log or feature type in [{}]'.format(", ".join(available_logtypes)))
@click.option('--root_dir', default=DEFAULT_ROOT_DIR, type=click.Path(), help='Root dir containing zeek logs, features and feeds.')
@click.option('--start_time', default=DEFAULT_START_TIME, help='start time (default: "{}") either in "%Y-%m-%d" or in "%Y-%m-%d %H:%M:%S" format'.format(DEFAULT_START_TIME))
@click.option('--end_time', default=yesterday, help='end date (default: "{}") either in "%Y-%m-%d" or in "%Y-%m-%d %H:%M:%S" format'.format(yesterday[:10]))
@click.option('--save_dir', default=None, help='save parquets in this directory')
@click.option('--pick-one-random/--all', default=False, help='only compute one randomly chosen parquet / compute them all')
@click.option('--account', default='netsec', help="SLURM account (default: 'netsec')")
@click.option('--partition', default='pcore', help="SLURM partition (default: 'pcore')")
@click.option('--mock/--for-real', default=False, help="only print commands that would be run in separate SLURM jobs / actually run SLURM jobs for real (default)")
@click.option('--mem-per-cpu', default=36000, help="RAM allocated per cpu in Mb (default: 36000)")
@click.option('--ntasks-per-node', default=1, help="CPU allocated per Node (default: 1)")
def slurm(logtypes, root_dir, start_time, end_time, save_dir, pick_one_random, account, partition, mock, mem_per_cpu, ntasks_per_node):
    #Schedule slurm jobs to compute and save parquets for given logtype(s), from a `start_time` to an `end_time`.
    
    start_t = parse_time(start_time)
    end_t = parse_time(end_time)
    if not logtypes:
        logtypes = list(available_features.keys())
    print("save_dir", save_dir)
    for logtype in logtypes:
        if pick_one_random:
            f = random_feature(root_dir, logtype, start_t, end_t)
            start_time, end_time = f.start_time, f.end_time
        else:
            start_time, end_time = start_t, end_t
        SlurmGenerateFeatures(root_dir, account=account, partition=partition, mem_per_cpu=mem_per_cpu, mock=mock, ntasks_per_node=ntasks_per_node).get_feature(logtype, start_time, end_time, save_dir=save_dir, allow_missing=True)
"""


@cli.command()
@click.argument('logtypes', nargs=-1) #, help='desired log or feature type in [{}]'.format(", ".join(available_logtypes)))
@click.option('--root_dir', default=DEFAULT_ROOT_DIR, type=click.Path(), help='Root dir containing zeek logs, features and feeds.')
@click.option('--start_time', default=DEFAULT_START_TIME, help='start time (default: "{}") either in "%Y-%m-%d" or in "%Y-%m-%d %H:%M:%S" format'.format(DEFAULT_START_TIME))
@click.option('--end_time', default=yesterday, help='end date (default: "{}") either in "%Y-%m-%d" or in "%Y-%m-%d %H:%M:%S" format'.format(yesterday[:10]))
@click.option('--save_dir', default=None, help='save parquets in this directory')
@click.option('--pick-one-random/--all', default=False, help='only compute one randomly chosen parquet / compute them all')
@click.option('--mock/--for-real', default=False, help="only print commands that would be run in separate SLURM jobs / actually run SLURM jobs for real (default)")
def schedule(logtypes, root_dir, start_time, end_time, save_dir, pick_one_random, mock):
    """Schedule slurm jobs to compute and save parquets for given logtype(s), from a `start_time` to an `end_time`.
    """ 
    start_t = parse_time(start_time)
    end_t = parse_time(end_time)
    if not logtypes:
        logtypes = list(available_features.keys())
    print("save_dir", save_dir)
    for logtype in logtypes:
        if pick_one_random:
            f = random_feature(root_dir, logtype, start_t, end_t)
            start_time, end_time = f.start_time, f.end_time
        else:
            start_time, end_time = start_t, end_t
        ScheduleGenerateFeatures(root_dir, save_dir=save_dir, mock=mock).get_feature(logtype, start_time, end_time, save_dir=save_dir, allow_missing=True)
 
"""
@cli.command()
@click.argument('logtypes', nargs=-1) #, help='desired log or feature type in [{}]'.format(", ".join(available_logtypes)))
@click.option('--root_dir', default=DEFAULT_ROOT_DIR, type=click.Path(), help='Root dir containing slurm-logs, zeek logs, features and feeds.')
@click.option('--start_time', default=DEFAULT_START_TIME, help='start time (default: "{}") either in "%Y-%m-%d" or in "%Y-%m-%d %H:%M:%S" format'.format(DEFAULT_START_TIME))
@click.option('--end_time', default=yesterday, help='end date (default: "{}") either in "%Y-%m-%d" or in "%Y-%m-%d %H:%M:%S" format'.format(yesterday[:10]))
@click.option('--scheduled-within-hours', default=1.0, help='only show slurm jobs scheduled since this many hours ago (default: 1.0)')
@click.option('--partition', default='pcore', help="SLURM partition (default: 'pcore')")
@click.option('--prettify/--just-markdown', default=False, help='prettify markdown for colorized display in terminal (default: --just-markdown)')
def monitor_slurm(logtypes, root_dir, start_time, end_time, scheduled_within_hours, partition, prettify):
    #Outputs a markdown-formatted report about slurm jobs sheduled in the last few hours. Reports the status of each job including the error log of failed jobs.
    
    start_t = parse_time(start_time)
    end_t = parse_time(end_time)
    parquets = sacct(datetime.now()-timedelta(hours=scheduled_within_hours), start_dt=start_t, end_dt=end_t, logtypes=logtypes, partition=partition)
    md = markdown(parquets)
    if prettify:
        print( mdv.main(md) )
    else:
        print( md )
"""

"""
@cli.command()
@click.argument('logtypes', nargs=-1)
@click.option('--dependencies/--no-dependencies', default=True, help='include dependencies in logtypes')
@click.option('--prettify/--just-markdown', default=False, help='prettify markdown for colorized display in terminal (default: --just-markdown)')
def info(logtypes, dependencies, prettify):
    #Outputs a markdown-formatted documentation about available features/logtypes.

    #\b
    #Examples:
    #features info made_url
    #features info --no-dependencies made_url
    #features info
    
    if not logtypes:
        logtypes = list(available_features.keys())
    if dependencies:
        logtypes = logtypes_with_dependencies(logtypes)
    logtypes.reverse()
    mdv.term_columns = 92
    md = "\n# current features"
    for logtype in logtypes:
        feature = available_features[logtype]
        md += "\n" + feature.markdown()

        columns = feature_columns_to_md(logtype) 
        if columns:
            md += columns
    if prettify:
        print( mdv.main(md) )
    else:
        print( md )
"""

@cli.command()
@click.argument('logtype', nargs=1)
@click.argument('score_column', nargs=1)
@click.argument('display_columns', nargs=-1)
@click.option('--root_dir', default=DEFAULT_ROOT_DIR, type=click.Path(), help='Root dir containing zeek logs, features and feeds.')
@click.option('--start_time', default=DEFAULT_START_TIME, help='start time (default: "{}") either in "%Y-%m-%d" or in "%Y-%m-%d %H:%M:%S" format'.format(DEFAULT_START_TIME))
@click.option('--end_time', default=yesterday, help='end date (default: "{}") either in "%Y-%m-%d" or in "%Y-%m-%d %H:%M:%S" format'.format(yesterday[:10]))
@click.option('--k', default=42, help='the "k" in "topk"; how many topk to display')
def topk(logtype, score_column, display_columns, k, root_dir, start_time, end_time):
    """Prints out the top k rows of a log as sorted by a given column.

    \b
    Example:
    feature topk --k 42 --start_time '2019-08-06 00:30:00' --end_time '2019-08-06 01:00:00' made_bayes score host
        displays the `host` and `score` fields for the top 42 rows of `made_bayes` ordered by `score.`
    """
    start_time = parse_time(start_time)
    end_time = parse_time(end_time)
    features = GenerateFeatures(root_dir)
    df = features.get_feature(logtype, start_time, end_time, 'pandas')
    df = df.sort_values(by=score_column, ascending=False)
    if display_columns:
        display_columns = list(display_columns)
        if score_column not in display_columns:
            display_columns = [score_column] + display_columns
        df = df[display_columns]
    print(df.head(k))


if __name__ == '__main__':
    cli()
