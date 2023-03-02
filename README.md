
# Features Framework 
This python framework is used for accessing and defining feature dataframes that are derived from raw logs (zeek and other feeds), interchangeably as pyspark or pandas dataframes. It saves dataframes into a repository of **parquet** files that can be shared between users, affording users often massive speedups over reading in the original raw logs or using file formats such as csv to save intermediate dataframes.

## Installation

From the toplevel `features/` dir where `setup.py` lives:


```bash
pip install '.[dev]' -t /home/$USER/local_pip --upgrade
```
​This will install all required dependencies in `/home/$USER/local_pip`.

Once installed, for speedier builds while developing `features`, instead you can build without re-installing dependencies:

```bash
pip install '.[dev]' -t /home/$USER/local_pip --upgrade -U --no-deps
```

Alternatively, read instructions in `reinstall.sh` for installation.

## Bash Config
To enable command line tools, open `~/.bashrc` and write the following at the end of the file:

```bash
export PYTHONPATH="/home/$USER/local_pip:/home/$USER/local_pip/bin:$PYTHONPATH"
PATH="/home/$USER/features:/home/$USER/local_pip:/home/$USER/local_pip/bin/:$PATH"
```
Save the `~/.bashrc` and run:

```bash
source ~/.bashrc
```

## Test
To test if installation and configurate are successful, open terminal and run: 

```bash
features --help
``` 
Successful configuration would return something similar like the following:

```bash
[INFO] [2023-03-01 19:48:12,313]: Create Schedule Scripts Dir.
Usage: features [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  compute   Compute and save the parquet file(s) for given logtype(s), from a `start_time` to an `end_time`.
  schedule  Schedule slurm jobs to compute and save parquets for given logtype(s), from a `start_time` to...
  topk      Prints out the top k rows of a log as sorted by a given column.
```


<!---
<[FEATURE_DOCS.md](https://code.vt.edu/p-core/features/blob/master/FEATURES_DOCS.md) contains the list of currently defined feature dataframes, with metadata and descriptions.>
#<[CRON_DOCS.md](https://code.vt.edu/p-core/features/blob/develop/CRON_DOCS.md) contains the list of descriptions of the features computed by the cron job, along with frequencies and locations.>
-->
# Usage

### Python API

`GenerateFeatures` provides users with a simple interface to:
- compute features (raw or derived) of the form `get_feature(feature_type, start_datetime, end_datetime; kwargs=...)`
- define new features (in the `generate_features/features` directory) that depend on other features

If a feature dataframe or its dependencies have their `memoize` flag set to `True`, they will be saved as shared `.parquet` files anytime they are computed. Permissions on the folder containing parquet files are set so they can live in a common folder shared by multiple users, so that users benefit from other users' previous computations.


Here is the framework in action (as a python library):

```python
import datetime
from generate_features.generate_feats import GenerateFeatures

# start and end times must be datetime objects
start_dt = datetime.datetime(2019, 8, 6, hour=0, minute=0)
end_dt = datetime.datetime(2019, 8, 6, hour=0, minute=30)

# the "root_dir" is the shared directory where features and intermediates features are stored as parquet files
<features = GenerateFeatures('/scratch/jjp3n/root_dir/')>

# in this case we are requesting a raw http log as a pandas dataframe between our start and end times
df1 = features.get_feature('http', start_dt, end_dt, 'pandas')

start_dt = datetime.datetime(2019, 8, 6, hour=0, minute=0)
end_dt = datetime.datetime(2019, 8, 7, hour=0, minute=0)

# in this case we are computing the popularity_host feature which is returned as a spark dataframe and depends
# on 24 hours worth of http logs
df2 = features.get_feature('popularity_http', start_dt, end_dt, 'spark')
```

<The framework is designed to make it easy to define new features, [here](https://github.com/yizhezhang07/features/blob/master/generate_features/features/ssh_conn/__init__.py) is an example.>


### Command line tools

There is a suite of command line tools for computing and saving feature dataframes in-process or via SLURM jobs.  Some common use-cases are:
#### Sequential computing
The `features compute` command run jobs sequentially in the terminal:
`features compute --root_dir=$logdir --save_dir=$savedir --start_time "2022-10-12 00:00:00" --end_time "2022-10-26 00:00:00" x509` <br>
compute and save x509 features from 2022-10-12 00:00:00 to 2022-10-26 00:00:00. The feature `x509` is defined [here](https://github.com/yizhezhang07/features/blob/master/generate_features/features/zeek_logs/__init__.py).

#### Parallel computing 
When there's no dependency in the features, the `features schedule`  command enable simple parallel computing.  
`features schedule --root_dir=$logdir --save_dir=$savedir --start_time "2022-10-12 00:00:00" --end_time "2022-10-26 00:00:00" x509` <br>
generates 30-min scripts that compute and save x509 features from 2022-10-12 00:00:00 to 2022-10-26 00:00:00. 

The scripts are saved in folder `scheduled-scripts`. We then run:
```bash
./jobscheduler.sh 
```
to run scripts in the terminal. Read `jobscheduler.sh` for configurations.

Errors are logged in seperate folders. To clean up log files, run:
```bash
./cleanup.sh
````

<!--
`features slurm --start_time 2019-11-13 --mem-per-cpu 42000 made_features`  <br>
schedules SLURM jobs to compute and save `made_features` and its dependencies from Nov. 13 til yesterday; this will save job stdout and stderr in `$PWD/logs` <br> <br>


From the same folder as where you launched features slurm (because this looks for slurm logs in `$PWD/logs`):

`features monitor-slurm --scheduled-within-hours 12` <br>
will output a markdown-formatted report on slurm jobs that were launched over ther last 12 hours; in particular, the section on jobs that failed show up with appropriate stack traces! <br> <br>


`features ls` <br>
lists all the time ranges over which parquet files have been saved

See [CLI_DOCS.md](https://code.vt.edu/p-core/features/blob/columns_in_features_info/CLI_DOCS.md) for complete command line tool docs.                                                                                                                                   

# Rivanna Setup

If working on Rivanna, run the following commands:
```bash
# 1. switch to python 3.6
module load anaconda/5.2.0-py3.6
# Note: add this to ~/.bashrc or ~/.zsh file

# 2. get an ijob for more memory
ijob -c 4 --mem-per-cpu=36000 -p pcore --time=03:00:00

# 3. create a local pip directory for having permission to install python packages
mkdir /home/$USER/local_pip

# 4. add your local pip to your python path
export PYTHONPATH=$PYTHONPATH:~/local_pip
# Note: add this to ~/.bashrc or ~/.zsh /home/$USER/local_pipfile

```

# Install

From the toplevel `features/` dir where `setup.py` lives:

```bash
pip install '.[dev]' -t /home/$USER/local_pip --upgrade
```

Once installed, for speedier builds while developing `features`, instead you can build without re-installing dependencies:

```bash
pip install '.[dev]' -t /home/$USER/local_pip --upgrade -U --no-deps
```
       
# Test

Navigate to the integration tests directory: <br>
``` bash
cd ./generate_features/tests/integration_tests
```
-->