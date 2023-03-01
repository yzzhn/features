#!/usr/bin/env bash

INSTALL_LOCATION=/home/$USER/local_pip
PYTHONPATH=$INSTALL_LOCATION:$PYTHONPATH

## first time install commands
pip install '.[dev]' -t $INSTALL_LOCATION -U --upgrade

## without python package dependencies
##pip install '.[dev]' -t $INSTALL_LOCATION -U --upgrade --no-deps


SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"

#$SCRIPTPATH/scripts/bash/package_spark_dependencies.sh

cd $INSTALL_LOCATION

############ NOTE #####################################
#
# If your tests need mock data or other resources that 
# are not source code, you need to add them to 
# MANIFEST.in so that those resources will be copied to
# the installation folder.
#
########################################################

#cd features/test

#pytest features/test

#cd -
