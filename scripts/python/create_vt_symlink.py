import os
import glob
import datetime
import logging
import re

import click


# raw log types
DEFAULT_LOG_TYPES = ['http', 'ssl']
VT_FMT = "anon.{}.{}-{}.log.gz"
SYMLINK_FMT = "anon.{}_{}_{}_{}-{}.log.gz"


"""
### TODO: fix wrong log names
### Prior to 2020-03-06 VT pushed logs every one hour
### Since 2020-03-06 VT pushes log every half an hour
def vt_log_dur_config(logdatetime):
    if logdate < datetime.datetime(2020, 3, 6):
        return datetime.timedelta(minute=30)
    return datetime.timedelta(minute=30)


def reformat_log_ts(start_time, end_time, vt_dur_config):    
    if start_time.minute != 0 and start_time.minute != 30: 
        print("start time", start_time, "wrong fmt.")
    if start_time.minute < 30:
        start_time = datetime.datetime(dt_form.year, dt_form.month, dt_form.day, start_time.hour, 0)
    else: 
        start_time = datetime.datetime(dt_form.year, dt_form.month, dt_form.day, start_time.hour, 30)
        print("start time change to", start_time.strftime("%H:%M"))
                        
    if end_time.minute != 0 and end_time.minute != 30: 
        print("end time", end_time, "wrong fmt.")
        end_time = start_time + datetime.datetime.timedelta(minute=30)
        print("end time change to", end_time.strftime("%H:%M"))
"""

# create symlink from /scratch/mg2hk and rename 
def create_symlink(log_dir, root_dir, start_date, mute):
    log_types = DEFAULT_LOG_TYPES

    # convert to a datetime for comparison purposes
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')

    # Create the directory for the raw type if it doesn't exist (in our root dir)
    for typ in log_types:
        log_type_dir = os.path.join(root_dir, typ)
        print("working directory:", log_type_dir)
        
        if (not os.path.exists(log_type_dir)):
            if not mute:
                print(typ)
            try:
                original_umask = os.umask(0)
                os.makedirs(log_type_dir, 0o777)
            finally:
                os.umask(original_umask)

    # Get the list of dates that contain raw logs
    log_dates = os.listdir(log_dir)
    log_dates.sort()
    log_dates.reverse()
    
    # for each date directory in root_dir:
    for date in log_dates:
        if not mute:
            print(date + ' ---------------------------------------------------------------------------')

        if len(date) != 10:
            continue

        dt_form = datetime.datetime.strptime(date, '%Y-%m-%d')
        # check to see if that date is after the start date we specified
        if (dt_form < start_date) :
            break

        if (dt_form >= start_date) and (dt_form < datetime.datetime(2021, 6, 1)):
            # if it is, get the list of all files (raw logs) in that directory
            log_date_dir = os.path.join(log_dir, date, '*')
            filelist = glob.glob(log_date_dir)

            # for each of the known raw types
            for log_typ in log_types:
                # create a destination path for the symlinks in our root dir
                log_typ_vt = log_typ + "_vt"
                destination_dir = os.path.join(root_dir, log_typ_vt, date)
                
                # create that destination directory if it doesn't already exist (example: .../http/2019-08-01/...)
                if (not os.path.exists(destination_dir)):
                    try:
                        original_umask = os.umask(0)
                        os.makedirs(destination_dir, 0o777)
                    finally:
                        os.umask(original_umask)
                        
                # for each of the raw log files in filelist
                for src in filelist:
                    # if the filename includes the substring of a known raw log type
                    if (log_typ in src):
                        
                        # parsing vt log name, generate the destination path where we are going to write the symlink and create it
                        time_str = src.split('.')[-3]
                        start_time = time_str.split('-')[0]
                        end_time = time_str.split('-')[1]
                        start_time = datetime.datetime.strptime(start_time, '%H:%M:%S')
                        end_time = datetime.datetime.strptime(end_time, '%H:%M:%S')
                        
                        # create symlink path
                        dest_fname = SYMLINK_FMT.format(log_typ, "vt", dt_form.strftime("%Y%m%d"), start_time.strftime("%H%M"), end_time.strftime("%H%M"))
                        
                        dest_filepath = os.path.join(destination_dir, dest_fname)
                        print(dest_filepath)
                        if (not os.path.exists(dest_filepath)):
                            if not mute:
                                print(dest_filepath)
                            os.symlink(src, dest_filepath)



@click.command()
@click.option('--log_dir', default='/mnt/data/border/vt/zeek/', help='The directory that contains all of the date dirs that contain logs.')
@click.option('--root_dir', default='/mnt/chaseproject/uva/yz6me', help='The directory where all of the various feature types are contained.')
@click.option('--start_date', default='2021-04-01', help='The first date that we are going to be creating symlinks for. No date before this will have symlinks created.')
@click.option('--mute', default=False, help='Boolean for whether or not you want to mute the output.')
def main(log_dir, root_dir, start_date, mute):
    create_symlink(log_dir, root_dir, start_date, mute)

if __name__ == '__main__':
    main()
