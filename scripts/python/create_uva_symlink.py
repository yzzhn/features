import os
import glob
from datetime import datetime
import logging
import re
import click


# raw log types
DEFAULT_LOG_TYPES = ['http', 'ssl', 'x509']


def temp(log_dir, root_dir, start_date, end_date, mute):

    log_types = DEFAULT_LOG_TYPES

    # convert to a datetime for comparison purposes
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # Create the directory for the raw type if it doesn't exist (in our root dir)
    for typ in log_types:
        log_type_dir = os.path.join(root_dir, typ)
        print(log_type_dir)
        
        if (not os.path.exists(log_type_dir)):
            if not mute:
                print(typ)
            try:
                original_umask = os.umask(0)
                os.makedirs(log_type_dir, 0o777)
            finally:
                os.umask(original_umask)

    # Get the list of dates that are the directories that contain raw logs
    log_dates = os.listdir(log_dir)
    log_dates.sort()
    
    log_dates.reverse()
    
    # for each date directory in /scratch/atn5vs/logs/uva/zeek/...
    for date in log_dates:
        if not mute:
            print(date + ' ---------------------------------------------------------------------------')
            
        if len(date) != 10:
            continue
        try:   
            dt_form = datetime.strptime(date, '%Y-%m-%d')
        except:
            continue
        
        ## specify your end date
        if dt_form > end_date:
            continue
        
        # check to see if that date is after the start date we specified
        if (dt_form >= start_date):
            #if dt_form > datetime(2021,1,3):
            #    continue
            # if it is, get the list of all files (raw logs) in that directory
            log_date_dir = os.path.join(log_dir, date, '*')
            filelist = glob.glob(log_date_dir)

            # for each of the known raw types
            for log_typ in log_types:
                # create a destination path for the symlinks in our root dir
                destination_dir = os.path.join(root_dir, log_typ, date)
                
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
                        
                        # then generate the destination path where we are going to write the symlink and create it
                        dest_filepath = os.path.join(destination_dir, re.sub(r"(_\d\d\d\d-\d\d\d\d)-\d\d\d\d\.log\.gz", r"\1.log.gz", src.split('/')[-1]))
                        
                        if (not os.path.exists(dest_filepath)):
                            if not mute:
                                print(dest_filepath)
                            os.symlink(src, dest_filepath)



@click.command()
@click.option('--log_dir', default='/data/ics383/ivy-hip-nta/data', help='The directory that contains all of the date dirs that contain logs.')
@click.option('--root_dir', default='/data/ics383/ivy-hip-nta/processed', help='The directory where all of the various feature types are contained.')
@click.option('--start_date', default='2022-10-12', help='The first date that we are going to be creating symlinks for. No date before this will have symlinks created.')
@click.option('--end_date', default='2022-10-26', help='The last date that we are going to be creating symlinks for. No date after this will have symlinks created.')
@click.option('--mute', default=False, help='Boolean for whether or not you want to mute the output.')
def main(log_dir, root_dir, start_date, end_date, mute):
    temp(log_dir, root_dir, start_date, end_date, mute)

if __name__ == '__main__':
    main()

