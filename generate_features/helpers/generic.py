import os
import logging
import shutil


def parquet_open_permissions(out_path):
    ''' Used for opening permissions up to other users.
        Spark writes parquet files inside of a subdirectory
        so os.walk is used to set those file permissions.
    '''

    if os.path.isfile(out_path): # pandas parquet file
        try:
            os.chmod(out_path, 0o777)
        except:
            logging.warning('Unable to give permissions to: ' + out_path)
            pass
    else: # spark parquet dir
        for root, dirs, files in os.walk(out_path, topdown=False):
            try:
                os.chmod(root, 0o777)
            except:
                logging.warning('Unable to give permissions to: ' + root)
                pass

            for fil in files:
                try:
                    filepath = os.path.join(root, fil)
                    os.chmod(filepath, 0o777)
                except:
                    logging.warning('Unable to give permissions to: ' + filepath)
                    pass


def remove_filepath(path):
    if os.path.exists(path) and ('.parquet' in path):
        if (os.path.isfile(path)):
            os.remove(path)
        else:
            shutil.rmtree(path)
