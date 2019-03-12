import os
import sys
sys.path.append('..')
import logging
from argoDatabase import argoDatabase
import tmpFunctions as tf
from datetime import datetime
import warnings
from numpy import warnings as npwarnings
from addFunctions import format_logger, run_parallel_process
#  Sometimes netcdf contain nan. This will suppress runtime warnings.
warnings.simplefilter('error', RuntimeWarning)
npwarnings.filterwarnings('ignore')

dbName = 'argo'
npes = 4

if __name__ == '__main__':
    format_logger('list.log', level=logging.WARNING)
    platformList = ['4900902']
    ncFileDir = 'tmp'

    logging.warning('Starting add_from_tmp script')
    basinPath = os.path.join(os.path.pardir, 'basinmask_01.nc')
    logging.warning('Downloading Profile Indexes')
    df = tf.get_df_from_platform_list(platformList)
    logging.warning('Num of files downloading to tmp: {}'.format(df.shape[0]))
    tf.create_dir_of_files(df, tf.GDAC, tf.FTP, tf.tmpDir)
    logging.warning('Download complete. Now going to add to db: {}'.format(dbName))

    ad = argoDatabase(dbName,
                      'profiles',
                      replaceProfile=True,
                      qcThreshold='1', 
                      dbDumpThreshold=1000,
                      removeExisting=True
                      basinFilename=basinPath)
    files = ad.get_file_names_to_add(ncFileDir, howToAdd='profiles')
    run_parallel_process(ad, files, ncFileDir, npes)

    logging.warning('Cleaning up space')
    tf.clean_up_space()
    logging.warning('End of log file')