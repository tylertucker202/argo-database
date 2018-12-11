import os
import sys
sys.path.append('..')
import logging
from datetime import datetime, timedelta
import multiprocessing as mp
from numpy import array_split
from argoDatabase import argoDatabase
from from_tmp_functions import download_todays_file, get_df_of_files_to_add, merge_dfs, mp_create_dir_of_files, clean_up_space, get_last_updated, write_last_updated, StreamToLogger

import warnings
from numpy import warnings as npwarnings
#  Sometimes netcdf contain nan. This will suppress runtime warnings.
warnings.simplefilter('error', RuntimeWarning)
npwarnings.filterwarnings('ignore')

dbName = 'argo2'
npes = mp.cpu_count()
if __name__ == '__main__':
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    LOGFILENAME = 'fromTmp.log'
    OUTPUTDIR = os.path.join(os.getcwd(), 'tmp')
    HOMEDIR = os.getcwd()
    basinPath = os.path.join(os.path.pardir, 'basinmask_01.nc')
    collectionName = 'profiles'
    if os.path.exists(os.path.join(HOMEDIR, LOGFILENAME)):
        os.remove(LOGFILENAME)
    logging.basicConfig(format=FORMAT,
                        filename=LOGFILENAME,
                        level=logging.INFO)
    #  Sets output stream to logger
    stdout_logger = logging.getLogger('STDOUT')
    sl = StreamToLogger(stdout_logger, logging.INFO)
    sys.stdout = sl
    stderr_logger = logging.getLogger('STDERR')
    sl = StreamToLogger(stderr_logger, logging.ERROR)
    sys.stderr = sl
    logging.warning('Starting add_from_tmp script')
    ftpPath = os.path.join(os.sep, 'ifremer', 'argo')
    GDAC = 'ftp.ifremer.fr'
    todayDate = datetime.today().strftime('%Y-%m-%d')
    globalProfileName = 'ar_index_this_week_prof.txt'
    mixedProfileName = 'argo_merge-profile_index.txt'
    globalProfileIndex = os.path.curdir \
                       + os.sep + globalProfileName[:-4] \
                       + '-' + todayDate + '.txt'
    mixedProfileIndex = os.path.curdir \
                       + os.sep + mixedProfileName[:-4] \
                       + '-' + todayDate + '.txt'
    logging.warning('Downloading Profile Indexes')
    download_todays_file(GDAC, ftpPath, globalProfileIndex, globalProfileName)
    download_todays_file(GDAC, ftpPath, mixedProfileIndex, mixedProfileName)
    logging.warning('Generating dataframes')
    minDate = get_last_updated(filename='lastUpdated.txt')
    maxDate = datetime.today()
    logging.warning('minDate: {}'.format(minDate))
    logging.warning('maxDate: {}'.format(maxDate))
    dfGlobal = get_df_of_files_to_add(globalProfileIndex, minDate, maxDate)
    dfMixed = get_df_of_files_to_add(mixedProfileIndex, minDate, maxDate)
    df = merge_dfs(dfGlobal, dfMixed)
    print(df.shape[0])
    logging.warning('Num of files downloading to tmp: {}'.format(df.shape[0]))
    mp_create_dir_of_files(df, GDAC, ftpPath)
    logging.warning('Download complete. Now going to add to db: {}'.format(dbName))

    hostname = os.uname().nodename
    ad = argoDatabase(dbName,
                      collectionName,
                      replaceProfile=True,
                      qcThreshold='1', 
                      dbDumpThreshold=1000,
                      removeExisting=False,
                      testMode=False,
                      basinFilename=basinPath)
    files = ad.get_file_names_to_add(OUTPUTDIR)

    try:
        npes
    except NameError:
        npes = mp.cpu_count()
    fileArray = array_split(files, npes)
    processes = [mp.Process(target=ad.add_locally, args=(OUTPUTDIR, fileChunk)) for fileChunk in fileArray]
    for p in processes:
        p.start()
    for p in processes:
        p.join()
        
    logging.warning('setting date updated to: {}'.format(todayDate))
    write_last_updated(todayDate)

    logging.warning('Cleaning up space')
    clean_up_space(globalProfileIndex, mixedProfileIndex)
    logging.warning('End of log file')
