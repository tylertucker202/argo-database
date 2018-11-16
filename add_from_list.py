import os
import pandas as pd
import pdb
import logging
import multiprocessing as mp
from numpy import array_split
from argoDatabase import argoDatabase
import add_from_tmp as aft
from datetime import datetime
import re

def get_df_of_files_to_add_from_platform_list(filename, platformList):
    dfChunk = pd.read_csv(filename, sep=',', chunksize=100000, header=8)
    df = pd.DataFrame()
    for chunk in dfChunk:
        pdb.set_trace()
        chunk.drop(['latitude', 'longitude', 'institution', 'ocean', 'profiler_type'], axis=1, inplace=True)
        chunk['filename'] = chunk['file'].apply(lambda x: x.split('/')[-1])
        chunk['profile'] = chunk['filename'].apply(lambda x: re.sub('[MDAR(.nc)]', '', x))
        chunk['prefix'] = chunk['filename'].apply(lambda x: re.sub(r'[0-9_(.nc)]', '', x))
        chunk['platform'] = chunk['profile'].apply(lambda x: re.sub(r'(_\d{})', '', x))
        chunk.dropna(axis=0, how='any', inplace=True)
        chunk.date_update = pd.to_datetime(chunk.date_update.astype(int), format='%Y%m%d%H%M%S')
        chunk.date = pd.to_datetime(chunk.date.astype(int), format='%Y%m%d%H%M%S')
        chunk = chunk[chunk['platform'].isin(platformList)]
        df = pd.concat([df, chunk], axis=0, sort=False)
    return df

if __name__ == '__main__':
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    LOGFILENAME = 'addFromList.log'
    OUTPUTDIR = os.path.join(os.getcwd(), 'tmp')
    HOMEDIR = os.getcwd()
    dbName = 'argo2'
    collectionName = 'profiles'
    platformList = ['4900902']
    if os.path.exists(os.path.join(HOMEDIR, LOGFILENAME)):
        os.remove(LOGFILENAME)
    logging.basicConfig(format=FORMAT,
                        filename=LOGFILENAME,
                        level=logging.INFO)
    logging.warning('Starting add_from_tmp script')
    ftpPath = os.path.join(os.sep, 'ifremer', 'argo')
    GDAC = 'ftp.ifremer.fr'
    todayDate = datetime.today().strftime('%Y-%m-%d')
    globalProfileName = 'ar_index_global_prof.txt'  #  Use if going back more than a week
    mixedProfileName = 'argo_merge-profile_index.txt'
    globalProfileIndex = os.path.curdir \
                       + os.sep + globalProfileName.strip('.txt') \
                       + '-' + todayDate + '.txt'
    mixedProfileIndex = os.path.curdir \
                       + os.sep + mixedProfileName.strip('.txt') \
                       + '-' + todayDate + '.txt'
    logging.warning('Downloading Profile Indexes')
    aft.download_todays_file(GDAC, ftpPath, globalProfileIndex, globalProfileName)
    aft.download_todays_file(GDAC, ftpPath, mixedProfileIndex, mixedProfileName)
    logging.warning('Generating dataframes')
    dfGlobal = get_df_of_files_to_add_from_platform_list(globalProfileIndex, platformList)
    dfMixed = get_df_of_files_to_add_from_platform_list(mixedProfileIndex, platformList)
    pdb.set_trace()
    df = aft.merge_dfs(dfGlobal, dfMixed)
    logging.warning('Num of files downloading to tmp: {}'.format(df.shape[0]))
    aft.mp_create_dir_of_files(df, GDAC, ftpPath)
    logging.warning('Download complete. Now going to add to db: {}'.format(dbName))

    hostname = os.uname().nodename
    ad = argoDatabase(dbName,
                      collectionName,
                      replaceProfile=True,
                      qcThreshold='1', 
                      dbDumpThreshold=1000,
                      removeExisting=False,
                      testMode=False)
    files = ad.get_file_names_to_add(OUTPUTDIR, howToAdd='profiles')
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

    logging.warning('Cleaning up space')
    #aft.clean_up_space(localProfileIndex)
    logging.warning('End of log file')
    
