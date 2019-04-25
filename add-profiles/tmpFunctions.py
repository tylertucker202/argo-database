import os
from glob import glob
from ftplib import FTP
import wget
import pandas as pd
import re
import logging
import pdb
import multiprocessing as mp
import tempfile
from numpy import array_split
from datetime import datetime, timedelta
import warnings
from numpy import warnings as npwarnings
#  Sometimes netcdf contain nan. This will suppress runtime warnings.
warnings.simplefilter('error', RuntimeWarning)
npwarnings.filterwarnings('ignore')


todayDate = datetime.today().strftime('%Y-%m-%d')
globalProfileName = 'ar_index_this_week_prof.txt'
mixedProfileName = 'argo_merge-profile_index.txt'
globalProfileIndex = globalProfileName[:-4] \
                   + '-' + todayDate + '.txt'
mixedProfileIndex = mixedProfileName[:-4] \
                   + '-' + todayDate + '.txt'
ftpPath = os.path.join('ifremer', 'argo')
GDAC = 'ftp.ifremer.fr'
tmpDir = os.path.join(os.getcwd(), 'tmp/')
         
def download_todays_file(GDAC, ftpPath, profileIndex, profileText):
    url = 'ftp:/' + '/' + GDAC + '/' + ftpPath + '/' + profileText
    wget.download(url, profileIndex)

def get_df_of_files_to_add_from_platform_list(filename, platformList):
    dfChunk = pd.read_csv(filename, sep=',', chunksize=100000, header=8)
    df = pd.DataFrame()
    for chunk in dfChunk:
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

def get_df_from_platform_list(platformList):
    logging.warning('Downloading Profile Indexes')
    download_todays_file(GDAC, ftpPath, globalProfileIndex, globalProfileName)
    download_todays_file(GDAC, ftpPath, mixedProfileIndex, mixedProfileName)
    logging.warning('Generating dataframes')
    dfGlobal = get_df_of_files_to_add_from_platform_list(globalProfileIndex, platformList)
    dfMixed = get_df_of_files_to_add_from_platform_list(mixedProfileIndex, platformList)
    df = merge_dfs(dfGlobal, dfMixed)
    return df

def get_df_from_dates_updated(minDate, maxDate):
    logging.warning('Downloading Profile Indexes')
    download_todays_file(GDAC, ftpPath, globalProfileIndex, globalProfileName)
    download_todays_file(GDAC, ftpPath, mixedProfileIndex, mixedProfileName)
    logging.warning('Generating dataframes')
    logging.warning('minDate: {}'.format(minDate))
    logging.warning('maxDate: {}'.format(maxDate))
    dfGlobal = get_df_of_files_to_add(globalProfileIndex, minDate, maxDate, dateCol='date_update')
    dfMixed = get_df_of_files_to_add(mixedProfileIndex, minDate, maxDate, dateCol='date_update')
    df = merge_dfs(dfGlobal, dfMixed)
    return df

def get_df_from_dates(minDate, maxDate):
    download_todays_file(GDAC, ftpPath, globalProfileIndex, globalProfileName)
    download_todays_file(GDAC, ftpPath, mixedProfileIndex, mixedProfileName)
    logging.warning('Generating dataframes')
    logging.warning('minDate: {}'.format(minDate))
    logging.warning('maxDate: {}'.format(maxDate))
    dfGlobal = get_df_of_files_to_add(globalProfileIndex, minDate, maxDate, dateCol='date')
    dfMixed = get_df_of_files_to_add(mixedProfileIndex, minDate, maxDate, dateCol='date')
    df = merge_dfs(dfGlobal, dfMixed)
    return df

def get_df_of_files_to_add(filename, minDate, maxDate, dateCol='date_update'):
    dfChunk = pd.read_csv(filename, sep=',', chunksize=100000, header=8)
    df = pd.DataFrame()
    for chunk in dfChunk:
        chunk.drop(['latitude', 'longitude', 'institution', 'ocean', 'profiler_type'], axis=1, inplace=True)
        chunk['filename'] = chunk['file'].apply(lambda x: x.split('/')[-1])
        chunk['profile'] = chunk['filename'].apply(lambda x: re.sub('[MDAR(.nc)]', '', x))
        chunk['prefix'] = chunk['filename'].apply(lambda x: re.sub(r'[0-9_(.nc)]', '', x))
        chunk['platform'] = chunk['profile'].apply(lambda x: re.sub(r'(_\d{3})', '', x))
        chunk.dropna(axis=0, how='any', inplace=True)
        chunk['date_update'] = pd.to_datetime(chunk['date_update'].astype(int), format='%Y%m%d%H%M%S')
        chunk['date'] = pd.to_datetime(chunk['date'].astype(int), format='%Y%m%d%H%M%S')
        chunk = chunk[(chunk[dateCol] >= minDate) & (chunk[dateCol] <= maxDate)]
        df = pd.concat([df, chunk])
    return df

def merge_dfs(dfCore, dfMixed):
    '''remove platforms from core that exist in mixed df'''
    rmPlat = dfMixed['platform'].unique().tolist()
    dfTruncCore = dfCore[ ~dfCore['platform'].isin(rmPlat)]
    df = pd.concat([dfMixed, dfTruncCore], axis=0, sort=False)
    return df
                
def create_dir_of_files(df, GDAC, ftpPath, tmpDir):
    tmpFileName = 'tmp-daily-files.txt'
    df['file'].to_csv(tmpFileName, index=None)
    rsyncCommand = 'rsync -arvzhim --files-from=' + \
                   tmpFileName + ' vdmzrs.ifremer.fr::argo ' + \
                   tmpDir + ' > tmp-rsync.txt'
    os.system(rsyncCommand)
    os.remove(tmpFileName)
            
def get_last_updated(filename='lastUpdated.txt'):
    if not os.path.exists(filename):
        print('{} does not exist. assuming yesterday'.format(filename))
        return datetime.today() - timedelta(days=1)
    with open(filename, 'r') as f:
        dateStr = f.read()
    dateStr = dateStr.strip('\n')
    date = datetime.strptime(dateStr, '%Y-%m-%d')
    return date
    
def write_last_updated(date, filename='lastUpdated.txt'):
    with open(filename, 'w') as f:
        f.write(date)    

def clean_up_space(globalProfileIndex='argo_merge-profile_index*.txt', mixedProfileIndex='ar_index_this_week_prof-*.txt', tmpDir='tmp/'):
    #remove indexList
    files = [ globalProfileIndex, mixedProfileIndex ]
    files = [ os.path.join(os.getcwd(), f) for f in files ]
    for file in files:
        if len(glob(file)) >= 1:
            os.system('rm ' + file)

    if os.path.exists(tmpDir):
        os.system('rm -r '+ tmpDir)
