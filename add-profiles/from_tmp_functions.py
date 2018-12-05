import os
from ftplib import FTP
import pandas as pd
import re
import multiprocessing as mp
import tempfile
from numpy import array_split
import shutil

import warnings
from numpy import warnings as npwarnings
#  Sometimes netcdf contain nan. This will suppress runtime warnings.
warnings.simplefilter('error', RuntimeWarning)
npwarnings.filterwarnings('ignore')
def profiles_from_ftp(conn, filename):
    """Create an Argo profile object from a remote FTP file
    """
    conn.cwd(os.path.dirname(filename))
    with tempfile.NamedTemporaryFile(mode='w+b', dir=None, delete=True) as tmp:
        conn.retrbinary('RETR %s' % os.path.basename(filename), tmp.write)
        tmp.file.flush()
    return tmp

def download_todays_file(GDAC, ftpPath, profileIndex, profileText):
    with FTP(GDAC) as ftp:
        ftp.login()
        ftp.cwd(ftpPath)
        with open(profileIndex, "wb") as f:
            ftp.retrbinary("RETR " + profileText, f.write)

def get_df_of_files_to_add(filename, minDate, maxDate):
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
        chunk = chunk[(chunk['date_update'] >= minDate) & (chunk['date_update'] <= maxDate)]
        df = pd.concat([df, chunk])
    return df

def merge_dfs(dfCore, dfMixed):
    '''remove platforms from core that exist in mixed df'''
    rmPlat = dfMixed['platform'].unique().tolist()
    dfTruncCore = dfCore[ ~dfCore['platform'].isin(rmPlat)]
    df = pd.concat([dfMixed, dfTruncCore], axis=0, sort=False)
    return df

def create_dir_of_files(df, GDAC, ftpPath):
    with FTP(GDAC, timeout=10) as ftp:
        ftp.set_pasv(True)
        ftp.login()
        dacPath = os.path.join(ftpPath, 'dac')
        ftp.cwd(dacPath)
        for idx, row in df.iterrows():
            fileName = os.path.join( os.getcwd(), 'tmp' , row.file )
            if not os.path.exists(os.path.dirname(fileName)):
                os.makedirs(os.path.dirname(fileName))
            if os.path.exists(fileName):
                continue
            with open(fileName, "wb") as f:
                ftp.retrbinary("RETR " + row.file, f.write)

def mp_create_dir_of_files(df, GDAC, ftpPath, npes=None):
    if npes is None:
        npes = mp.cpu_count()
    if npes is 1:
        create_dir_of_files(df, GDAC, ftpPath)
    else:
        dfArray = array_split(df, npes)
        processes = [mp.Process(target=create_dir_of_files, args=(chunk, GDAC, ftpPath)) for chunk in dfArray]
        for p in processes:
            p.start()
        for p in processes:
            p.join()

def clean_up_space(globalProfileIndex, mixedProfileIndex):
    #remove indexList
    os.remove(globalProfileIndex)
    os.remove(mixedProfileIndex)
    #remove files in tmp
    fileName = os.path.join( os.getcwd(), 'tmp' )
    shutil.rmtree(fileName)
