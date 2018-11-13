import os
#import ftputil  #also an option
from ftplib import FTP
import pandas as pd
import pdb
import logging
from datetime import datetime, timedelta
import multiprocessing as mp
import tempfile
from numpy import array_split
from argoDatabase import argoDatabase
import shutil

def profiles_from_ftp(conn, filename):
    """Create an Argo profile object from a remote FTP file
    """
    conn.cwd(os.path.dirname(filename))
    with tempfile.NamedTemporaryFile(mode='w+b', dir=None, delete=True) as tmp:
        conn.retrbinary('RETR %s' % os.path.basename(filename), tmp.write)
        tmp.file.flush()
    return tmp

def download_todays_file():
    with FTP(GDAC) as ftp:
        ftp.login()
        ftp.cwd(ftpPath)
        with open(localProfileIndex, "wb") as f:
            ftp.retrbinary("RETR " + profileText, f.write)
    
def get_df_of_files_to_add(filename):
    dfChunk = pd.read_csv(filename, sep=',', chunksize=100000, header=8)
    df = pd.DataFrame()
    for chunk in dfChunk:
        chunk.dropna(axis=0, how='any', inplace=True)
        filteredIndex = chunk['file'].filter(regex=reBR, axis=0).index
        chunk = chunk.loc[filteredIndex]
        chunk.date_update = pd.to_datetime(chunk.date_update.astype(int), format='%Y%m%d%H%M%S')
        chunk.date = pd.to_datetime(chunk.date.astype(int), format='%Y%m%d%H%M%S')
        chunk = chunk[(chunk.date_update >= minDate) & (chunk.date_update <= maxDate)]
        df = pd.concat([df, chunk])
    return df

def create_dir_of_files(df):
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

def mp_create_dir_of_files(df, npes=None):
    if npes is None:
        npes = mp.cpu_count()
    if npes is 1:
        create_dir_of_files(df)
    else:
        dfArray = array_split(df, npes)
        processes = [mp.Process(target=create_dir_of_files, args=(chunk,)) for chunk in dfArray]
        for p in processes:
            p.start()
        for p in processes:
            p.join()
            
def clean_up_space():
    #remove indexList
    os.remove(localProfileIndex)
    #remove files in tmp
    fileName = os.path.join( os.getcwd(), 'tmp' )
    shutil.rmtree(fileName)

    

if __name__ == '__main__':
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    LOGFILENAME = 'addFromTmp.log'
    OUTPUTDIR = os.path.join('/home', 'tyler', 'Desktop', 'argo-database', 'tmp')
    HOMEDIR = os.getcwd()
    dbName = 'argo-tmp'
    collectionName = 'profiles'
    if os.path.exists(os.path.join(HOMEDIR, LOGFILENAME)):
        os.remove(LOGFILENAME)
    logging.basicConfig(format=FORMAT,
                        filename=LOGFILENAME,
                        level=logging.INFO)
    logging.warning('Starting add_from_tmp script')
    ftpPath = os.path.join(os.sep, 'ifremer', 'argo')
    GDAC = 'ftp.ifremer.fr'
    todayDate = datetime.today().strftime('%Y-%m-%d')
    profileText = 'ar_index_global_prof.txt'
    localProfileIndex = os.path.curdir \
                       + os.sep \
                       + 'profile-indexes' \
                       + os.sep + profileText.strip('.txt') \
                       + '-' + todayDate + '.txt'
    reBR = r'^(?!.*BR\d{1,})' # ignore characters starting with BR followed by a digit
    logging.warning('Downloading Profile Indexes')
    download_todays_file()
    logging.warning('Generating dataframe')
    minDate = datetime.today() - timedelta(days=1)
    maxDate = datetime.today()
    logging.warning('minDate: {}'.format(minDate))
    logging.warning('maxDate: {}'.format(maxDate))
    df = get_df_of_files_to_add(localProfileIndex)
    print(df.shape)
    logging.warning('Num of files downloading to tmp: {}'.format(df.shape[0]))
    mp_create_dir_of_files(df)
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
        
    #ad.add_locally(OUTPUTDIR, files)
    logging.warning('Total documents added: {}'.format(ad.totalDocumentsAdded))
    
    logging.warning('Cleaning up space')
    clean_up_space()
    logging.warning('End of log file')
    
