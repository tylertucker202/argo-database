import os
#import ftputil  #also an option
from ftplib import FTP
import pandas as pd
import pdb
from datetime import datetime
import re
import tempfile


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
        #os.rename(profileText, localProfileIndex)
    
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
            with open(fileName, "wb") as f:
                ftp.retrbinary("RETR " + row.file, f.write)
            #os.rename(profileText, fileName)
    
if __name__ == '__main__':
    minDate = datetime(2018, 10, 31)
    maxDate = datetime.today()
    #ftpPath = os.path.join(os.sep, 'pub', 'outgoing', 'argo')
    #GDAC = 'usgodae.org'
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

    #download_todays_file()
    #df = get_df_of_files_to_add(localProfileIndex)
    create_dir_of_files(df)