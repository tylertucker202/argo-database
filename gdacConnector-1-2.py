import os
#import ftputil  #also an option
from ftplib import FTP
import pandas as pd
import pdb
from datetime import datetime

def download_todays_file():
    ftp_host = ftputil.FTPHost(GDAC, 'anonymous', 'anonymous@anonymous.net')
    ftp_host.keep_alive()
    ftp_host.chdir(ftpPath)
    ftp_host.download(profileText, localProfileIndex )
    
def get_df_of_files_to_add(filename):
    dfChunk = pd.read_csv(filename, sep=',', chunksize=100000, header=8)
    df = pd.DataFrame()
    for chunk in dfChunk:
        chunk.dropna(axis=0, how='any', inplace=True)
        chunk.date_update = pd.to_datetime(chunk.date_update.astype(int), format='%Y%m%d%H%M%S')
        chunk.date = pd.to_datetime(chunk.date.astype(int), format='%Y%m%d%H%M%S')
        chunk = chunk[chunk.date_update >= minDate]
        df = pd.concat([df, chunk])
    return df
    
if __name__ == '__main__':
    minDate = datetime(2018, 10, 18)
    ftpPath = os.path.join(os.sep, 'pub', 'outgoing', 'argo')
    GDAC = 'usgodae.org'
    todayDate = datetime.today().strftime('%Y-%m-%d')
    profileText = 'ar_index_global_prof.txt'
    localProfileIndex = os.path.curdir \
                       + os.sep \
                       + 'profile-indexes' \
                       + os.sep + profileText.strip('.txt') \
                       + '-' + todayDate + '.txt'

    #download_todays_file()
    df = get_df_of_files_to_add(localProfileIndex)
