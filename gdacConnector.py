import logging
import os
import sys
import ftputil
import pdb
from datetime import datetime

def get_files_after_date(path, date):
    files = ftp_host.listdir(path)
    ftp_host.chdir(path)
    returnFiles = []
    for file in files:
        timestamp = datetime.fromtimestamp(ftp_host.path.getmtime(file))
        if timestamp > minDate:
            filePath = os.path.join(path, file)
            returnFiles.append([filePath, timestamp])
    return returnFiles

if __name__ == '__main__':
    minDate = datetime(2018, 10, 31)
    ftpPath = os.path.join(os.sep, 'pub', 'outgoing', 'argo')
    GDAC = 'usgodae.org'
    todayDate = datetime.today().strftime('%Y-%m-%d')
    profileText = 'ar_index_global_prof.txt'
    
    localProfileIndex = os.path.curdir + os.sep + 'profile-indexes' + os.sep + profileText.strip('.txt') + '-' + todayDate + '.txt'

    ftp_host = ftputil.FTPHost(GDAC, 'anonymous', 'anonymous@anonymous.net')
    
    '''
    dacsToUpdate = get_files_after_date(ftpPath, minDate)
    profilesToUpdate = []
    pdb.set_trace()
    for dac in dacsToUpdate:
        dacPaths = os.path.join(ftpPath, dac[0])
        platformsToUpdate = get_files_after_date(dacPaths, minDate)
        for platform in platformsToUpdate:
            platformPaths = os.path.join(dacPaths, platform[0])
            profilesToUpdate = profilesToUpdate + get_files_after_date(platformPaths, minDate)
    '''
    #ftp_host.chdir(ftpPath)
    returnFiles = []
    ftp_host.keep_alive()
    ftp_host.chdir(ftpPath)
    ftp_host.download(profileText, localProfileIndex )
    '''
    for root, dirs, files in ftp_host.walk(ftpPath, topdown=False):
        pdb.set_trace()
        for name in files:
            timestamp = datetime.fromtimestamp(ftp_host.path.getmtime(name))
            print(os.path.join(root, name))
            if timestamp > minDate:
                returnFiles.append([name, timestamp])
    '''