#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import os
import sys
sys.path.append('..')
lib_path = os.path.abspath(os.path.join(__file__, os.pardir))
sys.path.append(lib_path)
from argoDatabase import argoDatabase
import multiprocessing as mp
from numpy import array_split
import warnings
from numpy import warnings as npwarnings
#  Sometimes netcdf contain nan. This will suppress runtime warnings.
warnings.simplefilter('error', RuntimeWarning)
npwarnings.filterwarnings('ignore')

dbName = 'argo2'
if __name__ == '__main__':

    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    LOGFILENAME = 'trouble.log'
    OUTPUTDIR = os.path.join('/home', 'tyler', 'Desktop', 'argo-database', 'troublesome-files')
    HOMEDIR = os.getcwd()
    basinPath = os.path.join(os.path.pardir, 'basinmask_01.nc')
    collectionName = 'profiles'
    if os.path.exists(os.path.join(HOMEDIR, LOGFILENAME)):
        os.remove(LOGFILENAME)
    logging.basicConfig(format=FORMAT,
                        filename=LOGFILENAME,
                        level=logging.INFO)
    logging.warning('Start of log file')
    HOME_DIR = os.getcwd()
    hostname = os.uname().nodename
    ad = argoDatabase(dbName,
                      collectionName,
                      replaceProfile=False,
                      qcThreshold='1', 
                      dbDumpThreshold=1000,
                      removeExisting=True,
                      testMode=False,
                      basinFilename=basinPath )
    
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
        
    #ad.add_locally(OUTPUTDIR, files)
    logging.warning('End of log file')
