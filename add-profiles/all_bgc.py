#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import os
import sys
sys.path.append('..')
from argoDatabase import argoDatabase, getOutput
import multiprocessing as mp
from numpy import array_split
import warnings
from numpy import warnings as npwarnings
from from_tmp_functions import StreamToLogger

#  Sometimes netcdf contain nan. This will suppress runtime warnings.
warnings.simplefilter('error', RuntimeWarning)
npwarnings.filterwarnings('ignore')

dbName = 'argo'
npes = 1
if __name__ == '__main__':

    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    LOGFILENAME = 'bgc.log'
    OUTPUTDIR = getOutput()
    HOMEDIR = os.getcwd()
    basinPath = os.path.join(os.path.pardir, 'basinmask_01.nc')
    collectionName = 'profiles'
    dacs = ['aoml', 'coriolis', 'nmdis', 'kordi', 'meds', 'kma', 'bodc', 'csio', 'incois', 'jma', 'csiro']
    if os.path.exists(os.path.join(HOMEDIR, LOGFILENAME)):
        os.remove(LOGFILENAME)
    logging.basicConfig(format=FORMAT,
                        filename=LOGFILENAME,
                        level=logging.WARNING)
    stdout_logger = logging.getLogger('STDOUT')
    sl = StreamToLogger(stdout_logger, logging.INFO)
    sys.stdout = sl
    stderr_logger = logging.getLogger('STDERR')
    sl = StreamToLogger(stderr_logger, logging.ERROR)
    logging.debug('Start of log file')
    HOME_DIR = os.getcwd()
    hostname = os.uname().nodename

    ad = argoDatabase(dbName, collectionName,
                 replaceProfile=True,
                 qcThreshold='1', 
                 dbDumpThreshold=1000,
                 removeExisting=True,
                 testMode=False,
                 basinFilename=basinPath)
    allFiles = ad.get_file_names_to_add(OUTPUTDIR, dacs=dacs)
    files = []
    for file in allFiles:
        fileName = file.split('/')[-1]
        if fileName.startswith('M'):
            files.append(file)
    try:
        npes
    except NameError:
        npes = mp.cpu_count() - 1
    fileArray = array_split(files, npes)
    processes = [mp.Process(target=ad.add_locally, args=(OUTPUTDIR, fileChunk)) for fileChunk in fileArray]
    for p in processes:
        p.start()
    for p in processes:
        p.join()
        
    #ad.add_locally(OUTPUTDIR, files)
    logging.warning('End of log file')

