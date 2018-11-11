#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import os
import sys
PATH = '../'
sys.path.append(PATH)
from argoDatabase import argoDatabase
import multiprocessing as mp
from numpy import array_split

if __name__ == '__main__':

    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    LOGFILENAME = 'argoTroublesomeProfiles.log'
    OUTPUTDIR = os.path.join('/home', 'tyler', 'Desktop', 'argo-database', 'troublesome-files')
    HOMEDIR = os.getcwd()
    dbName = 'argo'
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
                      dbDumpThreshold=10000,
                      removeExisting=True,
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
    logging.warning('End of log file')
