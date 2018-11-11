#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import os
from argoDatabase import argoDatabase, getOutput
import sys
import multiprocessing as mp
from numpy import array_split

PATH = '../'
sys.path.append(PATH)
if __name__ == '__main__':

    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    LOGFILENAME = 'minorDacs.log'
    OUTPUTDIR = getOutput()
    HOMEDIR = os.getcwd()
    dbName = 'argo2'
    collectionName = 'profiles'
    dacs = ['nmdis', 'kordi', 'meds', 'kma', 'bodc', 'csio', 'incois', 'jma', 'csiro']
    if os.path.exists(os.path.join(HOMEDIR, LOGFILENAME)):
        os.remove(LOGFILENAME)
    logging.basicConfig(format=FORMAT,
                        filename=LOGFILENAME,
                        level=logging.WARNING)
    logging.debug('Start of log file')
    HOME_DIR = os.getcwd()
    hostname = os.uname().nodename
    ad = argoDatabase(dbName, collectionName,
                 replaceProfile=True,
                 qcThreshold='1', 
                 dbDumpThreshold=10000,
                 removeExisting=False)
    files = ad.get_file_names_to_add(OUTPUTDIR, howToAdd='by_dac_profiles', dacs=dacs)
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

