#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import os
from argoDatabase import argoDatabase, getOutput

if __name__ == '__main__':

    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    LOGFILENAME = 'aoml.log'
    OUTPUTDIR = getOutput()
    HOMEDIR = os.getcwd()
    dbName = 'flag_argo'
    collectionName = 'profiles'
    dacs = ['aoml']    
    if os.path.exists(os.path.join(HOMEDIR, LOGFILENAME)):
        os.remove(LOGFILENAME)
    logging.basicConfig(format=FORMAT,
                        filename=LOGFILENAME,
                        level=logging.WARNING)
    logging.warning('Start of log file')
    HOME_DIR = os.getcwd()
    hostname = os.uname().nodename
    ad = argoDatabase(dbName, collectionName,
                 replaceProfile=True,
                 qcThreshold='1', 
                 dbDumpThreshold=10000,
                 removeExisting=False)
    ad.add_locally(OUTPUTDIR, howToAdd='by_dac_profiles', dacs=dacs)
    logging.warning('End of log file')
