#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import os
from argoDatabase import argoDatabase, getOutput

if __name__ == '__main__':

    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    LOGFILENAME = 'minorDacs.log'
    OUTPUTDIR = getOutput()
    HOMEDIR = os.getcwd()
    dbName = 'argo'
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
    ad = argoDatabase(dbName, collectionName, True)
    ad.add_locally(OUTPUTDIR, howToAdd='by_dac_profiles', dacs=dacs)
    logging.debug('End of log file')

