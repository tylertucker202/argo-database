#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import os
from argoDatabase import argoDatabase

if __name__ == '__main__':

    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    LOGFILENAME = 'argoTroublesomeProfiles.log'
    OUTPUTDIR = os.path.join('/home', 'tyler', 'Desktop', 'argo-database', 'troublesome-files')
    HOMEDIR = os.getcwd()
    dbName = 'argoTrouble'
    collectionName = 'profiles'
    if os.path.exists(os.path.join(HOMEDIR, LOGFILENAME)):
        os.remove(LOGFILENAME)
    logging.basicConfig(format=FORMAT,
                        filename=LOGFILENAME,
                        level=logging.INFO)
    logging.debug('Start of log file')
    HOME_DIR = os.getcwd()
    hostname = os.uname().nodename
    ad = argoDatabase(dbName, collectionName, True)
    ad.add_locally(OUTPUTDIR, howToAdd='profiles')
    logging.debug('End of log file')
