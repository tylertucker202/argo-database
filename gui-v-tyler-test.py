#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct 27 12:46:23 2018

@author: tyler
"""
import logging
import os
import sys
from argoDatabase import argoDatabase
GUI_PATH = './gui-code'
sys.path.append(GUI_PATH)
from argo4MongoDB import Argo4MongoDB


if __name__ == '__main__':

    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    LOGFILENAME = 'gui-v-tyler-test.log'
    OUTPUTDIR = os.path.join('/home', 'tyler', 'Desktop', 'argo-database', 'test-file-single')
    HOMEDIR = os.getcwd()
    dbName = 'argo'
    collectionName = 'profiles'
    basinFile = os.path.join(HOMEDIR, 'basinmask_01.nc')
    if os.path.exists(os.path.join(HOMEDIR, LOGFILENAME)):
        os.remove(LOGFILENAME)
    logging.basicConfig(format=FORMAT,
                        filename=LOGFILENAME,
                        level=logging.INFO)
    logging.info('Start of log file')
    HOME_DIR = os.getcwd()
    hostname = os.uname().nodename
    ad = argoDatabase(dbName,
                      collectionName,
                      replaceProfile=False,
                      qcThreshold='1', 
                      dbDumpThreshold=10000,
                      removeExisting=True,
                      testMode=True)
    logging.info('adding files using argoDatabase')
    ad.add_locally(OUTPUTDIR, howToAdd='profiles')
    logging.info('finished adding files using argoDatabase')
    
    TylerProfiles = ad.documents

    #new code that I am testing
    logging.info('adding files using Argo4MongoDB')
    GuiProfiles = []
    for doc in Argo4MongoDB(OUTPUTDIR,2500,basin_filename=basinFile):
        GuiProfiles.append(doc)
    logging.info('finished files using Argo4MongoDB')

    
    logging.info('Tyler profile length: {}'.format(len(TylerProfiles)))
    logging.info('Tyler profile length: {}'.format(len(GuiProfiles)))
    logging.info('end of log file')
