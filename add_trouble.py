#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import os
import sys
from argoDatabase import argoDatabase

def getOutput():
    try:
        mySystem = sys.argv[1]
    except IndexError:
        mySystem = 'carbyTrouble'
    if mySystem == 'carby':
        OUTPUT_DIR = os.path.join('/storage', 'ifremer')
    if mySystem == 'carbyTrouble':
        OUTPUT_DIR = os.path.join('/home', 'tyler', 'Desktop', 'argo-database', 'troublesome-files')
    elif mySystem == 'kadavu':
        OUTPUT_DIR = os.path.join('/home', 'tylertucker', 'ifremer')
    elif mySystem == 'ciLab':
        OUTPUT_DIR = os.path.join('/home', 'gstudent4', 'Desktop', 'ifremer')
    else:
        print('pc not found. assuming default')
        OUTPUT_DIR = os.path.join('/storage', 'ifremer')
    return OUTPUT_DIR

if __name__ == '__main__':
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=FORMAT,
                        filename='argoTroublesomeProfiles.log',
                        level=logging.INFO)
    logging.debug('Start of log file')
    HOME_DIR = os.getcwd()
    OUTPUT_DIR = getOutput()
    # init database
    dbName = 'argoTrouble'
    collectionName = 'profiles'
    ad = argoDatabase(dbName, collectionName, True)
    ad.add_locally(OUTPUT_DIR, howToAdd='profiles')
    logging.debug('End of log file')
