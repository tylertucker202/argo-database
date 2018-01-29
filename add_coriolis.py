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
        mySystem = 'ciLab'
    if mySystem == 'carby':
        OUTPUT_DIR = os.path.join('/storage', 'ifremer')
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
                        filename='coriolis.log',
                        level=logging.WARNING)
    logging.debug('Start of log file')
    HOME_DIR = os.getcwd()
    OUTPUT_DIR = getOutput()
    DB_NAME = 'argo2'
    COLLECTION_NAME = 'profiles'
    ad = argoDatabase(DB_NAME, COLLECTION_NAME)
    coriolisDac = ['coriolis']
    ad.add_locally(OUTPUT_DIR, how_to_add='by_dac_profiles', dacs=coriolisDac)
