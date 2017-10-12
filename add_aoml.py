#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import os
from argoDatabase import argoDatabase

if __name__ == '__main__':
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=FORMAT,
                        filename='add_aoml.log',
                        level=logging.DEBUG)
    logging.debug('Start of log file')
    HOME_DIR = os.getcwd()
    OUTPUT_DIR = os.path.join('/home', 'gstudent4', 'Desktop', 'ifremer')
    #OUTPUT_DIR = os.path.join('/home', 'gstudent4', 'Desktop', 'troublesome_files')
    # init database
    DB_NAME = 'argo_test'
    COLLECTION_NAME = 'profiles'
    DATA_DIR = os.path.join(HOME_DIR, 'data')

    ad = argoDatabase(DB_NAME, COLLECTION_NAME)
    aomlDac = ['aoml']
    ad.add_locally(OUTPUT_DIR, how_to_add='by_dac_profiles', dacs=aomlDac)