#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 29 16:46:56 2017

@author: gstudent4
"""
import logging
import os
from argoDatabase import argoDatabase

if __name__ == '__main__':
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=FORMAT,
                        filename='add_minor_dacs.log',
                        level=logging.WARNING)
    logging.debug('Start of log file')
    HOME_DIR = os.getcwd()
    OUTPUT_DIR = os.path.join('/storage', 'ifremer')
    #OUTPUT_DIR = os.path.join('/home', 'gstudent4', 'Desktop', 'troublesome_files')
    #OUTPUT_DIR = os.path.join('/home', 'tyler', 'Desktop', 'argo', 'argo-database', 'ifremer')
    # init database
    DB_NAME = 'argo'
    COLLECTION_NAME = 'profiles'
    DATA_DIR = os.path.join(HOME_DIR, 'data')

    ad = argoDatabase(DB_NAME, COLLECTION_NAME)
    minorDacs = ['nmdis', 'kordi', 'meds', 'kma', 'bodc', 'csio', 'incois' 'jma', 'csiro']
    ad.add_locally(OUTPUT_DIR, how_to_add='by_dac_profiles', dacs=minorDacs)