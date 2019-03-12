#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import os
import sys
sys.path.append('..')
from argoDatabase import argoDatabase
from multiprocessing import cpu_count
import warnings
from numpy import warnings as npwarnings
from addFunctions import format_logger, run_parallel_process
#  Sometimes netcdf contain nan. This will suppress runtime warnings.
warnings.simplefilter('error', RuntimeWarning)
npwarnings.filterwarnings('ignore') 

dbName = 'argo-trouble'
npes = 4
if __name__ == '__main__':
    ncFileDir = os.path.join('/home', 'tyler', 'Desktop', 'argo-database', 'troublesome-files')
    format_logger('trouble.log', level=logging.INFO)
    logging.warning('Start of log file')
    ad = argoDatabase(dbName,
                      'profiles',
                      replaceProfile=False,
                      qcThreshold='1', 
                      dbDumpThreshold=1000,
                      removeExisting=True)
    
    files = ad.get_file_names_to_add(ncFileDir)
    run_parallel_process(ad, files, ncFileDir, npes)
    logging.warning('End of log file')
