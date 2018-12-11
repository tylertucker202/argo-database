#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import sys
sys.path.append('..')
from argoDatabase import argoDatabase, getOutput
from multiprocessing import cpu_count
import warnings
from numpy import warnings as npwarnings
from addFunctions import format_logger, run_parallel_process
#  Sometimes netcdf contain nan. This will suppress runtime warnings.
warnings.simplefilter('error', RuntimeWarning)
npwarnings.filterwarnings('ignore') 

dbName = 'argo'
npes = cpu_count()
if __name__ == '__main__':
    ncFileDir = getOutput()
    format_logger('minor.log', level=logging.WARNING)
    logging.warning('Start of log file')
    dacs = ['nmdis', 'kordi', 'meds', 'kma', 'bodc', 'csio', 'incois', 'jma', 'csiro']
    ad = argoDatabase(dbName,
                      'profiles',
                      replaceProfile=True,
                      qcThreshold='1', 
                      dbDumpThreshold=1000,
                      removeExisting=True,
                      testMode=False)
    
    files = ad.get_file_names_to_add(ncFileDir, dacs)
    run_parallel_process(ad, files, npes)
    logging.warning('End of log file')