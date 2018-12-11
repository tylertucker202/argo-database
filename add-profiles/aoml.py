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
    format_logger('aoml.log', level=logging.WARNING)
    logging.warning('Start of log file')
    ncFileDir = getOutput()
    dacs = ['aoml']
    logging.warning('Start of log file')
    ad = argoDatabase(dbName,
                      'profiles',
                      replaceProfile=True,
                      qcThreshold='1', 
                      dbDumpThreshold=1000,
                      removeExisting=False,
                      testMode=False)
    files = ad.get_file_names_to_add(ncFileDir, dacs=dacs)
    run_parallel_process(ad, files, npes)
    logging.warning('End of log file')