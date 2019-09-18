# -*- coding: utf-8 -*-
import logging
import sys
import pdb
sys.path.append('..')
from argoDatabase import argoDatabase
from multiprocessing import cpu_count
import warnings
from numpy import warnings as npwarnings
from addFunctions import format_logger, run_parallel_process, cut_perc, getMirrorDir, single_out_threads
#  Sometimes netcdf contain nan. This will suppress runtime warnings.
warnings.simplefilter('error', RuntimeWarning)
npwarnings.filterwarnings('ignore')

dbName = 'argo'
npes = cpu_count()
        

if __name__ == '__main__':
    format_logger('coriolis-redo-2.log', level=logging.WARNING)
    logging.warning('Start of log file')
    ncFileDir = getMirrorDir()
    dacs = ['coriolis']
    logging.warning('Start of log file')
    ad = argoDatabase(dbName,
                      'profiles',
                      replaceProfile=True,
                      qcThreshold='1', 
                      dbDumpThreshold=1000,
                      removeExisting=False)
    files = ad.get_file_names_to_add(ncFileDir, dacs=dacs)
    files = cut_perc(ad, files, 40, npes) # use if script closes unexpectidly
    files = single_out_threads(ad, files, [0], npes) # use to run through certain section of files

    run_parallel_process(ad, files, ncFileDir, npes)
    logging.warning('End of log file')
