# -*- coding: utf-8 -*-
import logging
import sys
sys.path.append('..')
from argoDatabase import argoDatabase
from multiprocessing import cpu_count
import addFunctions as af #import format_logger, run_parallel_process, getMirrorDir, format_sysparams, get_dacs, cut_perc, single_out_threads
import warnings
import pdb
import os

from numpy import warnings as npwarnings
#  Sometimes netcdf contain nan. This will suppress runtime warnings.
warnings.simplefilter('error', RuntimeWarning)
npwarnings.filterwarnings('ignore')

if __name__ == '__main__':
    args = af.format_sysparams()
    #myVars = vars(args)
    logFileName = args.logName
    #dacs = get_dacs(['coriolis'])
    af.format_logger(logFileName, level=logging.WARNING)
    logging.warning('Start of log file')
    
    ncFileDir = af.getMirrorDir()
    ad = argoDatabase(args.dbName, 'profiles',
                      replaceProfile=args.replaceProfile,
                      qcThreshold=args.qcThreshold, 
                      dbDumpThreshold=args.dbDumpThreshold,
                      removeExisting=args.removeExisting,
                      basinFilename=args.basinFilename,
                      addToDb=args.addToDb, 
                      removeAddedFileNames=args.removeAddedFileNames, 
                      adjustedOnly=args.adjustedOnly)
    dacs = af.get_dacs(args.subset)
    files = ad.get_file_names_to_add(ncFileDir, dacs=dacs)

    files = af.reduce_files(args, files, ad)
    pdb.set_trace()

    
    af.run_parallel_process(ad, files, ncFileDir, args.npes)
    logging.warning('End of log file')
