# -*- coding: utf-8 -*-
import logging
import sys
sys.path.append('..')
from argoDatabase import argoDatabase
from multiprocessing import cpu_count
import addFunctions as af
import warnings
import pdb
import os
from datetime import datetime, timedelta

from numpy import warnings as npwarnings
#  Sometimes netcdf contain nan. This will suppress runtime warnings.
warnings.simplefilter('error', RuntimeWarning)
npwarnings.filterwarnings('ignore')

"""
example of uses:
python add_profiles.py --logName adjusted.log --adjustedOnly 1 --subset adjusted
python add_profiles.py --dbName argo-trouble --subset trouble --logName trouble.log --npes 1
python add_profiles.py --dbName argo-express-test --subset argo-express-test --logName argo-express-test.log --npes 1
python add_profiles.py --dbName argo --subset synthetic --logName synthetic.log --npes 1
python add_profiles.py --dbName argo --subset aoml --logName aoml.log
python add_profiles.py --dbName argo --subset coriolis --logName coriolis.log
python add_profiles.py --dbName argo --subset minor --logName minor.log
python add_profiles.py --dbName argo --subset missingDataMode --logName missingDataMode.log --npes 2 --dbDumpThreshold 300
python add_profiles.py --dbName argo --subset tmp --logName tmp.log --npes 1
python add_profiles.py --dbName argo --subset bgc --logName bgc.log --removeExisting False
python add_profiles.py --dbName argo --subset dateRangeUpdated --logName tmp.log --npes 1 --minDate 2019-12-30 --maxDate 2020-01-01
python add_profiles.py --dbName argo --subset dateRange --logName tmp.log --npes 1 --minDate 2020-04-07 --maxDate 2020-04-11
"""

if __name__ == '__main__':
    args = af.format_sysparams()
    logFileName = args.logName
    af.format_logger(logFileName, level=logging.WARNING)
    logging.warning('Start of log file')
    startTime = datetime.today()
    ad = argoDatabase(args.dbName, 'profiles',
                      qcThreshold=args.qcThreshold, 
                      dbDumpThreshold=args.dbDumpThreshold,
                      removeExisting=args.removeExisting,
                      basinFilename=args.basinFilename,
                      addToDb=args.addToDb,
                      removeAddedFileNames=args.removeAddedFileNames, 
                      adjustedOnly=args.adjustedOnly)

    ncFileDir = af.get_mirror_dir(args)
    dacs = af.get_dacs(args.subset)
    #pdb.set_trace()
    df = af.get_df_to_add(ncFileDir, dacs=dacs)
    df = af.reduce_files(args, df)
    #df = af.cut_perc(df, 38, 2) # cut top 38 perc
    #df = af.cut_perc(df, 40, 2) # cut top 40 percent of that
    af.run_parallel_process(ad, df.file.tolist(), ncFileDir, args.npes)

    if (args.subset == 'tmp') or (args.subset == 'dateRange'):
        logging.warning('cleaning up tmp')
        af.tmp_clean_up()

    finishTime = datetime.today()
    deltaTime = finishTime - startTime

    minutes = round(deltaTime.seconds / 60, 3)

    logging.warning('Time to complete: {} minutes'.format(minutes))

    logging.warning('End of log file')
