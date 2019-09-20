import sys
sys.path.append('..')
import addFunctions as af 
from argoDatabase import argoDatabase
import os
import glob
import logging
import pdb
from multiprocessing import cpu_count
import warnings
from numpy import warnings as npwarnings
#  Sometimes netcdf contain nan. This will suppress runtime warnings.
warnings.simplefilter('error', RuntimeWarning)
npwarnings.filterwarnings('ignore')

'''
example
processQueue --logName processQueue.log --npes 4 --subset
'''

if __name__ == '__main__':
    af.format_logger('processQueue.log', level=logging.WARNING)
    basinPath = os.path.join(os.path.pardir, 'basinmask_01.nc')
    ncFileDir = af.getMirrorDir()
    argoBaseDir = os.path.join(os.getcwd(), os.pardir)
    queueDir = os.path.join(argoBaseDir, 'queued-files')
    complDir =  os.path.join(argoBaseDir, 'completed-queues')

    args = af.format_sysparams()
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
    #  collect queued files and process them
    for file in glob.glob(os.path.join(queueDir, '*.txt')):
        if file.split('/')[-1] == 'testQueue.txt':
            continue
        content = af.get_nc_files_from_rsync_output(file, ncFileDir)
        content = ad.remove_duplicate_if_mixed_or_synthetic(content)
        new_file_location = os.path.join(complDir,file.split('/')[-1])
        if len(content) == 0:
            logging.warning('moving file to {}'.format(new_file_location))
            os.rename(file, new_file_location)	    
            continue

        try:
            logging.warning('adding {} files'.format(len(content)))
            af.run_parallel_process(ad, content, ncFileDir, args.npes)
            #  move qued file to competed directory upon sucessfull completion
            logging.warning('moving file to {}'.format(new_file_location))
            os.rename(file, new_file_location)
        except:
            logging.warning('adding {} to database not sucessfull.'.format(file))
    logging.warning('finished processing items')
