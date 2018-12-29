import sys
sys.path.append('..')
from addFunctions import format_logger, run_parallel_process
from argoDatabase import argoDatabase, getOutput
import os
import glob
import re
import logging
import pdb
from multiprocessing import cpu_count

dbName = 'argo2' 
argoBaseDir = os.path.join(os.getcwd(), os.pardir)
queueDir = os.path.join(argoBaseDir, 'queued-files')
complDir =  os.path.join(argoBaseDir, 'completed-queues')
npes = cpu_count()

def get_content(file):
    logging.warning('processing: {}:'.format(file))
    content = []
    with open(file, 'r') as f:
        content = f.readlines()
    content = [x.strip() for x in content]
    content = [x for x in content if x.startswith('>')]  # New files start with '>'
    content = [x for x in content if x.endswith('.nc' )]
    content = [x.split(' ')[1] for x in content]
    content = [x for x in content if re.search(r'\d+.nc', x)]
    content = [os.path.join(ncFileDir, profile) for profile in content]
    return content

if __name__ == '__main__':
    format_logger('processQueue.log', level=logging.WARNING)
    basinPath = os.path.join(os.path.pardir, 'basinmask_01.nc')
    ncFileDir = getOutput()
    ad = argoDatabase(dbName,
                      'profiles',
                      replaceProfile=True,
                      qcThreshold='1', 
                      dbDumpThreshold=1000,
                      removeExisting=False,
                      testMode=False,
                      basinFilename=basinPath)
    #  collect queued files and process them
    for file in glob.glob(os.path.join(queueDir, '*.txt')):
        content = get_content(file)
        if len(content) == 0:
            new_file_location = os.path.join(complDir,file.split('/')[-1])
            logging.debug('moving file to {}'.format(new_file_location))
            os.rename(file, new_file_location)	    
            continue

        try:
            logging.warning('adding {} files'.format(len(content)))
            #run_parallel_process(ad, content, ncFileDir, npes)\
            #  move qued file to competed directory upon sucessfull completion
            new_file_location = os.path.join(complDir,file.split('/')[-1])
            logging.debug('moving file to {}'.format(new_file_location))
            os.rename(file, new_file_location)
        except:
            logging.warning('adding {} to database not sucessfull.'.format(file))
    logging.warning('finished processing items')
