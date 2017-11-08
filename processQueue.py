from argoDatabase import argoDatabase
import os
import glob
import re
import logging
import pdb
queueDir = 'queuedFiles'
complDir = 'completedQueues'
home_dir = os.getcwd()
if __name__ == '__main__':
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=FORMAT,
                        filename='processQueue.log',
                        level=logging.DEBUG)
    logging.debug('Start of log file')
    DB_NAME = 'argo'
    COLLECTION_NAME = 'profiles'    
    IFREMER_DIR = os.path.join('/home', 'gstudent4', 'Desktop', 'ifremer')
    #IFREMER_DIR = os.path.join('/home', 'tyler', 'Desktop', 'argo', 'argo-database', 'ifremer')
    #IFREMER_DIR = os.path.join('/home', 'tylertucker', ifremer')
    replace_profile = True
    ad = argoDatabase(DB_NAME, COLLECTION_NAME, replace_profile)
    #  collect queued files and process them
    for file in glob.glob(os.path.join(queueDir,'*.txt')):
        logging.debug('processing: {}:'.format(file))
        content = []
        with open(file, 'r') as f:
            content = f.readlines()
        content = [x.strip() for x in content]
        content = [x for x in content if x.endswith('.nc' )]
        content = [x.split(' ')[1] for x in content]
        content = [x for x in content if re.search(r'\d+.nc', x)]
        content = [os.path.join(IFREMER_DIR, profile) for profile in content]
        if len(content) == 0:
            continue
        try:
            logging.getLogger().setLevel(logging.WARNING)
            #ad.add_locally(IFREMER_DIR, how_to_add='profile_list', files=content)
            logging.getLogger().setLevel(logging.DEBUG)
            #  move qued file to competed directory upon sucessfull completion
            new_file_location = os.path.join(home_dir,complDir,file.split('/')[-1])
            logging.debug('moving file to {}'.format(new_file_location))
            os.rename(file, new_file_location)
        except:
            logging.warning('adding {} to database not sucessfull.'.format(file))
