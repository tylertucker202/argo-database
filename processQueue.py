from argoDatabase import argoDatabase
import os
import glob
import re
import logging
import pdb
queueDir = 'queuedFiles'
complDir = 'completedQueues'
addFromProfiles = True #  either add from profiles or *_prof.nc files.


if __name__ == '__main__':
    print(os.getcwd())
    #  collect queued files and process them
    for file in glob.glob(os.path.join(queueDir,'*.txt')):
        print(file)
        with open(file, 'r') as f:
            content = f.readlines()
        content = [x.strip() for x in content]
        content = [x for x in content if x.endswith('.nc' )]
        content = [x.split(' ')[1] for x in content]
        if addFromProfiles:
            content = [x for x in content if re.search(r'\d+.nc', x)]
        else:
            content = [x for x in content if re.search(r'prof.nc', x)]
        #print(content)
        #  move file to competed directory
        new_file_location = os.path.join(complDir,file.split('/')[-1])
        #print('moving file to {}'.format(new_file_location))
        #os.rename(file, new_file_location)
    #  process queue
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=FORMAT,
                        filename='processQueue.log',
                        level=logging.DEBUG)
    logging.debug('Start of log file')
    HOME_DIR = os.getcwd()
    IFREMER_DIR = os.path.join('/home', 'gstudent4', 'Desktop', 'ifremer')
    #IFREMER_DIR = os.path.join('/home', 'tyler', 'Desktop', 'argo', 'argo-database', 'ifremer')

    DB_NAME = 'argo_test'
    COLLECTION_NAME = 'profiles'

    ad = argoDatabase(DB_NAME, COLLECTION_NAME)
    if addFromProfiles:
        ad.add_locally(IFREMER_DIR, how_to_add='profiles', files=content)
    else:
        ad.add_locally(IFREMER_DIR, how_to_add='prof_list', files=content)