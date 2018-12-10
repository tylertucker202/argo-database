from argoDatabase import argoDatabase, getOutput
import os
import glob
import re
import logging

argoBaseDir = os.getcwd()
queueDir = os.path.join(argoBaseDir, 'queuedFiles')
complDir =  os.path.join(argoBaseDir, 'completedQueues')

if __name__ == '__main__':
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    LOGFILENAME = 'processQueue.log'
    HOMEDIR = os.getcwd()
    DB_NAME = 'argo'
    COLLECTIONNAME = 'profiles'    
    IFREMERDIR = getOutput()
    replace_profile = True
    if os.path.exists(os.path.join(HOMEDIR, LOGFILENAME)):
        os.remove(LOGFILENAME)
    logging.basicConfig(format=FORMAT,
                        filename=os.path.join(argoBaseDir,
                                              LOGFILENAME),
                                              level=logging.DEBUG)
    logging.debug('Start of log file')
    ad = argoDatabase(DB_NAME, COLLECTIONNAME, replace_profile)
    #  collect queued files and process them
    for file in glob.glob(os.path.join(queueDir,'*.txt')):
        logging.debug('processing: {}:'.format(file))
        content = []
        with open(file, 'r') as f:
            content = f.readlines()
        content = [x.strip() for x in content]
        content = [x for x in content if x.startswith('>')]  # New files start with '>'
        content = [x for x in content if x.endswith('.nc' )]
        content = [x.split(' ')[1] for x in content]
        content = [x for x in content if re.search(r'\d+.nc', x)]
        content = [os.path.join(IFREMERDIR, profile) for profile in content]
        if len(content) == 0:
            new_file_location = os.path.join(complDir,file.split('/')[-1])
            logging.debug('moving file to {}'.format(new_file_location))
            os.rename(file, new_file_location)	    
            continue
        try:
            #pdb.set_trace()
            logging.getLogger().setLevel(logging.WARNING)
            ad.add_locally(IFREMERDIR, howToAdd='profile_list', files=content)
            logging.getLogger().setLevel(logging.DEBUG)
            #  move qued file to competed directory upon sucessfull completion
            new_file_location = os.path.join(complDir,file.split('/')[-1])
            logging.debug('moving file to {}'.format(new_file_location))
            os.rename(file, new_file_location)
        except:
            logging.warning('adding {} to database not sucessfull.'.format(file))
    logging.debug('finished processing items')
