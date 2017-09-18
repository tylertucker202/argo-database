import argoTestDatabase
import os
import glob
import re

queueDir = 'queuedFiles'
complDir = 'completedQueues'
addFromProfiles = True


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
        print(content)

        new_file_location = os.path.join(complDir,file.split('/')[-1])
        print('moving file to {}'.format(new_file_location))
        os.rename(file, new_file_location)