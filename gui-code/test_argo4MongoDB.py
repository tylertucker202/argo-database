from scipy.io import savemat
import sys

from argo4MongoDB import Argo4MongoDB

# make sure basinmask_01.nc,ftp.py, argo.py and argo4MongoDB.py are in the same folder as program

#src_path = 'ftp://ftp.ifremer.fr/ifremer/argo/dac/'
src_path = '/home/tylertucker/ifremer/'

path_files = './mat_files/'

# to create floatpath_list, we start from:  ftp://ftp.ifremer.fr/ifremer/argo/ar_index_global_prof.txt
# all the comments below are in the script:  script_to_create_floatpath_list.sh which should be used to create floatpath_list.txt
# wget ftp://ftp.ifremer.fr/ifremer/argo/ar_index_global_prof.txt
# cp ar_index_global_prof.txt floatpath_list_bfr00.txt
# sed -e '1,9d' < floatpath_list_bfr00.txt > floatpath_list_bfr01.txt
# awk -F',' '{print $1}' floatpath_list_bfr01.txt > floatpath_list_bfr02.txt 
# cat floatpath_list_bfr02.txt | cut -d/ -f1-3 > floatpath_list_bfr03.txt
# sed 's/profiles/profiles\//' floatpath_list_bfr03.txt > floatpath_list.txt

#path_list = './profilepath_list.txt'
#path_list = './floatpath_list.txt'
#paths2use =  open(path_list, 'r').read().split("\n")[:-1]
#paths2use = ['nmdis/']
argin = sys.argv[1]
paths2use = {argin}

for fn in paths2use:
    try:
        # fn is a single profile (rather than a path) if we use profilepath_list.txt... in this case, the loop below has only 1 cycle
        for d in Argo4MongoDB(src_path+fn+'/', 2500, './basinmask_01.nc'):
            # let's check that all the d that are returned have PRES (hence they should also have temp)
            if 'PRES' in d:
                print('Saving to mat: '+fn+', x_id: '+d['x_id'], flush=True)
                savemat(path_files+d['x_id']+'.mat', d, oned_as='row')
            else:
                print('No pressure in '+fn+', x_id: '+d['x_id'])
    except:
        print('Failed statement in test script for '+fn)
#break
