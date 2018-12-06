#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys, os, re
sys.path.append('..')
from argoDatabase import argoDatabase

OUTPUT_DIR = os.path.join('./test-files')

ad = argoDatabase('', '', False, '1', 0, False, True, './../basinmask_01.nc', False)                
profiles = ['6900287_8']

if __name__ == '__main__':
    files = ad.get_file_names_to_add(OUTPUT_DIR)
    df = ad.create_df_of_files(files)
    df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
    df = df[ df['_id'].isin(profiles)]
    files = df.file.tolist()
    ad.add_locally(OUTPUT_DIR, files)
    
    for doc in ad.documents:
        print(doc)