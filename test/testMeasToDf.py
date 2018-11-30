#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 29 15:55:28 2018

@author: tyler
"""

import os
import sys
import pdb
import re
sys.path.append('..')
from argoDatabase import argoDatabase
import unittest
import warnings
from numpy import warnings as npwarnings
#  Sometimes netcdf contain nan. This will suppress runtime warnings.
warnings.simplefilter('error', RuntimeWarning)
npwarnings.filterwarnings('ignore')

class measToDfTest(unittest.TestCase):

    def setUp(self):
        self.OUTPUTDIR = os.path.join(os.getcwd(), 'test-files')
        self.dbName = 'argo-test'
        self.collectionName = 'profiles'
        self.verificationErrors = []
        self.basinFilename = './../basinmask_01.nc'
        self.replaceProfile=False
        self.qcThreshold='1'
        self.dbDumpThreshold=1000
        self.removeExisting=False
        self.testMode=True
        self.addToDb=True
        self.basinFilename=self.basinFilename
        self.ad = argoDatabase(self.dbName,
                          self.collectionName,
                          self.replaceProfile,
                          self.qcThreshold,
                          self.dbDumpThreshold,
                          self.removeExisting,
                          self.testMode,
                          self.basinFilename,
                          self.addToDb)                

    def tearDown(self):
        return

    def test_all_bad_temp(self):
        '''nc with bad qc on temp should not be created '''
        profiles = ['6900287_8']
        files = self.ad.get_file_names_to_add(self.OUTPUTDIR)
        df = self.ad.create_df_of_files(files)
        df['_id'] = df.profile.apply(lambda x: re.sub('_0{1,}', '_', x))
        df = df[ df['_id'].isin(profiles)]
        self.ad.testMode = True
        self.ad.addToDb = False
        files = df.file.tolist()
        self.ad.add_locally(self.OUTPUTDIR, files)
        self.assertEqual(len(self.ad.documents), 0, "profile 6900287_8 should not be added")

if __name__ == '__main__':
    unittest.main()